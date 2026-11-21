"""A5 backpressure: bounded coalescing, critical events preserved,
ingestion never blocked by slow subscribers. Coalescer bounds are proven
at unit level; delivery guarantees are proven against a real server with
a genuinely lagging socket."""

import time

import httpx2 as httpx
from sqlalchemy import text

from app.stream import Coalescer
from tests.sse_harness import (
    collect,
    collect_until,
    open_stream,
    post_readings,
    start_instances,
    stop_instances,
)


def ev(eid, etype, vid):
    return {"id": eid, "type": etype, "vehicle_id": vid, "payload": {}}


def data_events(events, vehicle_id=None):
    out = [e for e in events if "id" in e]
    if vehicle_id is not None:
        out = [e for e in out if e["data"]["vehicle_id"] == vehicle_id]
    return out


# ---- Coalescer unit tests: pure logic, deterministic ----


def test_replaceable_collapses_to_newest_per_vehicle_and_type():
    c = Coalescer(window_seconds=60)
    assert c.add(ev(1, "telemetry_update", "V1")) == []
    assert c.add(ev(2, "telemetry_update", "V1")) == []
    assert c.add(ev(3, "vehicle_status_change", "V1")) == []
    assert c.add(ev(4, "telemetry_update", "V2")) == []
    assert c.pending_count == 3  # V1 telemetry, V1 status, V2 telemetry
    flushed = c.flush()
    assert [e["id"] for e in flushed] == [2, 3, 4]
    assert c.pending_count == 0


def test_critical_flushes_pending_first_in_id_order():
    c = Coalescer(window_seconds=60)
    c.add(ev(1, "telemetry_update", "V1"))
    c.add(ev(2, "telemetry_update", "V1"))
    out = c.add(ev(3, "alert_opened", "V1"))
    assert [e["id"] for e in out] == [2, 3]
    assert c.pending_count == 0


def test_critical_events_never_merged_or_dropped():
    c = Coalescer(window_seconds=60)
    out = []
    for eid, etype in [
        (1, "alert_opened"),
        (2, "alert_acknowledged"),
        (3, "alert_resolved"),
        (4, "vehicle_offline"),
        (5, "vehicle_recovered"),
    ]:
        out.extend(c.add(ev(eid, etype, "V1")))
    assert [e["id"] for e in out] == [1, 2, 3, 4, 5]


def test_pending_bounded_by_two_per_vehicle():
    c = Coalescer(window_seconds=60)
    for i in range(1000):
        vid = f"V{i % 50}"
        c.add(ev(i * 2, "telemetry_update", vid))
        c.add(ev(i * 2 + 1, "vehicle_status_change", vid))
    assert c.pending_count <= 2 * 50


def test_window_due_and_zero_window_passthrough():
    c = Coalescer(window_seconds=0.05)
    c.add(ev(1, "telemetry_update", "V1"))
    assert not c.due()
    time.sleep(0.06)
    assert c.due()
    assert [e["id"] for e in c.flush()] == [1]

    passthrough = Coalescer(window_seconds=0)
    assert passthrough.add(ev(1, "telemetry_update", "V1")) == [
        ev(1, "telemetry_update", "V1")
    ]
    assert passthrough.pending_count == 0


# ---- Real-server tests with a genuinely slow (non-reading) client ----


def test_slow_client_gets_coalesced_telemetry(isolated_client, payload):
    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    # Window longer than the ingest burst: everything coalesces into a
    # single flush, so the slow client sees exactly the newest state.
    procs, bases = start_instances(
        db_url, 1, {"SSE_COALESCE_WINDOW_SECONDS": "2.0"}
    )
    base = bases[0]
    try:
        vid = "V-BP-COALESCE"
        with open_stream(base, last_event_id=0) as resp:
            assert resp.status_code == 200
            # 50 readings commit while the client refuses to read.
            batch = [payload(vid, s) for s in range(1, 51)]
            assert post_readings(
                base, client.api_key, client.api_secret, batch
            ).status_code == 200
            time.sleep(1.0)  # inside the coalesce window; no flush yet
            events = collect(resp, 2.5)  # window expires -> one flush
        telemetry = [
            e for e in data_events(events, vid)
            if e["event"] == "telemetry_update"
        ]
        assert len(telemetry) == 1, "telemetry was not coalesced"
        assert telemetry[0]["data"]["payload"]["seq"] == 50
        assert any(
            e["event"] == "vehicle_status_change"
            for e in data_events(events, vid)
        )
    finally:
        stop_instances(procs)


def test_critical_lifecycle_preserved_for_slow_client(isolated_client, payload):
    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    procs, bases = start_instances(
        db_url, 1, {"SSE_COALESCE_WINDOW_SECONDS": "0.5"}
    )
    base = bases[0]
    try:
        vid = "V-BP-CRITICAL"
        with open_stream(base, last_event_id=0) as resp:
            assert resp.status_code == 200
            hot = [payload(vid, s, battery_temp_c=60.0) for s in range(1, 4)]
            assert post_readings(
                base, client.api_key, client.api_secret, hot
            ).status_code == 200
            with engine.connect() as conn:
                alert_id = conn.execute(
                    text(
                        "SELECT a.id FROM alerts a JOIN vehicles v ON v.id=a.vehicle_id "
                        "WHERE v.vehicle_id=:v AND a.rule_type='overheat'"
                    ),
                    {"v": vid},
                ).scalar_one()
            ack = httpx.post(
                f"{base}/v1/alerts/{alert_id}/ack", json={"version": 1}, timeout=10
            )
            assert ack.status_code == 200
            cool = [payload(vid, s, battery_temp_c=40.0) for s in range(4, 7)]
            assert post_readings(
                base, client.api_key, client.api_secret, cool
            ).status_code == 200
            time.sleep(1.5)  # client lags through the whole lifecycle
            events = collect(resp, 1.5)

        lifecycle = [
            e for e in data_events(events, vid)
            if e["event"] in ("alert_opened", "alert_acknowledged", "alert_resolved")
            and e["data"]["payload"]["rule_type"] == "overheat"
        ]
        assert [e["event"] for e in lifecycle] == [
            "alert_opened", "alert_acknowledged", "alert_resolved"
        ], [(e["event"], e["data"]["payload"]) for e in lifecycle]
        assert all(e["data"]["payload"]["alert_id"] == alert_id for e in lifecycle)
        ids = [e["id"] for e in data_events(events, vid)]
        assert ids == sorted(ids), "ordering violated around coalesced events"
    finally:
        stop_instances(procs)


def test_ingestion_unblocked_and_subscribers_isolated(isolated_client, payload):
    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    procs, bases = start_instances(
        db_url, 1, {"SSE_COALESCE_WINDOW_SECONDS": "0.5"}
    )
    base = bases[0]
    try:
        vid = "V-BP-UNBLOCKED"
        with open_stream(base, last_event_id=0) as slow:
            assert slow.status_code == 200
            # A second, healthy subscriber on the same server.
            with open_stream(base, last_event_id=0) as fast:
                assert fast.status_code == 200
                start = time.monotonic()
                r = post_readings(
                    base, client.api_key, client.api_secret,
                    [payload(vid, s) for s in range(1, 21)],
                )
                latency = time.monotonic() - start
                assert r.status_code == 200
                assert latency < 3.0, f"ingestion blocked by slow client: {latency:.2f}s"

                events, ok = collect_until(
                    fast,
                    lambda evs: any(
                        e.get("event") == "telemetry_update"
                        and e["data"]["payload"]["seq"] == 20
                        for e in evs
                    ),
                )
                assert ok
            # slow client never read during ingestion; nothing crashed,
            # and it can still drain its (bounded) buffered state.
            time.sleep(1.0)
            slow_events = collect(slow, 1.0)
        assert any(
            e["event"] == "telemetry_update" for e in data_events(slow_events, vid)
        )
    finally:
        stop_instances(procs)