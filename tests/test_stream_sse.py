"""A5 SSE streaming over real HTTP: structure, ordering, resumption,
and explicit handling of unknown/expired cursors and retention gaps.
Every test runs against a real uvicorn process and a real scratch
PostgreSQL database — no mocks, no in-memory shortcuts."""

import time

import httpx2 as httpx
import pytest
from sqlalchemy import text

from tests.sse_harness import (
    collect,
    collect_until,
    open_stream,
    post_readings,
    start_instances,
    stop_instances,
)


def data_events(events, vehicle_id=None):
    out = [e for e in events if "id" in e]
    if vehicle_id is not None:
        out = [e for e in out if e["data"]["vehicle_id"] == vehicle_id]
    return out


def test_stream_structure_and_envelope(stream_server, payload):
    env = stream_server
    vid = "V-SSE-STRUCT"
    r = post_readings(env.base, env.api_key, env.secret, [payload(vid, 1)])
    assert r.status_code == 200

    with open_stream(env.base, last_event_id=0) as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        events, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 2
        )
    assert ok
    for e in data_events(events, vid):
        assert set(e["data"]) == {"id", "type", "ts", "vehicle_id", "payload"}
        assert e["id"] == e["data"]["id"]
        assert e["event"] == e["data"]["type"]
        assert isinstance(e["id"], int)
    types = [e["event"] for e in data_events(events, vid)]
    assert "telemetry_update" in types
    assert "vehicle_status_change" in types
    change = next(
        e for e in data_events(events, vid) if e["event"] == "vehicle_status_change"
    )
    assert change["data"]["payload"]["from"] == "offline"
    assert change["data"]["payload"]["to"] == "active"


def test_ids_strictly_increasing_across_batches(stream_server, payload):
    env = stream_server
    vid = "V-SSE-ORDER"
    for seq in range(1, 6):
        assert post_readings(
            env.base, env.api_key, env.secret, [payload(vid, seq)]
        ).status_code == 200

    with open_stream(env.base, last_event_id=0) as resp:
        events, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 6
        )
    assert ok
    ids = [e["id"] for e in data_events(events, vid)]
    assert ids == sorted(set(ids))


def test_resume_no_skips_no_duplicates(stream_server, payload):
    env = stream_server
    vid = "V-SSE-RESUME"
    assert post_readings(
        env.base, env.api_key, env.secret, [payload(vid, 1), payload(vid, 2)]
    ).status_code == 200

    with open_stream(env.base, last_event_id=0) as resp:
        first, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 3
        )
    assert ok
    seen = data_events(first, vid)
    last_id = seen[-1]["id"]

    # Client disconnects (socket closed), more events happen meanwhile.
    assert post_readings(
        env.base, env.api_key, env.secret,
        [payload(vid, 3), payload(vid, 4), payload(vid, 5)],
    ).status_code == 200

    with open_stream(env.base, last_event_id=last_id) as resp:
        second, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 3
        )
    assert ok
    resumed = data_events(second, vid)
    assert resumed[0]["id"] == last_id + 1
    all_ids = [e["id"] for e in seen + resumed]
    assert len(all_ids) == len(set(all_ids)), "duplicate delivery on resume"

    with env.engine.connect() as conn:
        expected_rows = conn.execute(
            text(
                "SELECT id, event_type FROM fleet_events "
                "WHERE vehicle_id = :v ORDER BY id"
            ),
            {"v": vid},
        ).all()
    expected = [r[0] for r in expected_rows]
    assert all_ids == expected, f"silent skip on resume: db={expected_rows}"


def test_last_event_id_zero_replays_retained_history(stream_server, payload):
    env = stream_server
    vid = "V-SSE-ZERO"
    assert post_readings(
        env.base, env.api_key, env.secret, [payload(vid, 1)]
    ).status_code == 200
    with open_stream(env.base, last_event_id=0) as resp:
        events, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 2
        )
    assert ok


def test_replay_from_zero_starts_at_oldest_retained_id(stream_server, payload):
    # Retention trims the head of the log, so min_id is far above 0. Replay
    # must begin at min_id immediately, not stall in gap-grace and declare
    # the whole retained history a hole.
    env = stream_server
    vid = "V-SSE-ZERO-TRIM"
    assert post_readings(
        env.base, env.api_key, env.secret,
        [payload(vid, 1), payload(vid, 2), payload(vid, 3)],
    ).status_code == 200
    with env.engine.connect() as conn:
        first = conn.execute(
            text("SELECT min(id) FROM fleet_events WHERE vehicle_id = :v"),
            {"v": vid},
        ).scalar_one()
        conn.execute(
            text("DELETE FROM fleet_events WHERE id < :cut"), {"cut": first + 2}
        )
        conn.commit()
        min_id = conn.execute(text("SELECT min(id) FROM fleet_events")).scalar_one()
    assert min_id == first + 2 and min_id > 1

    with open_stream(env.base, last_event_id=0) as resp:
        events, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 2, timeout=4.0
        )
    assert ok, "replay from 0 stalled instead of starting at the retained head"
    assert not any(
        e.get("event") in ("stream_gap", "stream_reset") for e in events
    )
    assert data_events(events, vid)[0]["id"] == min_id


def test_unknown_last_event_id_explicit_reset(stream_server, payload):
    env = stream_server
    vid = "V-SSE-UNKNOWN"
    assert post_readings(
        env.base, env.api_key, env.secret, [payload(vid, 1)]
    ).status_code == 200
    with env.engine.connect() as conn:
        top = conn.execute(text("SELECT max(id) FROM fleet_events")).scalar_one()

    with open_stream(env.base, last_event_id=top + 10_000) as resp:
        assert resp.status_code == 200
        events, ok = collect_until(
            resp, lambda evs: any(e.get("event") == "stream_reset" for e in evs)
        )
        assert ok
        reset = next(e for e in events if e.get("event") == "stream_reset")
        assert reset["data"]["reason"] == "unknown_last_event_id"
        assert reset["data"]["latest_available_id"] == top
        assert "id" not in reset

        # The connection stays usable: new events flow after the reset.
        assert post_readings(
            env.base, env.api_key, env.secret, [payload(vid, 2)]
        ).status_code == 200
        events, ok = collect_until(
            resp, lambda evs: any(
                e.get("event") == "telemetry_update"
                and e["data"]["payload"]["seq"] == 2
                for e in evs
            )
        )
    assert ok


def test_expired_last_event_id_explicit_reset(stream_server, payload):
    env = stream_server
    vid = "V-SSE-EXPIRED"
    assert post_readings(
        env.base, env.api_key, env.secret, [payload(vid, 1), payload(vid, 2)]
    ).status_code == 200
    with env.engine.begin() as conn:
        ids = conn.execute(
            text("SELECT id FROM fleet_events WHERE vehicle_id=:v ORDER BY id"),
            {"v": vid},
        ).scalars().all()
        # Simulate retention deleting the oldest events.
        conn.execute(
            text("DELETE FROM fleet_events WHERE id = :id"), {"id": ids[0]}
        )
        oldest = conn.execute(
            text("SELECT min(id) FROM fleet_events")
        ).scalar_one()

    with open_stream(env.base, last_event_id=ids[0]) as resp:
        events, ok = collect_until(
            resp,
            lambda evs: any(e.get("event") == "stream_reset" for e in evs)
            and len(data_events(evs)) >= 1,
        )
    assert ok
    reset = next(e for e in events if e.get("event") == "stream_reset")
    assert reset["data"]["reason"] == "expired_last_event_id"
    assert reset["data"]["oldest_available_id"] == oldest
    delivered = [e["id"] for e in data_events(events)]
    assert delivered and min(delivered) == oldest


@pytest.mark.parametrize("bad", ["abc", "-5", "1.5", ""])
def test_invalid_last_event_id_rejected(stream_server, bad):
    env = stream_server
    with open_stream(env.base, last_event_id=bad) as resp:
        assert resp.status_code == 400


def test_gap_reported_when_retention_trims_lagging_cursor(isolated_client, payload):
    """Events newer than the client's cursor are deleted by retention
    before the client fetches them: the loss must be reported as an
    explicit stream_gap, never a silent skip. Slow polling makes the
    ingest-delete-ingest window deterministic."""
    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    procs, bases = start_instances(
        db_url, 1,
        {"SSE_POLL_INTERVAL_SECONDS": "1.5", "SSE_GAP_GRACE_SECONDS": "0.5"},
    )
    base = bases[0]
    try:
        vid = "V-SSE-GAP"
        assert post_readings(
            base, client.api_key, client.api_secret, [payload(vid, 1)]
        ).status_code == 200
        with engine.connect() as conn:
            first_ids = conn.execute(
                text(
                    "SELECT id FROM fleet_events WHERE vehicle_id=:v ORDER BY id"
                ),
                {"v": vid},
            ).scalars().all()

        # Resume after the first event; the first poll delivers the rest
        # and the cursor settles at the current top.
        with open_stream(base, last_event_id=first_ids[0]) as resp:
            events, ok = collect_until(
                resp, lambda evs: len(data_events(evs, vid)) >= 1, timeout=5
            )
            assert ok
            cursor = data_events(events, vid)[-1]["id"]
            time.sleep(0.4)  # server is now inside its 1.5s poll sleep

            # An event appears above the cursor, retention trims the log
            # past the cursor, then a newer event arrives — all before the
            # next poll.
            assert post_readings(
                base, client.api_key, client.api_secret, [payload(vid, 2)]
            ).status_code == 200
            with engine.begin() as conn:
                doomed = conn.execute(
                    text("SELECT max(id) FROM fleet_events")
                ).scalar_one()
                conn.execute(
                    text("DELETE FROM fleet_events WHERE id <= :id"),
                    {"id": doomed},
                )
            assert doomed > cursor
            assert post_readings(
                base, client.api_key, client.api_secret, [payload(vid, 3)]
            ).status_code == 200

            events, ok = collect_until(
                resp,
                lambda evs: any(e.get("event") == "stream_gap" for e in evs)
                and any(
                    e.get("event") == "telemetry_update"
                    and e["data"]["payload"]["seq"] == 3
                    for e in evs
                ),
                timeout=8,
            )
        assert ok
        gap = next(e for e in events if e.get("event") == "stream_gap")
        assert gap["data"]["reason"] == "retention_expired"
        assert gap["data"]["last_delivered_id"] == cursor
        assert gap["data"]["next_available_id"] == doomed + 1
        assert any(
            e.get("event") == "telemetry_update"
            and e["data"]["payload"]["seq"] == 3
            for e in events
        )
    finally:
        stop_instances(procs)


def test_in_flight_commit_is_not_a_gap(stream_server, payload):
    """Concurrent ingestion reserves event ids in order but can commit
    out of order: a poll may see id N+1 while N's transaction is still
    open. The stream must hold, then deliver N in id order — never
    declare a gap or skip the late-committed event."""
    env = stream_server
    vid = "V-SSE-INFLIGHT"
    # Seed one committed event so held_id - 1 is a real retained cursor
    # (Last-Event-ID: 0 has special "start at oldest retained id" semantics).
    with env.engine.begin() as conn:
        anchor_id = conn.execute(
            text(
                "INSERT INTO fleet_events (event_type, vehicle_id, payload) "
                "VALUES ('telemetry_update', :v, '{}'::jsonb) RETURNING id"
            ),
            {"v": vid},
        ).scalar_one()

    with env.engine.connect() as slow:
        with slow.begin():
            held_id = slow.execute(
                text(
                    "INSERT INTO fleet_events (event_type, vehicle_id, payload) "
                    "VALUES ('telemetry_update', :v, '{}'::jsonb) RETURNING id"
                ),
                {"v": vid},
            ).scalar_one()
            assert held_id == anchor_id + 1
            # Newer events commit while the first transaction stays open.
            assert post_readings(
                env.base, env.api_key, env.secret, [payload(vid, 1)]
            ).status_code == 200
            with open_stream(env.base, last_event_id=anchor_id) as resp:
                early = collect(resp, 1.0)
                assert not any(e.get("event") == "stream_gap" for e in early)
                assert not data_events(early)
                slow.commit()
                events, ok = collect_until(
                    resp, lambda evs: len(data_events(evs)) >= 3, timeout=5
                )
    assert ok
    assert not any(e.get("event") == "stream_gap" for e in events)
    ids = [e["id"] for e in data_events(events)]
    assert ids == [held_id, held_id + 1, held_id + 2]


def test_heartbeat_keeps_idle_connection_alive(stream_server):
    env = stream_server
    with open_stream(env.base) as resp:
        assert resp.status_code == 200
        deadline = time.monotonic() + 3
        saw_comment = False
        for line in resp.iter_lines():
            if line.startswith(":"):
                saw_comment = True
                break
            if time.monotonic() > deadline:
                break
    assert saw_comment


def test_alert_lifecycle_events(stream_server, payload):
    env = stream_server
    vid = "V-SSE-ALERT"
    all_events = []
    hot = [payload(vid, s, battery_temp_c=60.0) for s in range(1, 4)]
    assert post_readings(env.base, env.api_key, env.secret, hot).status_code == 200

    with open_stream(env.base, last_event_id=0) as resp:
        events, ok = collect_until(
            resp,
            lambda evs: any(e.get("event") == "alert_opened" for e in evs),
        )
        assert ok
        all_events.extend(events)
        opened = next(e for e in events if e.get("event") == "alert_opened")
        assert opened["data"]["payload"]["rule_type"] == "overheat"
        alert_id = opened["data"]["payload"]["alert_id"]

        ack = httpx.post(
            f"{env.base}/v1/alerts/{alert_id}/ack", json={"version": 1}, timeout=10
        )
        assert ack.status_code == 200
        events, ok = collect_until(
            resp,
            lambda evs: any(e.get("event") == "alert_acknowledged" for e in evs),
        )
        assert ok
        all_events.extend(events)
        acked = next(e for e in events if e.get("event") == "alert_acknowledged")
        assert acked["data"]["payload"]["alert_id"] == alert_id
        assert acked["data"]["payload"]["version"] == 2

        cool = [payload(vid, s, battery_temp_c=40.0) for s in range(4, 7)]
        assert post_readings(
            env.base, env.api_key, env.secret, cool
        ).status_code == 200
        events, ok = collect_until(
            resp,
            lambda evs: any(e.get("event") == "alert_resolved" for e in evs),
        )
    assert ok
    all_events.extend(events)
    resolved = next(e for e in all_events if e.get("event") == "alert_resolved")
    assert resolved["data"]["payload"]["alert_id"] == alert_id
    lifecycle_ids = [
        e["id"] for e in all_events
        if e.get("event") in ("alert_opened", "alert_acknowledged", "alert_resolved")
    ]
    assert len(lifecycle_ids) == 3
    assert lifecycle_ids == sorted(lifecycle_ids)


def test_offline_and_recovery_events(stream_server, payload):
    env = stream_server
    vid = "V-SSE-OFFLINE"
    assert post_readings(
        env.base, env.api_key, env.secret, [payload(vid, 1)]
    ).status_code == 200
    with env.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE vehicles SET last_seen_at = now() - interval '200 seconds' "
                "WHERE vehicle_id = :v"
            ),
            {"v": vid},
        )

    from app.alerts.worker import sweep_offline

    assert sweep_offline() >= 1

    with open_stream(env.base, last_event_id=0) as resp:
        events, ok = collect_until(
            resp,
            lambda evs: any(e.get("event") == "vehicle_offline" for e in evs),
        )
        assert ok
        offline = next(e for e in events if e.get("event") == "vehicle_offline")
        assert offline["data"]["vehicle_id"] == vid
        assert any(
            e.get("event") == "alert_opened"
            and e["data"]["payload"]["rule_type"] == "offline"
            for e in events
        )

        assert post_readings(
            env.base, env.api_key, env.secret, [payload(vid, 2)]
        ).status_code == 200
        events, ok = collect_until(
            resp,
            lambda evs: any(e.get("event") == "vehicle_recovered" for e in evs),
        )
    assert ok
    recovered = next(e for e in events if e.get("event") == "vehicle_recovered")
    assert recovered["data"]["payload"]["seq"] == 2
    assert any(
        e.get("event") == "alert_resolved"
        and e["data"]["payload"]["rule_type"] == "offline"
        for e in events
    )