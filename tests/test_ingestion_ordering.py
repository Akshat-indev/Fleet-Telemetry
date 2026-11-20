from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 11, 20, 9, 0, 0, tzinfo=timezone.utc)


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def test_newer_then_older_keeps_newer_state(ingest, payload, vehicle_state, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    newer = payload(vid, 2, ts=BASE, soc_pct=80.0)
    older = payload(vid, 1, ts=BASE - timedelta(minutes=5), soc_pct=90.0)

    assert post(client, [newer]).json()["accepted"] == 1
    assert post(client, [older]).json()["accepted"] == 1  # stored, not dropped

    state = vehicle_state(vid)
    assert state["latest_seq"] == 2
    assert state["latest_device_ts"] == BASE
    assert float(state["soc_pct"]) == 80.0
    assert reading_count(vid) == 2  # history retains the late reading


def test_older_then_newer_updates_state(ingest, payload, vehicle_state):
    client, prefix = ingest
    vid = prefix + "V1"
    older = payload(vid, 1, ts=BASE - timedelta(minutes=5), soc_pct=90.0)
    newer = payload(vid, 2, ts=BASE, soc_pct=80.0)

    post(client, [older])
    post(client, [newer])

    state = vehicle_state(vid)
    assert state["latest_seq"] == 2
    assert state["latest_device_ts"] == BASE
    assert float(state["soc_pct"]) == 80.0


def test_multiple_out_of_order_readings(ingest, payload, vehicle_state, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    readings = [
        payload(vid, seq, ts=BASE + timedelta(seconds=seq * 5), soc_pct=float(seq))
        for seq in (3, 1, 5, 2, 4)
    ]

    body = post(client, readings).json()
    assert body["accepted"] == 5

    state = vehicle_state(vid)
    assert state["latest_seq"] == 5
    assert state["latest_device_ts"] == BASE + timedelta(seconds=25)
    assert float(state["soc_pct"]) == 5.0
    assert reading_count(vid) == 5


def test_higher_seq_with_older_ts_does_not_win(ingest, payload, vehicle_state):
    """Current state follows device timestamp, NOT sequence number."""
    client, prefix = ingest
    vid = prefix + "V1"
    low_seq_new_ts = payload(vid, 1, ts=BASE, soc_pct=50.0)
    high_seq_old_ts = payload(vid, 99, ts=BASE - timedelta(minutes=1), soc_pct=10.0)

    post(client, [low_seq_new_ts])
    post(client, [high_seq_old_ts])

    state = vehicle_state(vid)
    assert state["latest_seq"] == 1
    assert state["latest_device_ts"] == BASE
    assert float(state["soc_pct"]) == 50.0


def test_last_seen_uses_server_time_even_for_late_reading(ingest, payload, vehicle_state):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 2, ts=BASE)])
    first_seen = vehicle_state(vid)["last_seen_at"]

    post(client, [payload(vid, 1, ts=BASE - timedelta(minutes=10))])
    state = vehicle_state(vid)

    assert state["last_seen_at"] >= first_seen
    assert state["latest_device_ts"] == BASE  # device state untouched
    # last_seen_at tracks arrival (now), not the 10-minutes-old device ts
    assert (datetime.now(timezone.utc) - state["last_seen_at"]).total_seconds() < 60