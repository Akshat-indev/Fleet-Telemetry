"""A5 multi-instance guarantees with TWO independent uvicorn processes
sharing one PostgreSQL event log: cross-instance delivery, cross-instance
resumption, and history that survives a full restart."""

from sqlalchemy import text

from tests.sse_harness import (
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


def test_events_from_instance_a_reach_clients_on_b(two_stream_instances, payload):
    env = two_stream_instances
    a, b = env.bases
    vid = "V-MI-SSE"
    assert post_readings(a, env.api_key, env.secret, [payload(vid, 1)]).status_code == 200

    with open_stream(b, last_event_id=0) as resp:
        events, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 2
        )
    assert ok
    assert {e["event"] for e in data_events(events, vid)} >= {
        "telemetry_update", "vehicle_status_change"
    }


def test_reconnect_to_other_instance_resumes_exactly(two_stream_instances, payload):
    env = two_stream_instances
    a, b = env.bases
    vid = "V-MI-RESUME"
    assert post_readings(
        a, env.api_key, env.secret, [payload(vid, 1), payload(vid, 2)]
    ).status_code == 200

    with open_stream(a, last_event_id=0) as resp:
        first, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 3
        )
    assert ok
    seen = data_events(first, vid)
    last_id = seen[-1]["id"]

    # More traffic lands on instance A while the client is away.
    assert post_readings(
        a, env.api_key, env.secret,
        [payload(vid, 3), payload(vid, 4), payload(vid, 5)],
    ).status_code == 200

    # Resume on instance B — a different process, same log.
    with open_stream(b, last_event_id=last_id) as resp:
        second, ok = collect_until(
            resp, lambda evs: len(data_events(evs, vid)) >= 3
        )
    assert ok
    resumed = data_events(second, vid)
    assert resumed[0]["id"] == last_id + 1
    all_ids = [e["id"] for e in seen + resumed]
    assert len(all_ids) == len(set(all_ids))
    with env.engine.connect() as conn:
        expected = conn.execute(
            text("SELECT id FROM fleet_events WHERE vehicle_id=:v ORDER BY id"),
            {"v": vid},
        ).scalars().all()
    assert all_ids == expected


def test_history_survives_full_restart(two_stream_instances, payload):
    env = two_stream_instances
    a, b = env.bases
    vid = "V-MI-RESTART"
    assert post_readings(
        a, env.api_key, env.secret, [payload(vid, 1), payload(vid, 2)]
    ).status_code == 200
    with env.engine.connect() as conn:
        expected = conn.execute(
            text("SELECT id FROM fleet_events WHERE vehicle_id=:v ORDER BY id"),
            {"v": vid},
        ).scalars().all()

    # Kill BOTH instances. Nothing survives in process memory.
    stop_instances(env.procs)
    env.procs.clear()

    db_url = env.engine.url.render_as_string(hide_password=False)
    procs, bases = start_instances(db_url, 1)
    try:
        with open_stream(bases[0], last_event_id=0) as resp:
            events, ok = collect_until(
                resp, lambda evs: len(data_events(evs, vid)) >= len(expected)
            )
        assert ok
        assert [e["id"] for e in data_events(events, vid)] == expected

        # The restarted instance also accepts new traffic whose events
        # continue the same id sequence.
        assert post_readings(
            bases[0], env.api_key, env.secret, [payload(vid, 3)]
        ).status_code == 200
        with open_stream(bases[0], last_event_id=expected[-1]) as resp:
            events, ok = collect_until(
                resp, lambda evs: len(data_events(evs, vid)) >= 1
            )
        assert ok
        assert data_events(events, vid)[0]["id"] == expected[-1] + 1
    finally:
        stop_instances(procs)