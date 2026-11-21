"""Transactional outbox guarantees: events exist exactly when their
causing state change commits, and never otherwise. Uses the real scratch
database; the only monkeypatch is a fault injection that forces a
rollback mid-transaction."""

from sqlalchemy import text


def events_for(engine, vehicle_id):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT id, event_type, payload FROM fleet_events "
                "WHERE vehicle_id = :v ORDER BY id"
            ),
            {"v": vehicle_id},
        ).mappings().all()


def test_telemetry_update_published_only_on_insert(
    isolated_client, payload, signed_post
):
    client, engine = isolated_client
    vid = "V-PUB-INSERT"
    reading = payload(vid, 1)
    assert signed_post(client, [reading], offset=0).status_code == 200
    first = events_for(engine, vid)
    assert any(e["event_type"] == "telemetry_update" for e in first)

    # Same reading again (fresh signature): data-level duplicate.
    r = signed_post(client, [reading], offset=1)
    assert r.status_code == 200
    assert r.json()["duplicates"] == 1
    assert events_for(engine, vid) == first, "duplicate emitted an event"


def test_rejected_reading_publishes_nothing(isolated_client, payload):
    client, engine = isolated_client
    vid = "V-PUB-REJECT"
    r = client.post(
        "/v1/telemetry/batch",
        json={"readings": [payload(vid, 1, soc_pct=150.0)]},
    )
    assert r.status_code == 200
    assert r.json()["rejected"] == 1
    assert events_for(engine, vid) == []


def test_rollback_leaves_no_events(isolated_client, payload, monkeypatch):
    client, engine = isolated_client
    vid = "V-PUB-ROLLBACK"

    import pytest

    import app.ingestion as ingestion_module

    original = ingestion_module.evaluate_reading

    def boom(*args, **kwargs):
        raise RuntimeError("injected failure after publish")

    monkeypatch.setattr(ingestion_module, "evaluate_reading", boom)
    with pytest.raises(RuntimeError, match="injected failure"):
        client.post("/v1/telemetry/batch", json={"readings": [payload(vid, 1)]})
    assert events_for(engine, vid) == [], "event survived a rollback"
    with engine.connect() as conn:
        stored = conn.execute(
            text(
                "SELECT count(*) FROM telemetry_readings t "
                "JOIN vehicles v ON v.id=t.vehicle_id WHERE v.vehicle_id=:v"
            ),
            {"v": vid},
        ).scalar_one()
    assert stored == 0

    monkeypatch.setattr(ingestion_module, "evaluate_reading", original)
    assert client.post(
        "/v1/telemetry/batch", json={"readings": [payload(vid, 1)]}
    ).status_code == 200
    assert any(
        e["event_type"] == "telemetry_update" for e in events_for(engine, vid)
    )


def test_status_change_on_charging_transition(isolated_client, payload):
    client, engine = isolated_client
    vid = "V-PUB-STATUS"
    assert client.post(
        "/v1/telemetry/batch", json={"readings": [payload(vid, 1)]}
    ).status_code == 200
    assert client.post(
        "/v1/telemetry/batch",
        json={"readings": [payload(vid, 2, charging=True, speed_kmh=0.0)]},
    ).status_code == 200
    changes = [
        e for e in events_for(engine, vid)
        if e["event_type"] == "vehicle_status_change"
    ]
    assert [(c["payload"]["from"], c["payload"]["to"]) for c in changes] == [
        ("offline", "active"),
        ("active", "charging"),
    ]


def test_telemetry_payload_matches_reading(isolated_client, payload):
    client, engine = isolated_client
    vid = "V-PUB-PAYLOAD"
    reading = payload(vid, 7, soc_pct=64.25, fault_codes=["BMS_WARN"])
    assert client.post(
        "/v1/telemetry/batch", json={"readings": [reading]}
    ).status_code == 200
    tel = next(
        e for e in events_for(engine, vid) if e["event_type"] == "telemetry_update"
    )
    p = tel["payload"]
    assert p["seq"] == 7
    assert p["soc_pct"] == 64.25
    assert p["fault_codes"] == ["BMS_WARN"]
    assert p["charging"] is False
    assert set(p) == {
        "seq", "ts", "soc_pct", "battery_temp_c", "speed_kmh",
        "lat", "lng", "odometer_km", "charging", "fault_codes",
    }


def test_duplicate_reading_still_proves_liveness(isolated_client, payload, signed_post):
    """A duplicate resolves an offline alert and emits recovery events,
    but no telemetry_update (history did not grow)."""
    client, engine = isolated_client
    vid = "V-PUB-LIVENESS"
    reading = payload(vid, 1)
    assert signed_post(client, [reading], offset=0).status_code == 200
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE vehicles SET last_seen_at = now() - interval '200 seconds' "
                "WHERE vehicle_id=:v"
            ),
            {"v": vid},
        )
    from app.alerts.worker import sweep_offline

    assert sweep_offline() >= 1
    before = events_for(engine, vid)
    assert any(e["event_type"] == "vehicle_offline" for e in before)

    assert signed_post(client, [reading], offset=1).json()["duplicates"] == 1
    after = events_for(engine, vid)
    new = after[len(before):]
    types = [e["event_type"] for e in new]
    assert "vehicle_recovered" in types
    assert "alert_resolved" in types
    assert "telemetry_update" not in types