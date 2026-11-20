"""Fleet summary against a fresh scratch database: exact aggregates,
empty fleet, null handling, and consistency with the listing endpoint."""

from sqlalchemy import text

from app.alerts.worker import sweep_offline


def seed(client, payload, readings):
    r = client.post("/v1/telemetry/batch", json={"readings": readings})
    assert r.status_code == 200
    assert r.json()["rejected"] == 0


def test_empty_fleet_summary(isolated_client):
    client, _ = isolated_client
    r = client.get("/v1/fleet/summary")
    assert r.status_code == 200
    assert r.json() == {
        "vehicles_total": 0,
        "vehicles_by_status": {"offline": 0, "charging": 0, "active": 0, "idle": 0},
        "avg_soc_pct": None,
        "open_alerts_by_rule": {
            "overheat": 0, "rapid_discharge": 0, "geofence_breach": 0, "offline": 0
        },
        "readings_total": 0,
        "data_completeness_pct": None,
    }


def test_exact_aggregates_with_gap_and_nulls(isolated_client, payload):
    client, engine = isolated_client
    seed(client, payload, [
        payload("V-A", 1, soc_pct=80, speed_kmh=30),
        payload("V-A", 2, soc_pct=80, speed_kmh=30),
        payload("V-A", 3, soc_pct=80, speed_kmh=30),
        payload("V-B", 1, soc_pct=60, charging=True),
        payload("V-B", 3, soc_pct=60, charging=True),  # seq 2 missing
        payload("V-C", 1, soc_pct=40, speed_kmh=0),
    ])
    # Vehicle that never sent a reading: no state, counts as offline.
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO vehicles (vehicle_id) VALUES ('V-EMPTY')"))

    body = client.get("/v1/fleet/summary").json()
    assert body["vehicles_total"] == 4
    assert body["vehicles_by_status"] == {
        "offline": 1, "charging": 1, "active": 1, "idle": 1
    }
    assert body["avg_soc_pct"] == 60.0  # (80+60+40)/3; null excluded
    assert body["readings_total"] == 6
    # received 6, expected 3 + 3 + 1 = 7
    assert body["data_completeness_pct"] == 85.71


def test_open_alerts_by_rule(isolated_client, payload):
    client, engine = isolated_client
    hot = [payload("V-HOT", s, battery_temp_c=60) for s in (1, 2, 3)]
    outside = [payload("V-OUT", 1, lat=19.076, lng=72.877)]
    seed(client, payload, hot + outside)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE vehicles SET last_seen_at = now() - interval '120 seconds' "
                 "WHERE vehicle_id = 'V-OUT'")
        )
    assert sweep_offline() >= 1

    body = client.get("/v1/fleet/summary").json()
    assert body["open_alerts_by_rule"] == {
        "overheat": 1, "rapid_discharge": 0, "geofence_breach": 1, "offline": 1
    }

    # Acknowledged alerts are no longer "open".
    alerts = client.get("/v1/alerts", params={"rule_type": "overheat"}).json()
    r = client.post(f"/v1/alerts/{alerts[0]['id']}/ack",
                    json={"version": alerts[0]["version"]})
    assert r.status_code == 200
    body = client.get("/v1/fleet/summary").json()
    assert body["open_alerts_by_rule"]["overheat"] == 0


def test_summary_consistent_with_listing(isolated_client, payload):
    client, engine = isolated_client
    seed(client, payload, [
        payload("V-1", 1, charging=True),
        payload("V-2", 1, speed_kmh=0),
        payload("V-3", 1),
        payload("V-4", 1),
    ])
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE vehicles SET last_seen_at = now() - interval '120 seconds' "
                 "WHERE vehicle_id IN ('V-3', 'V-4')")
        )
    summary = client.get("/v1/fleet/summary").json()
    listed = client.get("/v1/vehicles", params={"limit": 200}).json()["vehicles"]
    counts = {s: 0 for s in summary["vehicles_by_status"]}
    for v in listed:
        counts[v["status"]] += 1
    assert counts == summary["vehicles_by_status"]
    assert summary["vehicles_total"] == len(listed)


def test_completeness_reaches_100_when_gap_filled(isolated_client, payload):
    client, _ = isolated_client
    seed(client, payload, [payload("V-G", 1), payload("V-G", 3)])
    assert client.get("/v1/fleet/summary").json()["data_completeness_pct"] == 66.67
    seed(client, payload, [payload("V-G", 2)])
    assert client.get("/v1/fleet/summary").json()["data_completeness_pct"] == 100.0


def test_duplicate_does_not_change_summary(isolated_client, payload, signed_post):
    client, _ = isolated_client
    reading = payload("V-D", 1)
    seed(client, payload, [reading])
    before = client.get("/v1/fleet/summary").json()
    r = signed_post(client, [reading], offset=-1)
    assert r.status_code == 200 and r.json()["duplicates"] == 1
    after = client.get("/v1/fleet/summary").json()
    assert before == after


def test_summary_matches_direct_sql(isolated_client, payload):
    """Cross-check every summary field against independent SQL — including
    completeness derived from raw readings, not the maintained counters."""
    client, engine = isolated_client
    seed(client, payload, [
        payload("V-A", 1, soc_pct=80, speed_kmh=30),
        payload("V-A", 2, soc_pct=80, speed_kmh=30),
        payload("V-A", 4, soc_pct=80, speed_kmh=30),  # gap at 3
        payload("V-B", 1, soc_pct=60, charging=True),
        payload("V-C", 1, soc_pct=40, speed_kmh=0),
        payload("V-HOT", 1, soc_pct=90, battery_temp_c=60),
        payload("V-HOT", 2, soc_pct=90, battery_temp_c=60),
        payload("V-HOT", 3, soc_pct=90, battery_temp_c=60),
    ])
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE vehicles SET last_seen_at = now() - interval '120 seconds' "
                 "WHERE vehicle_id = 'V-C'")
        )
        conn.execute(text("INSERT INTO vehicles (vehicle_id) VALUES ('V-EMPTY')"))

        expected_total = conn.execute(text("SELECT count(*) FROM vehicles")).scalar_one()
        expected_avg = conn.execute(text("SELECT avg(soc_pct) FROM vehicles")).scalar_one()
        offline = conn.execute(text(
            "SELECT count(*) FROM vehicles WHERE last_seen_at IS NULL "
            "OR last_seen_at < now() - interval '90 seconds'"
        )).scalar_one()
        charging = conn.execute(text(
            "SELECT count(*) FROM vehicles WHERE charging "
            "AND last_seen_at >= now() - interval '90 seconds'"
        )).scalar_one()
        active = conn.execute(text(
            "SELECT count(*) FROM vehicles WHERE speed_kmh > 0 AND NOT charging "
            "AND last_seen_at >= now() - interval '90 seconds'"
        )).scalar_one()
        expected_alerts = dict(conn.execute(text(
            "SELECT rule_type, count(*) FROM alerts WHERE status = 'open' "
            "GROUP BY rule_type"
        )).all())
        # Completeness from raw history, independent of the seq counters.
        rows = conn.execute(text(
            "SELECT count(*) AS n, min(seq) AS lo, max(seq) AS hi "
            "FROM telemetry_readings GROUP BY vehicle_id"
        )).all()
        received = sum(r.n for r in rows)
        expected_span = sum(r.hi - r.lo + 1 for r in rows)

    body = client.get("/v1/fleet/summary").json()
    assert body["vehicles_total"] == expected_total
    assert body["avg_soc_pct"] == round(float(expected_avg), 2)
    assert body["vehicles_by_status"]["offline"] == offline
    assert body["vehicles_by_status"]["charging"] == charging
    assert body["vehicles_by_status"]["active"] == active
    assert body["vehicles_by_status"]["idle"] == (
        expected_total - offline - charging - active
    )
    assert body["readings_total"] == received
    assert body["data_completeness_pct"] == round(100.0 * received / expected_span, 2)
    for rule, count in body["open_alerts_by_rule"].items():
        assert count == expected_alerts.get(rule, 0)
    assert body["open_alerts_by_rule"]["overheat"] == 1