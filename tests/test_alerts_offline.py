"""Offline detection is driven by the background sweep, called directly here
(TestClient does not run the lifespan, so the watchdog task never starts
during tests — results are deterministic).

The sweep touches every stale vehicle in the database, so assertions are
scoped to this test's vehicle id, never to global alert counts."""

from app.alerts.worker import sweep_offline

RULE = "offline"


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def test_fresh_vehicle_no_offline_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1)])
    sweep_offline()
    assert alerts_for(vid, rule_type=RULE) == []


def test_stale_vehicle_opens_offline_alert(ingest, payload, alerts_for, backdate_last_seen):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1)])
    backdate_last_seen(vid, 120)
    opened = sweep_offline()
    assert opened >= 1
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"
    assert alerts[0]["details"]["last_seen_at"] is not None


def test_repeated_sweep_does_not_duplicate(ingest, payload, alerts_for, backdate_last_seen):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1)])
    backdate_last_seen(vid, 120)
    sweep_offline()
    sweep_offline()
    sweep_offline()
    assert len(alerts_for(vid, rule_type=RULE)) == 1


def test_new_reading_resolves_offline(ingest, payload, alerts_for, backdate_last_seen):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1)])
    backdate_last_seen(vid, 120)
    sweep_offline()
    assert alerts_for(vid, rule_type=RULE)[0]["status"] == "open"

    post(client, [payload(vid, 2)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert alerts[0]["status"] == "resolved"
    assert alerts[0]["resolved_at"] is not None


def test_duplicate_reading_also_resolves_offline(ingest, payload, alerts_for, backdate_last_seen):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1)])
    backdate_last_seen(vid, 120)
    sweep_offline()

    body = post(client, [payload(vid, 1)]).json()
    assert body["duplicates"] == 1
    assert alerts_for(vid, rule_type=RULE)[0]["status"] == "resolved"


def test_goes_offline_again_after_resolution(ingest, payload, alerts_for, backdate_last_seen):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [payload(vid, 1)])
    backdate_last_seen(vid, 120)
    sweep_offline()
    post(client, [payload(vid, 2)])
    assert alerts_for(vid, rule_type=RULE)[0]["status"] == "resolved"

    backdate_last_seen(vid, 120)
    sweep_offline()
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 2
    assert {a["status"] for a in alerts} == {"open", "resolved"}