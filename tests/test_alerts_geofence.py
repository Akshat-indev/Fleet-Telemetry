"""Geofence tests use the default operating polygon (Chennai rectangle:
lat 12.8..13.3, lng 79.9..80.5). The payload default (13.0827, 80.2707)
is inside."""

RULE = "geofence_breach"


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def at(payload, vid, seq, lat, lng):
    return payload(vid, seq, lat=lat, lng=lng)


def test_inside_boundary_no_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [at(payload, vid, 1, 13.0827, 80.2707)])
    assert alerts_for(vid, rule_type=RULE) == []


def test_outside_boundary_opens_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [at(payload, vid, 1, 19.076, 72.877)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"


def test_boundary_coordinates_are_inside(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [
        at(payload, vid, 1, 12.8, 80.2),   # on the southern edge
        at(payload, vid, 2, 12.8, 79.9),   # on a vertex
    ])
    assert alerts_for(vid, rule_type=RULE) == []


def test_repeated_violation_single_active_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [at(payload, vid, i, 19.0 + i / 100, 72.8) for i in range(1, 6)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"


def test_reentry_resolves_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [at(payload, vid, 1, 19.076, 72.877)])
    post(client, [at(payload, vid, 2, 13.0827, 80.2707)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1
    assert alerts[0]["status"] == "resolved"
    assert alerts[0]["resolved_at"] is not None