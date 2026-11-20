from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
RULE = "overheat"


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def hot(payload, vid, seq):
    return payload(vid, seq, ts=BASE + timedelta(seconds=seq * 10), battery_temp_c=60)


def open_alert(client, payload, alerts_for, vid):
    post(client, [hot(payload, vid, s) for s in (1, 2, 3)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"
    return alerts[0]


def test_ack_with_correct_version(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    alert = open_alert(client, payload, alerts_for, vid)

    r = client.post(f"/v1/alerts/{alert['id']}/ack", json={"version": alert["version"]})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "acknowledged"
    assert body["version"] == alert["version"] + 1
    assert body["acknowledged_at"] is not None


def test_ack_with_stale_version_conflict(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    alert = open_alert(client, payload, alerts_for, vid)

    ok = client.post(f"/v1/alerts/{alert['id']}/ack", json={"version": alert["version"]})
    assert ok.status_code == 200
    stale = client.post(f"/v1/alerts/{alert['id']}/ack", json={"version": alert["version"]})
    assert stale.status_code == 409


def test_ack_resolved_alert_conflict(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    alert = open_alert(client, payload, alerts_for, vid)
    cool = [
        payload(vid, s, ts=BASE + timedelta(seconds=s * 10), battery_temp_c=45)
        for s in (4, 5, 6)
    ]
    post(client, cool)
    assert alerts_for(vid, rule_type=RULE)[0]["status"] == "resolved"

    r = client.post(f"/v1/alerts/{alert['id']}/ack", json={"version": alert["version"]})
    assert r.status_code == 409


def test_ack_nonexistent_alert_404(ingest):
    client, _ = ingest
    r = client.post("/v1/alerts/999999999/ack", json={"version": 1})
    assert r.status_code == 404


def test_ack_invalid_version_422(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    alert = open_alert(client, payload, alerts_for, vid)
    r = client.post(f"/v1/alerts/{alert['id']}/ack", json={"version": 0})
    assert r.status_code == 422


def test_concurrent_acks_one_wins(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    alert = open_alert(client, payload, alerts_for, vid)

    def ack(_):
        return client.post(
            f"/v1/alerts/{alert['id']}/ack", json={"version": alert["version"]}
        ).status_code

    with ThreadPoolExecutor(max_workers=10) as pool:
        codes = list(pool.map(ack, range(10)))
    assert codes.count(200) == 1
    assert codes.count(409) == 9

    final = alerts_for(vid, rule_type=RULE)[0]
    assert final["status"] == "acknowledged"
    assert final["version"] == alert["version"] + 1


def test_concurrent_triggers_single_active_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [hot(payload, vid, 1), hot(payload, vid, 2)])
    assert alerts_for(vid, rule_type=RULE) == []

    # 20 clients each deliver a different third hot reading at the same time.
    def send(seq):
        return post(client, [hot(payload, vid, seq)]).status_code

    with ThreadPoolExecutor(max_workers=20) as pool:
        codes = list(pool.map(send, range(3, 23)))
    assert all(c == 200 for c in codes)

    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"


def test_resolved_alerts_stay_in_history(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    alert = open_alert(client, payload, alerts_for, vid)
    post(client, [
        payload(vid, s, ts=BASE + timedelta(seconds=s * 10), battery_temp_c=45)
        for s in (4, 5, 6)
    ])

    resolved = alerts_for(vid, rule_type=RULE, status="resolved")
    assert len(resolved) == 1 and resolved[0]["id"] == alert["id"]
    assert alerts_for(vid, rule_type=RULE, status="open") == []