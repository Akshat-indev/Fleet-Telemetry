from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def hot(payload, vid, seq, seconds):
    return payload(vid, seq, ts=BASE + timedelta(seconds=seconds), battery_temp_c=60)


def cool(payload, vid, seq, seconds):
    return payload(vid, seq, ts=BASE + timedelta(seconds=seconds), battery_temp_c=45)


def test_two_hot_readings_no_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [hot(payload, vid, 1, 0), hot(payload, vid, 2, 5)])
    assert alerts_for(vid, rule_type="overheat") == []


def test_third_hot_reading_opens_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [hot(payload, vid, i, i * 5) for i in range(1, 4)])
    alerts = alerts_for(vid, rule_type="overheat")
    assert len(alerts) == 1
    assert alerts[0]["status"] == "open"


def test_resolution_needs_three_cool_readings(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [hot(payload, vid, i, i * 5) for i in range(1, 4)])
    post(client, [cool(payload, vid, 4, 20), cool(payload, vid, 5, 25)])
    assert alerts_for(vid, rule_type="overheat")[0]["status"] == "open"

    post(client, [cool(payload, vid, 6, 30)])
    alerts = alerts_for(vid, rule_type="overheat")
    assert alerts[0]["status"] == "resolved"
    assert alerts[0]["resolved_at"] is not None


def test_neutral_reading_does_not_reset_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [hot(payload, vid, i, i * 5) for i in range(1, 4)])
    post(client, [payload(vid, 4, ts=BASE + timedelta(seconds=20), battery_temp_c=52)])
    assert alerts_for(vid, rule_type="overheat")[0]["status"] == "open"


def test_out_of_order_hot_readings_single_alert(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    readings = [hot(payload, vid, 3, 10), hot(payload, vid, 1, 0), hot(payload, vid, 2, 5)]
    body = post(client, readings).json()
    assert body["accepted"] == 3
    alerts = alerts_for(vid, rule_type="overheat")
    assert len(alerts) == 1 and alerts[0]["status"] == "open"


def test_duplicate_hot_batch_no_second_alert(ingest, payload, alerts_for, signed_post):
    client, prefix = ingest
    vid = prefix + "V1"
    batch = [hot(payload, vid, i, i * 5) for i in range(1, 4)]
    signed_post(client, batch)
    body = signed_post(client, batch, offset=-1).json()
    assert body["duplicates"] == 3
    assert len(alerts_for(vid, rule_type="overheat")) == 1


def test_recurring_condition_after_resolution_creates_new_alert(
    ingest, payload, alerts_for
):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [hot(payload, vid, i, i * 5) for i in range(1, 4)])
    post(client, [cool(payload, vid, i, i * 5) for i in range(4, 7)])
    first = alerts_for(vid, rule_type="overheat")[0]
    assert first["status"] == "resolved"

    post(client, [hot(payload, vid, i, i * 5) for i in range(7, 10)])
    alerts = alerts_for(vid, rule_type="overheat")
    assert len(alerts) == 2  # resolved one stays in history
    active = [a for a in alerts if a["status"] != "resolved"]
    assert len(active) == 1 and active[0]["id"] != first["id"]


def test_out_of_order_neutral_before_third_hot_still_opens_alert(
    ingest, payload, alerts_for
):
    """Order-independence for hysteresis: [60@0, 60@5, 60@10, 52@15] must
    end with an open alert even if 52@15 arrives before 60@10."""
    client, prefix = ingest
    vid = prefix + "OOOHYST-OPEN"
    post(client, [
        hot(payload, vid, 1, 0),
        hot(payload, vid, 2, 5),
        payload(vid, 4, ts=BASE + timedelta(seconds=15), battery_temp_c=52),
    ])
    assert alerts_for(vid, rule_type="overheat") == []

    # Late 3rd hot reading completes [60, 60, 60] behind the neutral 52.
    post(client, [hot(payload, vid, 3, 10)])
    alerts = alerts_for(vid, rule_type="overheat")
    assert len(alerts) == 1
    assert alerts[0]["status"] == "open"


def test_out_of_order_neutral_before_third_cool_still_resolves_alert(
    ingest, payload, alerts_for
):
    """Order-independence for hysteresis resolution: [60x3, 45@20, 45@25,
    45@30, 52@35] must end resolved even if 52@35 arrives before 45@30."""
    client, prefix = ingest
    vid = prefix + "OOOHYST-RESOLVE"
    post(client, [hot(payload, vid, i, (i - 1) * 5) for i in range(1, 4)])
    assert alerts_for(vid, rule_type="overheat")[0]["status"] == "open"

    post(client, [
        cool(payload, vid, 4, 20),
        cool(payload, vid, 5, 25),
        payload(vid, 7, ts=BASE + timedelta(seconds=35), battery_temp_c=52),
    ])
    assert alerts_for(vid, rule_type="overheat")[0]["status"] == "open"

    # Late 3rd cool reading completes [45, 45, 45] behind the neutral 52.
    post(client, [cool(payload, vid, 6, 30)])
    alerts = alerts_for(vid, rule_type="overheat")
    assert len(alerts) == 1
    assert alerts[0]["status"] == "resolved"