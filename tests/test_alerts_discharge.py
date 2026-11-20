from datetime import datetime, timedelta, timezone
from decimal import Decimal

BASE = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
RULE = "rapid_discharge"


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def soc_at(payload, vid, seq, seconds, soc, charging=False):
    return payload(
        vid, seq, ts=BASE + timedelta(seconds=seconds), soc_pct=soc, charging=charging
    )


def test_drop_of_exactly_8_does_not_trigger(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [soc_at(payload, vid, 1, 0, 90), soc_at(payload, vid, 2, 240, 82)])
    assert alerts_for(vid, rule_type=RULE) == []


def test_drop_over_8_within_window_triggers(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [soc_at(payload, vid, 1, 0, 90), soc_at(payload, vid, 2, 240, 81.9)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"
    assert Decimal(alerts[0]["details"]["drop_pct"]) == Decimal("8.1")


def test_charging_reading_does_not_trigger(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [
        soc_at(payload, vid, 1, 0, 90),
        soc_at(payload, vid, 2, 240, 80, charging=True),
    ])
    assert alerts_for(vid, rule_type=RULE) == []


def test_charging_in_interval_breaks_discharge(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [
        soc_at(payload, vid, 1, 0, 90),
        soc_at(payload, vid, 2, 120, 85, charging=True),
        soc_at(payload, vid, 3, 240, 80),
    ])
    assert alerts_for(vid, rule_type=RULE) == []


def test_outside_five_minute_window_does_not_trigger(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [soc_at(payload, vid, 1, 0, 90), soc_at(payload, vid, 2, 360, 80)])
    assert alerts_for(vid, rule_type=RULE) == []


def test_resolves_when_drop_falls_below_threshold(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    post(client, [soc_at(payload, vid, 1, 0, 90), soc_at(payload, vid, 2, 240, 81.9)])
    assert alerts_for(vid, rule_type=RULE)[0]["status"] == "open"

    # 6 minutes after the high reading: it left the 5-minute window.
    post(client, [soc_at(payload, vid, 3, 360, 81.5)])
    alerts = alerts_for(vid, rule_type=RULE)
    assert alerts[0]["status"] == "resolved"


def test_out_of_order_arrival_same_result(ingest, payload, alerts_for):
    client, prefix = ingest
    vid = prefix + "V1"
    # The low (later) reading arrives first.
    body = post(client, [
        soc_at(payload, vid, 2, 240, 81.9), soc_at(payload, vid, 1, 0, 90),
    ]).json()
    assert body["accepted"] == 2
    alerts = alerts_for(vid, rule_type=RULE)
    assert len(alerts) == 1 and alerts[0]["status"] == "open"


def test_duplicate_does_not_retrigger(ingest, payload, alerts_for, signed_post):
    client, prefix = ingest
    vid = prefix + "V1"
    batch = [soc_at(payload, vid, 1, 0, 90), soc_at(payload, vid, 2, 240, 81.9)]
    signed_post(client, batch)
    body = signed_post(client, batch, offset=-1).json()
    assert body["duplicates"] == 2
    assert len(alerts_for(vid, rule_type=RULE)) == 1