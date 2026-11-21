import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.security import compute_signature


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def post_raw_json(client, raw_bytes: bytes):
    ts = str(int(time.time()))
    sig = compute_signature(client.api_secret, client.api_key, ts, raw_bytes)
    return client.post(
        "/v1/telemetry/batch",
        content=raw_bytes,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": client.api_key,
            "X-Timestamp": ts,
            "X-Signature": sig,
        },
    )


def test_valid_reading_accepted(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    response = post(client, [payload(vid, 1)])

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 1 and body["rejected"] == 0
    assert body["results"][0]["status"] == "accepted"
    assert reading_count(vid) == 1


@pytest.mark.parametrize("soc", [0, 0.0, 50.5, 100, 100.0])
def test_soc_boundary_valid_values_accepted(ingest, payload, reading_count, soc):
    client, prefix = ingest
    vid = f"{prefix}SOC-{str(soc).replace('.', '_')}"
    response = post(client, [payload(vid, 1, soc_pct=soc)])

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 1 and body["rejected"] == 0
    assert body["results"][0]["status"] == "accepted"
    assert reading_count(vid) == 1


@pytest.mark.parametrize("soc", [101, 100.01, 140, -1, -0.01, None])
def test_soc_out_of_range_or_null_rejected(ingest, payload, reading_count, vehicle_state, soc):
    client, prefix = ingest
    vid = f"{prefix}BADSOC-{str(soc).replace('.', '_')}"
    response = post(client, [payload(vid, 1, soc_pct=soc)])

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 0 and body["rejected"] == 1
    result = body["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"] == "soc_pct" for e in result["errors"])
    assert reading_count(vid) == 0
    assert vehicle_state(vid) is None


def test_missing_required_field_rejected_individually(ingest, payload):
    client, prefix = ingest
    for field in (
        "vehicle_id",
        "seq",
        "ts",
        "soc_pct",
        "battery_temp_c",
        "speed_kmh",
        "lat",
        "lng",
        "odometer_km",
        "charging",
    ):
        bad = payload(prefix + "V1", 1)
        del bad[field]
        response = post(client, [bad])
        assert response.status_code == 200
        result = response.json()["results"][0]
        assert result["status"] == "rejected"
        assert any(e["field"] == field for e in result["errors"])


@pytest.mark.parametrize(
    "field,value",
    [
        ("soc_pct", 101),
        ("soc_pct", 140),
        ("soc_pct", -1),
        ("lat", 90.0001),
        ("lat", 95),
        ("lat", -90.0001),
        ("lat", -95),
        ("lng", 180.0001),
        ("lng", 181),
        ("lng", -180.0001),
        ("lng", -181),
        ("speed_kmh", -0.01),
        ("speed_kmh", -5),
        ("speed_kmh", 10000),
        ("speed_kmh", 9999.999),
        ("odometer_km", -0.01),
        ("odometer_km", -1),
        ("odometer_km", 10_000_000_000),
        ("odometer_km", 9_999_999_999.999),
        ("seq", 0),
        ("seq", -3),
        ("battery_temp_c", 150.01),
        ("battery_temp_c", 200),
        ("battery_temp_c", -50.01),
        ("battery_temp_c", -80),
        ("fault_codes", "P0A80"),
        ("fault_codes", [123]),
        ("fault_codes", [True]),
        ("fault_codes", [""]),
        ("fault_codes", ["   "]),
        ("fault_codes", None),
        ("vehicle_id", ""),
        ("vehicle_id", "   "),
    ],
)
def test_invalid_values_rejected(ingest, payload, reading_count, field, value):
    client, prefix = ingest
    vid = prefix + "V1"
    reading = payload(vid, 1)
    reading[field] = value
    response = post(client, [reading])

    assert response.status_code == 200
    result = response.json()["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"].split(".")[0] == field for e in result["errors"])
    assert reading_count(vid) == 0


@pytest.mark.parametrize(
    "field,value",
    [
        # Strings must not bypass numeric / integer / boolean validation.
        ("soc_pct", "50"),
        ("soc_pct", "100"),
        ("soc_pct", "NaN"),
        ("soc_pct", "Infinity"),
        ("soc_pct", "-Infinity"),
        ("soc_pct", True),
        ("soc_pct", False),
        ("battery_temp_c", "35.5"),
        ("battery_temp_c", True),
        ("battery_temp_c", None),
        ("speed_kmh", "25"),
        ("speed_kmh", True),
        ("speed_kmh", False),
        ("speed_kmh", None),
        ("odometer_km", "1200.5"),
        ("odometer_km", False),
        ("odometer_km", None),
        ("lat", "13.08"),
        ("lat", True),
        ("lat", None),
        ("lng", "80.27"),
        ("lng", False),
        ("lng", None),
        ("seq", "1"),
        ("seq", True),
        ("seq", False),
        ("seq", 1.0),
        ("seq", 1.5),
        ("seq", None),
        ("charging", "true"),
        ("charging", "false"),
        ("charging", "yes"),
        ("charging", 1),
        ("charging", 0),
        ("charging", None),
        ("vehicle_id", 12345),
        ("vehicle_id", True),
        ("vehicle_id", None),
    ],
)
def test_invalid_types_rejected(ingest, payload, reading_count, field, value):
    client, prefix = ingest
    vid = prefix + "TYPE"
    reading = payload(vid, 1)
    reading[field] = value
    response = post(client, [reading])

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 0 and body["rejected"] == 1
    result = body["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"].split(".")[0] == field for e in result["errors"])
    assert reading_count(vid) == 0


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_json_numbers_rejected(ingest, payload, reading_count, token):
    client, prefix = ingest
    vid = prefix + "NONFIN"
    valid = payload(vid, 1, soc_pct=50.0)
    raw_str = json.dumps({"readings": [valid]}).replace("50.0", token, 1)
    response = post_raw_json(client, raw_str.encode())

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 0 and body["rejected"] == 1
    result = body["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"] == "soc_pct" for e in result["errors"])
    assert reading_count(vid) == 0


@pytest.mark.parametrize(
    "bad_ts",
    [
        "2026-11-20T10:00:00",        # naive ISO string (no timezone)
        "2026-11-20",                  # date only
        "not-a-timestamp",             # malformed string
        "2026-13-45T99:99:99Z",        # impossible calendar date
        "",                            # empty string
        "1700000000",                  # numeric epoch string
        1700000000,                    # integer epoch
        0,                             # zero epoch
        1700000000.5,                  # float epoch
        True,                          # boolean
        None,                          # null
    ],
)
def test_malformed_timestamps_rejected(ingest, payload, reading_count, bad_ts):
    client, prefix = ingest
    vid = prefix + "TS"
    reading = payload(vid, 1)
    reading["ts"] = bad_ts
    response = post(client, [reading])

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 0 and body["rejected"] == 1
    result = body["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"] == "ts" for e in result["errors"])
    assert reading_count(vid) == 0


def test_cold_and_overheating_temperatures_accepted_for_alert_engine(
    ingest, payload, alerts_for
):
    """Abnormal operating temperatures inside [-50, 150] C are valid telemetry
    so the alert engine can detect overheating."""
    client, prefix = ingest
    vid = prefix + "TEMP"
    base = datetime(2026, 11, 20, 10, 0, 0, tzinfo=timezone.utc)

    # Cold weather reading (-25 C) accepted
    r_cold = post(client, [payload(vid, 1, ts=base, battery_temp_c=-25.0)])
    assert r_cold.json()["results"][0]["status"] == "accepted"

    # Three consecutive readings above 55 C alert trigger (e.g. 68 C, 90 C, 120 C)
    # are accepted as valid telemetry and open an overheat alert.
    hot_batch = [
        payload(vid, 2, ts=base + timedelta(seconds=10), battery_temp_c=68.0),
        payload(vid, 3, ts=base + timedelta(seconds=20), battery_temp_c=90.0),
        payload(vid, 4, ts=base + timedelta(seconds=30), battery_temp_c=120.0),
    ]
    r_hot = post(client, hot_batch)
    assert r_hot.json()["accepted"] == 3
    assert r_hot.json()["rejected"] == 0
    alerts = alerts_for(vid, rule_type="overheat", status="open")
    assert len(alerts) == 1


def test_non_object_reading_rejected_individually(ingest, payload):
    client, prefix = ingest
    response = post(client, ["garbage", payload(prefix + "V1", 1)])
    body = response.json()
    assert body["results"][0]["status"] == "rejected"
    assert body["results"][1]["status"] == "accepted"


def test_batch_over_500_rejected(ingest, payload):
    client, prefix = ingest
    readings = [payload(prefix + "V1", i) for i in range(1, 502)]
    assert post(client, readings).status_code == 422


def test_empty_batch_rejected(ingest):
    client, _ = ingest
    assert post(client, []).status_code == 422


def test_mixed_batch_partial_success_and_clear_reasons(
    ingest, payload, reading_count, vehicle_state
):
    client, prefix = ingest
    vid = prefix + "MIX"
    base = datetime(2026, 11, 20, 11, 0, 0, tzinfo=timezone.utc)
    readings = [
        payload(vid, 1, ts=base, soc_pct=75),
        payload(vid, 2, ts=base + timedelta(seconds=5), soc_pct=101),
        payload(vid, 3, ts=base + timedelta(seconds=10), soc_pct=45),
        payload(vid, 1, ts=base, soc_pct=75),  # duplicate of reading 1
    ]
    response = post(client, readings)

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 2
    assert body["duplicates"] == 1
    assert body["rejected"] == 1
    assert [r["status"] for r in body["results"]] == [
        "accepted",
        "rejected",
        "accepted",
        "duplicate",
    ]
    assert any(e["field"] == "soc_pct" for e in body["results"][1]["errors"])
    assert reading_count(vid) == 2

    state = vehicle_state(vid)
    assert state["latest_seq"] == 3
    assert state["soc_pct"] == Decimal("45.00")
    assert state["received_count"] == 2


def test_invalid_reading_never_updates_existing_vehicle_state_or_history(
    ingest, payload, reading_count, vehicle_state
):
    client, prefix = ingest
    vid = prefix + "IMMUT"
    base = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)

    # Establish valid vehicle state at seq=1, soc=72.5
    ok_resp = post(client, [payload(vid, 1, ts=base, soc_pct=72.5, speed_kmh=30.0)])
    assert ok_resp.status_code == 200
    assert ok_resp.json()["accepted"] == 1
    before = vehicle_state(vid)
    assert reading_count(vid) == 1

    # Submit newer timestamp with invalid SOC=101, negative speed, invalid lat, etc.
    bad_batch = [
        payload(vid, 2, ts=base + timedelta(seconds=10), soc_pct=101),
        payload(vid, 3, ts=base + timedelta(seconds=20), soc_pct=-1),
        payload(vid, 4, ts=base + timedelta(seconds=30), lat=95.0),
        payload(vid, 5, ts=base + timedelta(seconds=40), speed_kmh=-10.0),
        payload(vid, 6, ts=base + timedelta(seconds=50), odometer_km=-5.0),
    ]
    bad_resp = post(client, bad_batch)
    assert bad_resp.status_code == 200
    body = bad_resp.json()
    assert body["accepted"] == 0
    assert body["duplicates"] == 0
    assert body["rejected"] == 5

    # History and vehicle current state must be completely untouched.
    assert reading_count(vid) == 1
    after = vehicle_state(vid)
    assert after["latest_seq"] == before["latest_seq"] == 1
    assert after["latest_device_ts"] == before["latest_device_ts"]
    assert after["soc_pct"] == before["soc_pct"] == Decimal("72.50")
    assert after["speed_kmh"] == before["speed_kmh"] == Decimal("30.00")
    assert after["last_seen_at"] == before["last_seen_at"]
    assert after["received_count"] == before["received_count"] == 1
    assert after["max_seq"] == before["max_seq"] == 1


def test_db_constraint_violation_handled_per_reading_without_breaking_batch(
    ingest, payload, reading_count, monkeypatch
):
    """Defense-in-depth: even if Pydantic were bypassed and PostgreSQL raised
    a CHECK violation on one reading, only that reading is rejected and the
    rest of the batch still succeeds cleanly."""
    from types import SimpleNamespace

    import app.ingestion as ingestion_module

    client, prefix = ingest
    v_bad = prefix + "DBBAD"
    v_ok = prefix + "DBOK"
    base = datetime(2026, 11, 20, 13, 0, 0, tzinfo=timezone.utc)

    real_validate = ingestion_module.TelemetryReadingIn.model_validate

    def bypass_once(raw):
        if raw.get("vehicle_id") == v_bad:
            return SimpleNamespace(
                vehicle_id=v_bad,
                seq=raw["seq"],
                ts=base,
                soc_pct=Decimal("101"),  # violates ck_telemetry_readings_soc_pct_range
                battery_temp_c=Decimal("30"),
                speed_kmh=Decimal("10"),
                lat=Decimal("13.0"),
                lng=Decimal("80.0"),
                odometer_km=Decimal("100"),
                charging=False,
                fault_codes=[],
            )
        return real_validate(raw)

    monkeypatch.setattr(ingestion_module.TelemetryReadingIn, "model_validate", bypass_once)

    resp = post(
        client,
        [
            payload(v_bad, 1, ts=base, soc_pct=101),
            payload(v_ok, 1, ts=base, soc_pct=55),
        ],
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["accepted"] == 1
    assert body["rejected"] == 1
    assert body["results"][0]["status"] == "rejected"
    assert "ck_telemetry_readings_soc_pct_range" in body["results"][0]["errors"][0]["message"]
    assert body["results"][1]["status"] == "accepted"
    assert reading_count(v_bad) == 0
    assert reading_count(v_ok) == 1
