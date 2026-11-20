import pytest


def post(client, readings):
    return client.post("/v1/telemetry/batch", json={"readings": readings})


def test_valid_reading_accepted(ingest, payload, reading_count):
    client, prefix = ingest
    vid = prefix + "V1"
    response = post(client, [payload(vid, 1)])

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 1 and body["rejected"] == 0
    assert body["results"][0]["status"] == "accepted"
    assert reading_count(vid) == 1


def test_missing_required_field_rejected_individually(ingest, payload):
    client, prefix = ingest
    bad = payload(prefix + "V1", 1)
    del bad["soc_pct"]
    response = post(client, [bad])

    assert response.status_code == 200
    result = response.json()["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"] == "soc_pct" for e in result["errors"])


@pytest.mark.parametrize(
    "field,value",
    [
        ("soc_pct", 140),
        ("soc_pct", -1),
        ("lat", 95),
        ("lat", -95),
        ("lng", 181),
        ("lng", -181),
        ("speed_kmh", -5),
        ("odometer_km", -1),
        ("seq", 0),
        ("seq", -3),
        ("battery_temp_c", 200),
        ("fault_codes", "P0A80"),
        ("fault_codes", [123]),
    ],
)
def test_invalid_values_rejected(ingest, payload, field, value):
    client, prefix = ingest
    reading = payload(prefix + "V1", 1)
    reading[field] = value
    response = post(client, [reading])

    assert response.status_code == 200
    result = response.json()["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"].split(".")[0] == field for e in result["errors"])


def test_cold_weather_temperature_accepted(ingest, payload):
    client, prefix = ingest
    response = post(client, [payload(prefix + "V1", 1, battery_temp_c=-25.0)])
    assert response.json()["results"][0]["status"] == "accepted"


def test_naive_timestamp_rejected(ingest, payload):
    client, prefix = ingest
    response = post(
        client, [payload(prefix + "V1", 1, ts="2026-11-20T10:00:00")]
    )
    result = response.json()["results"][0]
    assert result["status"] == "rejected"
    assert any(e["field"] == "ts" for e in result["errors"])


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


def test_mixed_batch_valid_readings_succeed(ingest, payload, reading_count):
    client, prefix = ingest
    v1, v2 = prefix + "V1", prefix + "V2"
    readings = [payload(v1, 1), payload(v2, 1, soc_pct=140), payload(v2, 2)]
    response = post(client, readings)

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 2 and body["rejected"] == 1
    assert [r["status"] for r in body["results"]] == [
        "accepted", "rejected", "accepted",
    ]
    assert reading_count(v1) == 1
    assert reading_count(v2) == 1