"""Observability (A6) and history route parity (A4) tests:
- X-Request-ID generation and propagation
- Structured JSON HTTP access logging with request_id
- Basic /metrics endpoint (JSON + Prometheus text format)
- GET /v1/vehicles/{id}/history route matching /v1/vehicles/{id}/telemetry
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from app.observability import JsonLogFormatter, http_logger, metrics


def test_request_id_generated_and_propagated(client):
    r1 = client.get("/health")
    assert r1.status_code == 200
    rid = r1.headers.get("X-Request-ID")
    assert rid and len(rid) >= 16

    r2 = client.get("/health", headers={"X-Request-ID": "trace-abc-123"})
    assert r2.status_code == 200
    assert r2.headers.get("X-Request-ID") == "trace-abc-123"

    # Unsafe/invalid client header is replaced with a server-generated id.
    r3 = client.get("/health", headers={"X-Request-ID": "bad id with spaces\n"})
    assert r3.status_code == 200
    assert r3.headers.get("X-Request-ID") != "bad id with spaces\n"


def test_structured_json_log_emitted_with_request_id(client):
    records: list[str] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(self.format(record))

    handler = CaptureHandler()
    handler.setFormatter(JsonLogFormatter())
    http_logger.addHandler(handler)
    try:
        r = client.get("/health", headers={"X-Request-ID": "req-json-001"})
        assert r.status_code == 200
    finally:
        http_logger.removeHandler(handler)

    parsed = [json.loads(line) for line in records]
    match = [p for p in parsed if p.get("request_id") == "req-json-001"]
    assert len(match) == 1
    entry = match[0]
    assert entry["method"] == "GET"
    assert entry["path"] == "/health"
    assert entry["status_code"] == 200
    assert isinstance(entry["duration_ms"], (int, float))
    assert "ts" in entry and entry["level"] == "INFO"


def test_metrics_endpoint_json_and_prometheus(ingest, payload):
    client, prefix = ingest
    metrics.reset()

    vid = prefix + "MET"
    base = datetime(2026, 11, 20, 14, 0, 0, tzinfo=timezone.utc)
    batch = [
        payload(vid, 1, ts=base, battery_temp_c=60.0),
        payload(vid, 2, ts=base + timedelta(seconds=5), battery_temp_c=61.0),
        payload(vid, 3, ts=base + timedelta(seconds=10), battery_temp_c=62.0),
        payload(vid, 3, ts=base + timedelta(seconds=10), battery_temp_c=62.0),  # dup
        payload(vid, 4, ts=base + timedelta(seconds=15), soc_pct=101),         # invalid
    ]
    r = client.post("/v1/telemetry/batch", json={"readings": batch})
    assert r.status_code == 200

    m = client.get("/metrics")
    assert m.status_code == 200
    body = m.json()
    assert body["http_requests_total"] >= 1
    assert body["ingestion_readings_total"]["accepted"] == 3
    assert body["ingestion_readings_total"]["duplicate"] == 1
    assert body["ingestion_readings_total"]["rejected"] == 1
    assert body["alerts_total"]["opened"] >= 1
    assert body["http_request_duration_ms"]["samples"] >= 1

    prom = client.get("/metrics", params={"format": "prometheus"})
    assert prom.status_code == 200
    assert "text/plain" in prom.headers["content-type"]
    assert 'voltgrid_ingestion_readings_total{status="accepted"} 3' in prom.text
    assert 'voltgrid_ingestion_readings_total{status="rejected"} 1' in prom.text


def test_history_route_matches_telemetry_route(ingest, payload):
    client, prefix = ingest
    vid = prefix + "HIST"
    base = datetime(2026, 11, 20, 15, 0, 0, tzinfo=timezone.utc)
    readings = [
        payload(vid, s, ts=base + timedelta(seconds=s * 5), soc_pct=80.0 - s * 0.5)
        for s in range(1, 21)
    ]
    assert client.post("/v1/telemetry/batch", json={"readings": readings}).status_code == 200

    params = {
        "from": base.isoformat(),
        "to": (base + timedelta(minutes=5)).isoformat(),
        "points": 8,
    }
    r_hist = client.get(f"/v1/vehicles/{vid}/history", params=params)
    r_tel = client.get(f"/v1/vehicles/{vid}/telemetry", params=params)
    assert r_hist.status_code == 200
    assert r_tel.status_code == 200
    assert r_hist.json() == r_tel.json()
    assert r_hist.json()["downsampled"] is True
    assert r_hist.json()["returned_count"] <= 8
