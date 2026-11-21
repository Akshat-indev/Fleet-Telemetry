"""Reproducible mixed-workload HTTP load test for VoltGrid Fleet Telemetry API.

Runs concurrent HMAC-signed batch ingestion (60% valid / 40% invalid + dupes)
simultaneously with operator read endpoints (summary, vehicles keyset listing,
24-hour downsampled history over ~17,280 raw readings, and alerts), then prints
a measured latency/throughput table and cleans up its temporary LOAD-% data.

Usage:
  SIM_API_KEY=vg-... SIM_API_SECRET=... python loadtest/run_load_test.py --duration 20
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx2 as httpx
from sqlalchemy import create_engine, text

from app.config import get_settings
from app.security import compute_signature

PREFIX = "LOADTEST-"
HISTORY_VID = f"{PREFIX}HIST24H"
HISTORY_POINTS = 17_280  # 24 hours at 5-second cadence


def pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(p / 100.0 * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def seed_24h_history(engine) -> tuple[datetime, datetime]:
    """Seed one vehicle with 17,280 readings spanning 24 h (5 s cadence)
    for realistic GET /v1/vehicles/{id}/history?points=200 benchmarking."""
    to_ts = datetime.now(timezone.utc)
    from_ts = to_ts - timedelta(hours=24)
    with engine.begin() as conn:
        db_id = conn.execute(
            text("""
                INSERT INTO vehicles
                    (vehicle_id, latest_seq, latest_device_ts, soc_pct,
                     battery_temp_c, speed_kmh, lat, lng, odometer_km,
                     charging, last_seen_at, min_seq, max_seq, received_count)
                VALUES
                    (:vid, :n, :to_ts, 62.5, 34.0, 28.0, 13.05, 80.25, 2500.0,
                     false, now(), 1, :n, :n)
                ON CONFLICT (vehicle_id) DO UPDATE SET vehicle_id = EXCLUDED.vehicle_id
                RETURNING id
            """),
            {"vid": HISTORY_VID, "n": HISTORY_POINTS, "to_ts": to_ts},
        ).scalar_one()
        conn.execute(
            text("""
                INSERT INTO telemetry_readings
                    (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
                     lat, lng, odometer_km, charging, fault_codes)
                SELECT :db_id, s,
                       :from_ts + make_interval(secs => s * 5),
                       GREATEST(5, LEAST(98, 50 + 35 * sin(s / 600.0) + CASE WHEN s % 2000 = 0 THEN 12 ELSE 0 END)),
                       30 + (s % 15),
                       25 + (s % 30),
                       13.05, 80.25,
                       1000 + s * 0.05,
                       false, '[]'::jsonb
                FROM generate_series(1, :n) s
                ON CONFLICT ON CONSTRAINT uq_telemetry_readings_vehicle_seq DO NOTHING
            """),
            {"db_id": db_id, "from_ts": from_ts, "n": HISTORY_POINTS},
        )
    return from_ts, to_ts


def cleanup_load_data(engine) -> None:
    with engine.begin() as conn:
        ids = conn.execute(
            text("SELECT id FROM vehicles WHERE vehicle_id LIKE :p"),
            {"p": PREFIX + "%"},
        ).scalars().all()
        if ids:
            for tbl in ("telemetry_readings", "alerts"):
                conn.execute(text(f"DELETE FROM {tbl} WHERE vehicle_id = ANY(:ids)"), {"ids": ids})
            conn.execute(text("DELETE FROM vehicles WHERE id = ANY(:ids)"), {"ids": ids})
        conn.execute(text("DELETE FROM fleet_events WHERE vehicle_id LIKE :p"), {"p": PREFIX + "%"})


def sign_headers(api_key: str, secret: str, body: bytes) -> dict[str, str]:
    ts = str(int(time.time()))
    sig = compute_signature(secret, api_key, ts, body)
    return {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
        "X-Timestamp": ts,
        "X-Signature": sig,
    }


async def run_load(
    base_url: str,
    api_key: str,
    secret: str,
    duration_s: float,
    ingest_workers: int,
    query_workers: int,
    batch_size: int,
    from_ts: datetime,
    to_ts: datetime,
) -> dict:
    latencies: dict[str, list[float]] = defaultdict(list)
    statuses: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    ingest_totals = {"accepted": 0, "duplicates": 0, "rejected": 0, "unexpected_invalid_accepted": 0}
    deadline = time.monotonic() + duration_s

    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        async def ingest_worker(wid: int) -> None:
            rng = random.Random(1000 + wid)
            seq_base = wid * 100_000 + 1
            while time.monotonic() < deadline:
                now = datetime.now(timezone.utc)
                readings = []
                expected_invalid = 0
                for i in range(batch_size):
                    vid = f"{PREFIX}W{wid}-{i % 10:02d}"
                    is_invalid = rng.random() < 0.40
                    if is_invalid:
                        expected_invalid += 1
                    readings.append({
                        "vehicle_id": vid,
                        "seq": seq_base + i,
                        "ts": (now - timedelta(seconds=(batch_size - i) * 5)).isoformat(),
                        "soc_pct": 101 if is_invalid else round(rng.uniform(25, 90), 2),
                        "battery_temp_c": round(rng.uniform(28, 42), 2),
                        "speed_kmh": round(rng.uniform(0, 55), 2),
                        "lat": 13.05,
                        "lng": 80.25,
                        "odometer_km": round(1200 + seq_base * 0.1 + i * 0.2, 2),
                        "charging": False,
                        "fault_codes": [],
                    })
                seq_base += batch_size
                body = json.dumps({"readings": readings}).encode()
                t0 = time.perf_counter()
                resp = await client.post(
                    "/v1/telemetry/batch",
                    content=body,
                    headers=sign_headers(api_key, secret, body),
                )
                ms = (time.perf_counter() - t0) * 1000.0
                latencies["POST /v1/telemetry/batch"].append(ms)
                statuses["POST /v1/telemetry/batch"][resp.status_code] += 1
                if resp.status_code == 200:
                    data = resp.json()
                    ingest_totals["accepted"] += data["accepted"]
                    ingest_totals["duplicates"] += data["duplicates"]
                    ingest_totals["rejected"] += data["rejected"]
                    if data["rejected"] < expected_invalid:
                        ingest_totals["unexpected_invalid_accepted"] += (expected_invalid - data["rejected"])
                await asyncio.sleep(0.05)

        async def query_worker(wid: int) -> None:
            hist_params = {
                "from": from_ts.isoformat(),
                "to": to_ts.isoformat(),
                "points": 200,
            }
            endpoints = [
                ("GET /v1/fleet/summary", "/v1/fleet/summary", {}),
                ("GET /v1/vehicles", "/v1/vehicles", {"limit": 50}),
                ("GET /v1/vehicles/{id}/history (24h/17k->200)", f"/v1/vehicles/{HISTORY_VID}/history", hist_params),
                ("GET /v1/alerts", "/v1/alerts", {"status": "open", "limit": 50}),
            ]
            idx = wid
            while time.monotonic() < deadline:
                label, path, params = endpoints[idx % len(endpoints)]
                idx += 1
                t0 = time.perf_counter()
                resp = await client.get(path, params=params)
                ms = (time.perf_counter() - t0) * 1000.0
                latencies[label].append(ms)
                statuses[label][resp.status_code] += 1

        started = time.perf_counter()
        tasks = [
            *[asyncio.create_task(ingest_worker(i)) for i in range(ingest_workers)],
            *[asyncio.create_task(query_worker(i)) for i in range(query_workers)],
        ]
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - started

    return {
        "elapsed": elapsed,
        "latencies": latencies,
        "statuses": statuses,
        "ingest_totals": ingest_totals,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--api-key", default=os.environ.get("SIM_API_KEY"))
    parser.add_argument("--api-secret", default=os.environ.get("SIM_API_SECRET"))
    parser.add_argument("--duration", type=float, default=15.0)
    parser.add_argument("--ingest-workers", type=int, default=4)
    parser.add_argument("--query-workers", type=int, default=6)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()

    if not args.api_key or not args.api_secret:
        parser.error("SIM_API_KEY and SIM_API_SECRET (or --api-key/--api-secret) are required")

    engine = create_engine(get_settings().database_url)
    try:
        from_ts, to_ts = seed_24h_history(engine)
        res = asyncio.run(
            run_load(
                args.base_url.rstrip("/"),
                args.api_key,
                args.api_secret,
                args.duration,
                args.ingest_workers,
                args.query_workers,
                args.batch_size,
                from_ts,
                to_ts,
            )
        )
    finally:
        if not args.keep_data:
            cleanup_load_data(engine)
        engine.dispose()

    elapsed = res["elapsed"]
    print(f"\nMixed-workload load test ({elapsed:.2f}s, {args.ingest_workers} ingest VUs + {args.query_workers} query VUs, batch={args.batch_size}):\n")
    print("| Endpoint | Requests | HTTP 200 | 429/Err | RPS | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for label, vals in sorted(res["latencies"].items()):
        s = sorted(vals)
        st = res["statuses"][label]
        ok = st.get(200, 0)
        err = sum(c for code, c in st.items() if code != 200)
        print(
            f"| `{label}` | {len(s)} | {ok} | {err} | {len(s) / elapsed:.1f} | "
            f"{statistics.mean(s):.1f} | {pct(s, 50):.1f} | {pct(s, 95):.1f} | {pct(s, 99):.1f} |"
        )
    it = res["ingest_totals"]
    print(
        f"\nIngestion verdicts: accepted={it['accepted']}, duplicates={it['duplicates']}, "
        f"rejected={it['rejected']}, unexpected_invalid_accepted={it['unexpected_invalid_accepted']}"
    )
    return 0 if it["unexpected_invalid_accepted"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
