"""Reproducible benchmark for GET /v1/fleet/summary at ~5M readings.

Explicit, separate operation — never runs at startup or in tests.

  # 1. generate ~5,000,000 readings (2,000 vehicles x 2,500)
  python scripts/benchmark_summary.py --generate

  # 2. measure the live endpoint (API must be running, e.g. docker compose up)
  python scripts/benchmark_summary.py --bench --samples 200

  # 3. EXPLAIN ANALYZE the summary queries + the naive alternative
  python scripts/benchmark_summary.py --explain

  # 4. remove benchmark rows (only BENCH-% vehicles and their data)
  python scripts/benchmark_summary.py --cleanup

DATABASE_URL is read from the environment or .env (same as the app).
"""

import argparse
import platform
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text

from app.config import get_settings
from app.queries import _SUMMARY_ALERTS, _SUMMARY_VEHICLES

PREFIX = "BENCH-"
VEHICLES = 2000
READINGS_PER_VEHICLE = 2500
CHUNK = 100  # vehicles per insert statement


def _engine():
    return create_engine(get_settings().database_url)


def generate():
    engine = _engine()
    start = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO vehicles
                (vehicle_id, latest_seq, latest_device_ts, soc_pct, battery_temp_c,
                 speed_kmh, lat, lng, odometer_km, charging, last_seen_at,
                 min_seq, max_seq, received_count)
            SELECT '{PREFIX}' || lpad(g::text, 4, '0'),
                   {READINGS_PER_VEHICLE},
                   now() - interval '3.5 hours' + make_interval(secs => {READINGS_PER_VEHICLE} * 5),
                   20 + (g % 70), 30 + (g % 15), (g % 60),
                   12.8 + (g % 50) / 100.0, 79.9 + (g % 60) / 100.0,
                   1000 + g, (g % 10 = 0), now(),
                   1, {READINGS_PER_VEHICLE}, {READINGS_PER_VEHICLE}
            FROM generate_series(1, {VEHICLES}) g
            ON CONFLICT (vehicle_id) DO NOTHING
        """))
    print(f"vehicles ready in {time.perf_counter() - start:.1f}s")

    gen_start = time.perf_counter()
    total = 0
    for lo in range(1, VEHICLES + 1, CHUNK):
        hi = min(lo + CHUNK - 1, VEHICLES)
        t = time.perf_counter()
        with engine.begin() as conn:
            result = conn.execute(text(f"""
                INSERT INTO telemetry_readings
                    (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
                     lat, lng, odometer_km, charging, fault_codes)
                SELECT v.id, s,
                       now() - interval '3.5 hours' + make_interval(secs => s * 5),
                       20 + ((s * 7) % 60) + random() * 5,
                       25 + random() * 20,
                       random() * 60,
                       12.8 + random() * 0.5, 79.9 + random() * 0.6,
                       1000 + s * 0.01,
                       (s % 500 < 50),
                       '[]'::jsonb
                FROM vehicles v
                CROSS JOIN generate_series(1, {READINGS_PER_VEHICLE}) s
                WHERE v.vehicle_id BETWEEN '{PREFIX}' || lpad(CAST(:lo AS text), 4, '0')
                                       AND '{PREFIX}' || lpad(CAST(:hi AS text), 4, '0')
                ON CONFLICT ON CONSTRAINT uq_telemetry_readings_vehicle_seq DO NOTHING
            """), {"lo": lo, "hi": hi})
            total += result.rowcount
        print(f"  vehicles {lo}-{hi}: +{result.rowcount} rows "
              f"({time.perf_counter() - t:.1f}s)")
    print(f"generated {total} readings in {time.perf_counter() - gen_start:.1f}s "
          f"(total incl. setup {time.perf_counter() - start:.1f}s)")
    engine.dispose()


def bench(url: str, samples: int):
    import httpx2

    with httpx2.Client(base_url=url, timeout=30) as client:
        for _ in range(10):
            r = client.get("/v1/fleet/summary")
            r.raise_for_status()
        body = r.json()
        print(f"fleet under test: {body['vehicles_total']} vehicles, "
              f"{body['readings_total']} readings")

        latencies = []
        for _ in range(samples):
            t = time.perf_counter()
            r = client.get("/v1/fleet/summary")
            r.raise_for_status()
            latencies.append((time.perf_counter() - t) * 1000)

    latencies.sort()

    def pct(p):
        return latencies[min(len(latencies) - 1, int(len(latencies) * p / 100))]

    print(f"samples: {samples} (after 10 warm-up requests)")
    print(f"p50: {pct(50):.1f} ms")
    print(f"p95: {pct(95):.1f} ms")
    print(f"p99: {pct(99):.1f} ms")
    print(f"min: {latencies[0]:.1f} ms  max: {latencies[-1]:.1f} ms  "
          f"mean: {statistics.mean(latencies):.1f} ms")


def explain():
    engine = _engine()
    with engine.connect() as conn:
        print("PostgreSQL:", conn.execute(text("SELECT version()")).scalar_one())
        print("shared_buffers:", conn.execute(text("SHOW shared_buffers")).scalar_one())
        print("work_mem:", conn.execute(text("SHOW work_mem")).scalar_one())
        print(f"machine: {platform.platform()} | {platform.processor()} "
              f"| {__import__('os').cpu_count()} logical CPUs")
        readings = conn.execute(text("SELECT count(*) FROM telemetry_readings")).scalar_one()
        vehicles = conn.execute(text("SELECT count(*) FROM vehicles")).scalar_one()
        print(f"rows: {readings} readings, {vehicles} vehicles\n")

        params = {"offline_after": get_settings().offline_after_seconds}
        for name, stmt, p in [
            ("summary: vehicles aggregate (used)", _SUMMARY_VEHICLES, params),
            ("summary: open alerts by rule (used)", _SUMMARY_ALERTS, {}),
            ("naive alternative: completeness from raw readings (NOT used)",
             text("SELECT vehicle_id, count(*), min(seq), max(seq) "
                  "FROM telemetry_readings GROUP BY vehicle_id"), {}),
        ]:
            print(f"===== {name} =====")
            rows = conn.execute(
                text("EXPLAIN (ANALYZE, BUFFERS) " + stmt.text), p
            ).all()
            for row in rows:
                print(row[0])
            print()
    engine.dispose()


def cleanup():
    engine = _engine()
    start = time.perf_counter()
    with engine.begin() as conn:
        ids = conn.execute(text(
            "SELECT id FROM vehicles WHERE vehicle_id LIKE :p"
        ), {"p": PREFIX + "%"}).scalars().all()
        if not ids:
            print("no benchmark vehicles found")
            return
        for table in ("telemetry_readings", "alerts"):
            r = conn.execute(text(
                f"DELETE FROM {table} WHERE vehicle_id = ANY(:ids)"
            ), {"ids": ids})
            print(f"deleted {r.rowcount} {table}")
        r = conn.execute(text("DELETE FROM vehicles WHERE id = ANY(:ids)"), {"ids": ids})
        print(f"deleted {r.rowcount} vehicles")
    print(f"cleanup done in {time.perf_counter() - start:.1f}s")
    engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--explain", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--samples", type=int, default=200)
    args = parser.parse_args()
    if args.generate:
        generate()
    if args.bench:
        bench(args.url, args.samples)
    if args.explain:
        explain()
    if args.cleanup:
        cleanup()
    if not any((args.generate, args.bench, args.explain, args.cleanup)):
        parser.print_help()