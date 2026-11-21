# VoltGrid Fleet Telemetry Service

Telemetry ingestion, alerting, query, and live-streaming service for a fleet of electric two-wheelers. Built with **FastAPI**, **PostgreSQL 16**, **SQLAlchemy 2.x**, **Alembic**, and **Docker Compose**.

See [DESIGN.md](DESIGN.md) for architecture decisions, scaling analysis, and engineering trade-offs.

---

## Quick Start (Fresh Machine)

### Prerequisites
- **Docker Desktop** (with `docker compose`)
- **Python 3.10+** *(only needed for running `pytest` or host-side load/benchmark scripts)*

### 1. Clone and start everything

```bash
git clone https://github.com/Akshat-indev/Fleet-Telemetry.git
cd Fleet-Telemetry
docker compose up -d --build
```

No `.env` file is required—defaults (`fleet` / `fleet`) work out of the box, and the `api` container automatically waits for PostgreSQL and runs `alembic upgrade head`.

- **API Server:** `http://localhost:8000` (`/health`, `/ready`, `/metrics`, OpenAPI at `/docs` & `/openapi.json`)
- **Operations Console UI:** `http://localhost:3000` *(live dashboard proxying `/v1/*`)*
- **PostgreSQL 16:** `localhost:5432` (`fleet` / `fleet`, persisted in the `pgdata` volume)

Verify the stack is healthy:

```bash
curl -s http://localhost:8000/health   # {"status":"ok"}
curl -s http://localhost:8000/ready    # {"status":"ready"}
```

### 2. Provision credentials & run the fleet simulator

Create a device group (prints `api_key` and `secret` **once**):

```bash
docker compose exec api python -m app.manage create-group --name sim
```

Drive messy simulated traffic (default **60% valid / 40% invalid**, duplicates, out-of-order, delays, clock drift, and alert profiles) through the real signed API:

**macOS / Linux / Git Bash:**
```bash
SIM_API_KEY=<api_key> SIM_API_SECRET=<secret> \
  docker compose --profile simulation run --rm simulator --vehicles 50 --duration 15 --interval 1
```

**CMD (Windows):**
```cmd
set SIM_API_KEY=api_key
set SIM_API_SECRET=api_secret
docker compose --profile simulation run --rm simulator --vehicles 50 --duration 15 --interval 1
```

**PowerShell (Windows):**
```powershell
$env:SIM_API_KEY = "<api_key>"
$env:SIM_API_SECRET = "<secret>"
docker compose --profile simulation run --rm simulator --vehicles 50 --duration 15 --interval 1
```

*(Omit `--vehicles 50 --duration 15 --interval 1` to run the full 2,000-vehicle / 60s default.)*

### 3. Query the live API & SSE stream

```bash
curl -s http://localhost:8000/v1/fleet/summary
curl -s "http://localhost:8000/v1/vehicles?limit=10"
curl -s "http://localhost:8000/v1/alerts?status=open"
curl -s http://localhost:8000/metrics
curl -N http://localhost:8000/v1/stream
```

### 4. Run the full test suite (400 tests)

```bash
python -m venv .venv
# Activate:
#   macOS/Linux: source .venv/bin/activate
#   PowerShell:  .venv\Scripts\Activate.ps1
#   Git Bash:    source .venv/Scripts/activate
pip install -r requirements-dev.txt
```

Start PostgreSQL if not already running (`docker compose up -d --wait db`), then run `pytest` *(auto-migrates to Alembic head once per session)*:

```bash
# macOS / Linux / Git Bash
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet pytest

# PowerShell
$env:DATABASE_URL = "postgresql+psycopg://fleet:fleet@localhost:5432/fleet"
pytest
```

*(If you customized `POSTGRES_PASSWORD` in `.env`, use that password in `DATABASE_URL`. Copy `.env.example` to `.env` to override defaults.)*

### 5. Run the load test & `EXPLAIN ANALYZE`

```bash
# Mixed-workload load test (concurrent signed ingestion + all query endpoints)
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet \
  SIM_API_KEY=<api_key> SIM_API_SECRET=<secret> \
  python loadtest/run_load_test.py --duration 15

# Or with k6:
SIM_API_KEY=<api_key> SIM_API_SECRET=<secret> k6 run loadtest/k6_load_test.js

# Verbatim EXPLAIN (ANALYZE, BUFFERS) for summary + 24h history queries:
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet \
  python scripts/benchmark_summary.py --explain
```

### 6. Shut down

```bash
docker compose down        # stop containers, keep DB volume
docker compose down -v     # stop containers AND wipe DB volume for a fresh reset
```

---

## Architecture Overview

![VoltGrid Fleet Telemetry Architecture](docs/architecture.jpg)

---

## API Reference

Interactive OpenAPI docs are served live at `http://localhost:8000/docs` (`/openapi.json`).

| Method & Path | Auth | Description |
|---|---|---|
| `POST /v1/telemetry/batch` | HMAC | Ingest 1–500 readings with per-reading validation (`200` partial success, `401`, `422`, `429` + `Retry-After`, `503`). |
| `GET /v1/vehicles` | Public | Keyset-paginated (`cursor`, no OFFSET) vehicle list with `status` (`active`/`idle`/`charging`/`offline`) and bbox (`min_lat`, `min_lng`, `max_lat`, `max_lng`) filters. |
| `GET /v1/vehicles/{id}/history` | Public | Chronological history (`from`, `to` required, ≤31d). With `points=4..5000`, runs SQL min/max downsampling preserving peaks/troughs. *(Alias: `/v1/vehicles/{id}/telemetry`)* |
| `GET /v1/fleet/summary` | Public | Fleet counts by status, `avg_soc_pct`, `open_alerts_by_rule`, `readings_total`, `data_completeness_pct` in O(vehicles). |
| `GET /v1/alerts` | Public | List alerts filtered by `vehicle_id`, `status`, `rule_type`, `limit`. |
| `POST /v1/alerts/{id}/ack` | Public | Acknowledge an open alert with `{"version": N}` (`200`, `404`, `409` on stale version or invalid transition). |
| `GET /v1/stream` | Public | Resumable SSE stream (`Last-Event-ID` header, `0` = replay retained, explicit `stream_reset`/`stream_gap` control events, bounded coalescer; `?coalesce=false` for raw telemetry). |
| `GET /health` / `GET /ready` | Public | Liveness probe and real PostgreSQL readiness probe (`503` if DB unreachable). |
| `GET /metrics` | Public | Operational metrics in JSON (`?format=prometheus` for Prometheus text exposition). |

### Request Signing (`POST /v1/telemetry/batch`)

Headers: `X-API-Key`, `X-Timestamp` (Unix epoch seconds, ±300s window), `X-Signature`:

```text
X-Signature = hex(HMAC-SHA256(secret, api_key + "\n" + timestamp + "\n" + raw_body_bytes))
```

- Verified against exact raw bytes **before** JSON parsing using `hmac.compare_digest`.
- Every auth failure (missing/bad header, unknown key, stale timestamp, bad signature, replay) returns identical `401 {"detail": "authentication failed"}`.
- Rate limiting (`RATE_LIMIT_CAPACITY=120`, `RATE_LIMIT_REFILL_PER_SECOND=20.0`) runs in PostgreSQL before nonce insertion so `429` never burns a nonce; retries must re-sign with a fresh timestamp.

### Strict Validation & Partial Batch Success

Every reading in `POST /v1/telemetry/batch` is validated independently (`app/schemas.py` + PostgreSQL `CHECK` constraints):
- Strict types: rejects numeric strings, booleans for numbers/integers, `NaN`/`Infinity`, non-boolean `charging`, blank `vehicle_id`, and non-ISO/naive timestamps.
- Ranges: `0 <= soc_pct <= 100`, `-50 <= battery_temp_c <= 150` (accepts overheating >55 °C so alerts fire), `-90 <= lat <= 90`, `-180 <= lng <= 180`, `speed_kmh >= 0`, `odometer_km >= 0`, `seq >= 1`.
- One invalid reading never rejects valid readings in the same batch and never enters `telemetry_readings` or updates `vehicles`.

### Alert Rules (`app/alerts/`)

| Rule | Trigger | Resolves When |
|---|---|---|
| `overheat` | 3 consecutive readings `> 55 °C` | 3 consecutive readings `≤ 50 °C` |
| `rapid_discharge` | SOC drops `> 8` pts in 5 min while not charging | Drop returns `≤ 8` pts (window slides / charging starts) |
| `geofence_breach` | Frontier position outside `GEOFENCE_POLYGON` | Frontier position back inside (edges count as inside) |
| `offline` | No valid reading for `> 90 s` (server `last_seen_at`) | Any new valid reading arrives (even a duplicate) |

Deduplicated via partial unique index `uq_alerts_active_vehicle_rule (vehicle_id, rule_type) WHERE status <> 'resolved'`. Lifecycle: `open → acknowledged → resolved`.

---

## Fleet Simulator

`python -m simulator` drives seeded virtual vehicles through the real signed HTTP API (never writes to PostgreSQL directly):

| Flag | Default | Description |
|---|---|---|
| `--vehicles` | `2000` | Number of simulated vehicles |
| `--duration` / `--interval` | `60` / `5` | Generation duration (s) and tick interval (s) |
| `--batch-size` / `--concurrency` | `250` / `8` | Readings per POST (≤500) and concurrent workers |
| `--valid-pct` / `--invalid-pct` | `60` / `40` | Configurable valid vs invalid distribution (20 invalid scenarios) |
| `--duplicate-prob` / `--out-of-order-prob` | `0.05` / `0.10` | Duplicate and out-of-order injection rates |
| `--delayed-prob` / `--max-delay` | `0.05` / `60` | Late-arrival hold-back rate and max delay (s) |
| `--clock-drift` / `--seed` | `30` / `42` | Per-vehicle ±clock offset (s) and deterministic RNG seed |

Valid traffic includes abnormal operating profiles (`overheat`, `discharge`, `breacher`, `charger`, `low_battery`, `offline_prone`). The terminal report audits every server verdict against expected validity and exits with code `1` if any invalid reading is accepted.

---

## Performance: Load Test Results & `EXPLAIN (ANALYZE, BUFFERS)`

### 1. Mixed-Workload Load Test (`loadtest/run_load_test.py`)

Measured over `15.13 s` with 4 concurrent ingest workers (25-reading signed batches, 60% valid / 40% invalid) + 6 concurrent query workers against the Dockerized API (PostgreSQL 16.16, Windows Docker Desktop):

| Endpoint | Requests | HTTP 200 | 429/Err | RPS | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `GET /v1/alerts` | 364 | 364 | 0 | 24.1 | 50.4 | 46.9 | 86.9 | 116.3 |
| `GET /v1/fleet/summary` | 366 | 366 | 0 | 24.2 | 53.1 | 50.8 | 74.4 | 93.2 |
| `GET /v1/vehicles` | 367 | 367 | 0 | 24.3 | 45.7 | 44.3 | 62.8 | 95.9 |
| `GET /v1/vehicles/{id}/history` (24h/17,280→200) | 367 | 367 | 0 | 24.3 | 96.0 | 91.9 | 128.0 | 152.0 |
| `POST /v1/telemetry/batch` (25 readings, 60/40) | 33 | 33 | 0 | 2.2 | 1782.9 | 1853.6 | 2121.4 | 2241.9 |

Ingestion verdicts: `accepted=528`, `duplicates=0`, `rejected=297`, `unexpected_invalid_accepted=0` (0% error rate).

### 2. Fleet Summary at 5,000,027 Readings (`scripts/benchmark_summary.py`)

200 HTTP samples after 10 warm-up requests against 2,000 vehicles × 2,500 readings: **p50 = 8.3 ms**, **p95 = 11.6 ms**, **p99 = 15.9 ms**. Naive `GROUP BY` over raw readings took **492 ms** (parallel seq scan); maintaining transactional `min_seq`/`max_seq`/`received_count` on `vehicles` keeps summary O(vehicles).

### 3. Verbatim `EXPLAIN (ANALYZE, BUFFERS)` — Two Most Important Queries

**Query 1 — `GET /v1/fleet/summary` (vehicles aggregate + open alerts by rule):**

```text
Aggregate  (cost=255.75..255.76 rows=1 width=168) (actual time=1.202..1.203 rows=1 loops=1)
  Buffers: shared hit=65
  ->  Seq Scan on vehicles  (cost=0.00..85.62 rows=2062 width=45) (actual time=0.009..0.402 rows=2063 loops=1)
        Buffers: shared hit=65
Planning Time: 0.346 ms
Execution Time: 1.498 ms

HashAggregate  (cost=86.97..87.01 rows=4 width=16) (actual time=0.376..0.377 rows=4 loops=1)
  Group Key: rule_type
  Batches: 1  Memory Usage: 24kB
  Buffers: shared hit=46
  ->  Seq Scan on alerts  (cost=0.00..75.35 rows=2324 width=8) (actual time=0.011..0.201 rows=2089 loops=1)
        Filter: ((status)::text = 'open'::text)
        Rows Removed by Filter: 57
        Buffers: shared hit=46
Planning Time: 0.602 ms
Execution Time: 0.414 ms
```

**Query 2 — `GET /v1/vehicles/{id}/history?from=&to=&points=200` (24h window, 17,280 raw readings → 199 points):**

```text
Sort  (cost=95.07..95.13 rows=26 width=131) (actual time=30.571..30.584 rows=199 loops=1)
  Sort Key: "*SELECT* 1".ts, "*SELECT* 1".seq
  Sort Method: quicksort  Memory: 42kB
  Buffers: shared hit=330
  CTE bucketed
    ->  Index Scan using ix_telemetry_readings_vehicle_ts on telemetry_readings  (cost=0.41..91.78 rows=12 width=71) (actual time=0.026..5.442 rows=17280 loops=1)
          Index Cond: ((vehicle_id = 155090) AND (ts >= '2026-11-20 04:48:05+00') AND (ts <= '2026-11-21 04:48:05+00'))
          Buffers: shared hit=321
  ->  HashAggregate  (cost=2.42..2.68 rows=26 width=131) (actual time=30.456..30.498 rows=199 loops=1)
        Batches: 1  Memory Usage: 72kB
        ->  Append  (actual time=15.815..30.297 rows=202 loops=1)
              ->  Subquery Scan (bucket min soc_pct, 100 rows, quicksort 2388kB)
              ->  Subquery Scan (bucket max soc_pct, 100 rows, quicksort 2388kB)
              ->  Limit 1 (first point, top-N heapsort 25kB)
              ->  Limit 1 (last point, top-N heapsort 25kB)
Planning Time: 0.518 ms
Execution Time: 31.655 ms
```

---

## Schema, Migrations & Observability

- **Tables:** `vehicles`, `telemetry_readings`, `alerts`, `fleet_events`, `device_groups`, `request_nonces`, `rate_limit_buckets`.
- **Key constraints & indexes:** `uq_telemetry_readings_vehicle_seq` (idempotency), `ix_telemetry_readings_vehicle_ts` (24h history), `uq_alerts_active_vehicle_rule` (partial unique active-alert dedup), `ix_alerts_rule_type_status` & `ix_alerts_open` (summary/dashboard), `ON DELETE RESTRICT` FKs, and full `CHECK` constraints on both `telemetry_readings` and `vehicles`.
- **Migrations:** `alembic upgrade head` (runs automatically on container start) / `alembic downgrade base` (verified in `tests/test_migrations.py`).
- **Structured logs:** every HTTP request logs single-line JSON with `request_id` (`X-Request-ID` header), `method`, `path`, `route`, `status_code`, and `duration_ms`.
