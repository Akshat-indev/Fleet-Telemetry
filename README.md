# VoltGrid Fleet Telemetry

FastAPI + PostgreSQL (SQLAlchemy 2.x, psycopg 3, Alembic), Docker Compose.

- **Phase 1:** app foundation, `/health` and `/ready`, Docker Compose.
- **Phase 2:** schema (`vehicles`, `telemetry_readings`, `alerts`) and Alembic migrations.
- **Phase 3:** `POST /v1/telemetry/batch` — idempotent ingestion with per-reading
  results, device-time current state, and sequence-gap reporting.
- **Phase 4:** alert engine — overheat, rapid discharge, geofence breach,
  offline detection; alert lifecycle with optimistic-concurrency acknowledge.
- **Phase 5:** query API — vehicle listing (keyset pagination, status/bbox
  filters), downsampled telemetry history, fleet summary.
- **Phase 6:** device authentication (A2) — per-group API key + HMAC-SHA256
  request signing, ±5-minute timestamp window, database-backed replay
  protection and distributed token-bucket rate limiting.
- **Phase 8:** fleet simulator (2,000 seeded virtual vehicles with fault
  injection, sent through the real signed API) and resumable live streaming
  (`GET /v1/stream`, Server-Sent Events over a PostgreSQL-backed event log).

## Architecture overview

```text
  Devices / Simulator (HMAC-SHA256 signed batches, <=500 readings)
          │
          ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  FastAPI Service (app/main.py)                                      │
  │  • RequestObservabilityMiddleware (X-Request-ID, JSON logs, /metrics│
  │  • Auth & Abuse (app/security.py): HMAC raw body, ±300s, DB nonces, │
  │    distributed PostgreSQL token bucket (429 + Retry-After)          │
  │  • Ingestion (app/ingestion.py): strict per-reading validation,     │
  │    idempotent INSERT ON CONFLICT DO NOTHING, device-time state CAS, │
  │    seq-gap summary, inline frontier alert evaluation                │
  │  • Query API (app/queries.py): keyset /v1/vehicles, SQL min/max     │
  │    downsampled /v1/vehicles/{id}/history, O(vehicles) /v1/fleet/    │
  │    summary, optimistic-concurrency POST /v1/alerts/{id}/ack         │
  │  • Live Stream (app/stream.py): resumable SSE over transactional    │
  │    outbox (Last-Event-ID, gap grace, bounded per-vehicle coalescer) │
  │  • Watchdog (app/alerts/worker.py): advisory-locked offline sweep,  │
  │    nonce & event-log retention cleanup                              │
  └──────────────────────────────┬──────────────────────────────────────┘
                                 │ SQLAlchemy 2.x + psycopg 3
                                 ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  PostgreSQL 16 (Alembic migrations + CHECK/UNIQUE/partial indexes)  │
  │  vehicles · telemetry_readings · alerts · fleet_events ·            │
  │  device_groups · request_nonces · rate_limit_buckets                │
  └─────────────────────────────────────────────────────────────────────┘
```

## Run with Docker

```bash
docker compose up --build
```

- **Backend API (graded service):** http://localhost:8000 (`GET /health`, `GET /ready`, `GET /metrics`, OpenAPI at `/docs` and `/openapi.json`)
- **Optional visualization console:** http://localhost:3000 (`frontend` container proxying `/v1/*`; the assignment is backend-only and judged through the API, tests, docs, and load test)
- **PostgreSQL:** `db` service, port 5432, data persisted in the `pgdata` named volume. The API runs `alembic upgrade head` automatically after PostgreSQL passes its health check.

Credentials and ports are configurable via `POSTGRES_USER`, `POSTGRES_PASSWORD`,
`POSTGRES_DB`, `POSTGRES_PORT`, `API_PORT` (see `.env.example`). Defaults are for
local development only. Both services restart automatically unless stopped, and
the API has its own container health check.

## Local development and tests

Create a virtualenv and install dev dependencies:

```bash
python -m venv .venv
pip install -r requirements-dev.txt   # after activating, see below
```

Activate the virtualenv:

| Shell | Command |
|---|---|
| Git Bash | `source .venv/Scripts/activate` |
| PowerShell | `.venv\Scripts\Activate.ps1` |
| CMD | `.venv\Scripts\activate.bat` |

(On Linux/macOS use `.venv/bin/activate`.)

Start the database and run the tests (the default local credentials from
`docker-compose.yml` / `.env.example` are `fleet:fleet`; if you customized
`POSTGRES_PASSWORD` in `.env`, use that password in `DATABASE_URL` instead):

**Git Bash / Linux / macOS**

```bash
docker compose up -d --wait db
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet pytest
```

**PowerShell**

```powershell
docker compose up -d --wait db
$env:DATABASE_URL = "postgresql+psycopg://fleet:fleet@localhost:5432/fleet"
pytest
```

**CMD**

```bat
docker compose up -d --wait db
set DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet
pytest
```

The test suite automatically runs `alembic upgrade head` once against
`DATABASE_URL` so it works on a brand-new empty database. The `/ready`
integration test hits a real PostgreSQL — no mocks. It is skipped with a
message when `DATABASE_URL` is not set. The 503 failure case is covered by a
separate unit test that always runs.

## Device authentication (A2)

`POST /v1/telemetry/batch` requires a signed request. Read/ops endpoints
(`GET /v1/vehicles`, history, summary, alerts, ack, `/health`, `/ready`)
are unchanged and unauthenticated.

### Provisioning a device group

Each device group gets an API key and a shared secret, generated with
`secrets.token_hex` and printed **once**:

```bash
docker compose exec api python -m app.manage create-group --name fleet-a
# api_key: vg-...
# secret:  ...        <- store now; it cannot be retrieved later
docker compose exec api python -m app.manage list-groups   # keys + names, never secrets
```

(Outside Docker: `python -m app.manage create-group --name fleet-a` with
`DATABASE_URL` set.) The secret is stored verbatim because HMAC verification
must recompute it; it is never returned by any API, never logged, and never
included in error messages.

### Signing a request

Headers:

| Header | Value |
|---|---|
| `X-API-Key` | the group's API key |
| `X-Timestamp` | Unix epoch **seconds**, canonical integer string (no sign, no leading zeros, no fraction) |
| `X-Signature` | lowercase hex `HMAC-SHA256(secret, canonical_message)` |

Canonical message — UTF-8 bytes, `\n` separators, **the exact raw body bytes
as sent on the wire** (not a reserialized JSON object):

```
api_key + "\n" + timestamp + "\n" + raw_body
```

Python example:

```python
import hashlib, hmac, json, time
import httpx2 as httpx

api_key, secret = "vg-...", "..."
body = json.dumps({"readings": [reading]}).encode()
ts = str(int(time.time()))
sig = hmac.new(secret.encode(), api_key.encode() + b"\n" + ts.encode() + b"\n" + body,
               hashlib.sha256).hexdigest()
r = httpx.post("http://localhost:8000/v1/telemetry/batch", content=body,
               headers={"X-API-Key": api_key, "X-Timestamp": ts, "X-Signature": sig,
                        "Content-Type": "application/json"})
```

Because the signature covers the raw bytes, any change to whitespace, key
order, or values invalidates it — sign exactly the bytes you transmit.

### Verification rules

Checks run in order: header presence/format → key lookup → timestamp window
→ constant-time signature compare (`hmac.compare_digest`) → rate limit →
replay insert. **Every** authentication failure returns the same response,
so nothing reveals whether a key exists or which check failed:

- **401** `{"detail": "authentication failed"}` — missing/malformed headers,
  unknown key, timestamp outside **±300 s** of server time, bad signature,
  or a replayed request.
- **429** `{"detail": "rate limit exceeded"}` + `Retry-After: <seconds>` —
  token bucket empty (below).

### Replay protection

The signature itself is the replay identifier: it covers key + timestamp +
exact body, so an identical signature is an identical request. Each accepted
signature is inserted into `request_nonces` (primary key = signature) with
`ON CONFLICT DO NOTHING` — concurrent copies of one request race on the
primary key and exactly one wins (verified with 20 threads and with two
independent API instances).

- Nonce TTL = **2 × timestamp window** (600 s): a signature stays acceptable
  for ±300 s around its timestamp, so the nonce always outlives the newest
  moment the signature could be accepted. Expired nonces are deleted by the
  background watchdog each cycle.
- Consequence: a byte-identical retry signed in the **same second** is a
  replay and gets 401. Clients retrying a batch must re-sign with a fresh
  timestamp — the data layer still deduplicates the readings themselves.
- A 429 does not burn the nonce: rate limiting runs before the replay
  insert, so the same request can be retried after `Retry-After` seconds.

### Rate limiting (distributed token bucket)

One bucket row per API key in `rate_limit_buckets`. Defaults: capacity
**120** requests, refill **20** requests/second (burst-friendly: a fleet
gateway can batch freely, sustained abuse is capped). A single atomic
statement refills from elapsed time, caps at capacity, and consumes one
token only if available:

```sql
UPDATE rate_limit_buckets
SET tokens = LEAST(:capacity, tokens + elapsed_seconds * :refill) - 1, ...
WHERE api_key = :key AND LEAST(:capacity, tokens + elapsed_seconds * :refill) >= 1
```

The row lock serializes consumers; under READ COMMITTED the `WHERE` clause
is re-evaluated after a lock wait, so concurrent requests — across threads
**and across API instances** — can never overdraw the bucket (verified with
20 concurrent requests and with two uvicorn processes sharing one database).
`Retry-After` is `ceil(seconds until one token refills)`, minimum 1.
Authentication failures and replays roll back, refunding any consumed token.

Tunables (environment variables, defaults shown):

```
AUTH_TIMESTAMP_WINDOW_SECONDS=300
RATE_LIMIT_CAPACITY=120
RATE_LIMIT_REFILL_PER_SECOND=20.0
```

## Telemetry ingestion API

### `POST /v1/telemetry/batch`

Request — 1 to 500 readings:

```json
{
  "readings": [
    {
      "vehicle_id": "VG-00417",
      "seq": 88213,
      "ts": "2026-11-18T09:14:05.250Z",
      "soc_pct": 71.4,
      "battery_temp_c": 38.2,
      "speed_kmh": 23.5,
      "lat": 13.0827,
      "lng": 80.2707,
      "odometer_km": 1203.4,
      "charging": false,
      "fault_codes": ["P0A80"]
    }
  ]
}
```

Validation per reading: all fields required except `fault_codes` (defaults to
`[]`, must be a list of strings); `seq` positive integer; `ts` timezone-aware;
`soc_pct` 0–100; `lat` ±90; `lng` ±180; `speed_kmh`/`odometer_km` ≥ 0;
`battery_temp_c` −50…150 (cold-weather readings stay valid).

Status codes:
- **200** — batch processed; inspect per-reading `status`
  (`accepted` / `duplicate` / `rejected`, with `errors` for rejections).
  One bad reading never rejects the batch.
- **401** — authentication failed (see [Device authentication](#device-authentication-a2));
  the response is identical for every failure mode.
- **429** — rate limit exceeded; retry after the `Retry-After` header's seconds.
- **422** — invalid envelope: empty batch, more than 500 readings, malformed JSON.
- **503** — database unavailable. Readings already committed stay committed;
  retrying the batch is safe (idempotent — re-sign with a fresh timestamp).

Response:

```json
{
  "accepted": 2, "duplicates": 0, "rejected": 1,
  "results": [
    {"index": 0, "vehicle_id": "VG-00417", "seq": 88213, "status": "accepted"},
    {"index": 1, "vehicle_id": "VG-00417", "seq": 88214, "status": "rejected",
     "errors": [{"field": "soc_pct", "message": "Input should be less than or equal to 100"}]}
  ],
  "vehicles": [
    {"vehicle_id": "VG-00417", "min_seq": 88213, "max_seq": 88215,
     "received_count": 2, "missing_count": 1, "missing_seqs": [88214],
     "missing_seqs_truncated": false, "gaps_filled": [], "completeness_pct": 66.67}
  ]
}
```

Behavior:
- **Duplicates:** `INSERT … ON CONFLICT ON CONSTRAINT uq_telemetry_readings_vehicle_seq
  DO NOTHING` — safe under any concurrency (verified with 20 simultaneous clients).
  A duplicate never creates a row or changes current state.
- **Out-of-order:** late readings are stored in history, but current state only
  changes when the device timestamp is strictly newer, via an atomic conditional
  `UPDATE … WHERE latest_device_ts < :ts`. Highest sequence number does not win —
  highest device timestamp does. `last_seen_at` always uses server receive time.
- **Sequence gaps:** derived from stored readings per vehicle — `missing_count`
  is exact (`max − min + 1 − count`); `missing_seqs` lists up to 100 holes for
  ranges ≤ 10,000 (else `missing_seqs_truncated: true`). When a missing seq
  arrives it appears in `gaps_filled` and completeness rises. No extra tables;
  reporting is recomputed per batch so it can never go stale.

Example (sign the exact bytes you send — see the Python snippet above):

```bash
BODY='{"readings":[{"vehicle_id":"VG-00417","seq":1,"ts":"2026-11-18T09:14:05.250Z","soc_pct":71.4,"battery_temp_c":38.2,"speed_kmh":23.5,"lat":13.0827,"lng":80.2707,"odometer_km":1203.4,"charging":false,"fault_codes":[]}]}'
TS=$(date +%s)
SIG=$(API_KEY="$API_KEY" SECRET="$SECRET" TS="$TS" BODY="$BODY" python -c "import hmac,hashlib,os;print(hmac.new(os.environ['SECRET'].encode(), os.environ['API_KEY'].encode()+b'\n'+os.environ['TS'].encode()+b'\n'+os.environ['BODY'].encode(), hashlib.sha256).hexdigest())")
curl -s http://localhost:8000/v1/telemetry/batch \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: $API_KEY" -H "X-Timestamp: $TS" -H "X-Signature: $SIG" \
  -d "$BODY"
```

## Alert engine

Four rules run automatically. Three are evaluated inside the ingestion
transaction whenever a **new** reading is stored (duplicates never trigger
evaluation); offline is detected by a background worker.

| Rule | Trigger | Resolution |
|---|---|---|
| `overheat` | 3 consecutive readings > 55 °C | 3 consecutive readings ≤ 50 °C |
| `rapid_discharge` | SOC drops > 8 points within 5 minutes while not charging | drop falls back to ≤ 8 points (window slides / charging intervenes) |
| `geofence_breach` | newest reading outside the operating polygon | newest reading back inside (edges count as inside) |
| `offline` | no reading received for > 90 s (server `last_seen_at`) | any new valid reading arrives — even a duplicate |

Behavior:
- **Order-independent:** rules are recomputed from device-time-ordered history
  and evaluated at the vehicle's newest reading (the *frontier*). Out-of-order
  and replayed batches converge to the same active-alert state. Hysteresis:
  an open alert stays open until its explicit resolution condition holds
  (e.g. 52 °C neither triggers nor resolves overheat).
- **Dedup:** at most one active (`open`/`acknowledged`) alert per
  (vehicle, rule), enforced by the partial unique index
  `uq_alerts_active_vehicle_rule … WHERE status <> 'resolved'` plus
  `ON CONFLICT DO NOTHING` — safe under concurrent ingestion and multiple
  API instances. After resolution, a recurrence opens a **new** alert;
  resolved alerts stay in history.
- **Offline worker:** an asyncio task started by the app's lifespan sweeps
  every 15 s (configurable). One set-based `INSERT … SELECT` opens alerts for
  all stale vehicles; `pg_try_advisory_xact_lock` ensures only one instance
  sweeps per cycle and `FOR UPDATE SKIP LOCKED` avoids blocking ingestion.
  No extra infrastructure — it runs inside the API container, so plain
  `docker compose up` starts everything.
- **Concurrency:** alert evaluation runs in the same transaction that holds
  the vehicle row lock, serializing per-vehicle alert work. Acknowledge uses
  optimistic concurrency on `version` (see below).

Tunables (environment variables, defaults shown):

```
OFFLINE_AFTER_SECONDS=90
OFFLINE_CHECK_INTERVAL_SECONDS=15
DISCHARGE_DROP_PCT=8
DISCHARGE_WINDOW_SECONDS=300
GEOFENCE_POLYGON=[[12.8,79.9],[13.3,79.9],[13.3,80.5],[12.8,80.5]]
```

The default polygon is a rectangle around Chennai; override with any simple
polygon as JSON `[lat, lng]` vertices.

### `GET /v1/alerts`

Query parameters: `vehicle_id`, `status` (`open`/`acknowledged`/`resolved`),
`rule_type`, `limit` (1–500, default 100). Newest first.

```bash
curl -s 'http://localhost:8000/v1/alerts?vehicle_id=VG-00417&status=open'
```

### `POST /v1/alerts/{id}/ack`

Acknowledges an **open** alert. The client must send the `version` it last
saw; the update is a single atomic
`UPDATE … WHERE id = :id AND version = :version AND status = 'open'`.

```bash
curl -s -X POST http://localhost:8000/v1/alerts/42/ack \
  -H 'Content-Type: application/json' -d '{"version": 1}'
```

- **200** — acknowledged; `version` incremented, `acknowledged_at` set.
- **409** — stale version (someone else updated first) or the alert is not
  `open` (already acknowledged/resolved). Re-fetch and retry if appropriate.
- **404** — alert does not exist. **422** — invalid body (`version` must be ≥ 1).

Two operators acknowledging simultaneously with the same version: exactly
one gets 200, the other 409 — verified by a 10-thread test.

Known limitation: the *active* alert state is order-independent, but under
extreme reordering the historical rows can differ — an episode that both
starts and ends inside a late-arriving window may never materialize as an
alert row, because evaluation only happens at the frontier.

See [DESIGN.md](DESIGN.md) for the architectural decisions behind the engine.

## Query API

### Vehicle status (derived, shared by all endpoints)

One SQL `CASE`, evaluated consistently everywhere, precedence top to bottom:

| Status | Condition |
|---|---|
| `offline` | `last_seen_at` is null (never reported) or older than `OFFLINE_AFTER_SECONDS` (90 s) |
| `charging` | current state `charging = true` |
| `active` | current state `speed_kmh > 0` |
| `idle` | otherwise |

### `GET /v1/vehicles`

Keyset (cursor) pagination — **no OFFSET**. Sorted by `vehicle_id` ascending;
`vehicle_id` is unique, so it is its own tie-breaker and the order is total.

Parameters:
- `limit` — 1–200, default 50.
- `cursor` — opaque value from the previous page's `next_cursor`.
- `status` — one of `offline`, `charging`, `active`, `idle`.
- `min_lat`, `min_lng`, `max_lat`, `max_lng` — bounding box on the vehicle's
  current position. All four required together; lat ±90, lng ±180,
  min ≤ max, else 422.

Cursor format: URL-safe base64 of `{"vehicle_id": "<last id on the page>"}`.
The next page is `WHERE vehicle_id > cursor ORDER BY vehicle_id LIMIT n` —
insertions before the cursor never shift later pages, so paging neither
skips nor duplicates vehicles while new data arrives. Malformed cursors → 422.

```bash
curl -s 'http://localhost:8000/v1/vehicles?limit=2&status=active'
curl -s 'http://localhost:8000/v1/vehicles?limit=2&cursor=eyJ2ZWhpY2xlX2lkIjogIlZHLTAwNDE3In0='
curl -s 'http://localhost:8000/v1/vehicles?min_lat=12.8&min_lng=79.9&max_lat=13.3&max_lng=80.5'
```

Response: `{"vehicles": [...], "limit": 50, "has_more": true, "next_cursor": "..."}`.
Each vehicle carries current state, derived `status`, and seq counters —
no telemetry history is loaded.

### `GET /v1/vehicles/{vehicle_id}/history` *(alias: `/v1/vehicles/{vehicle_id}/telemetry`)*

Chronological history (`ts, seq` ascending) served by the
`(vehicle_id, ts)` index. Both `/history` and `/telemetry` invoke the same
handler and return the identical schema.

Parameters:
- `from`, `to` — **required**, timezone-aware ISO 8601; `from < to`;
  window ≤ 31 days (prevents unbounded scans).
- `points` — 4–5000. When the range holds more readings than `points`,
  the series is **min/max downsampled** (below).
- `cursor` — raw mode only (omit `points`): keyset continuation after the
  10,000-row raw cap, base64 of `{"ts": ..., "seq": ...}`.

Unknown vehicle → 404. Empty range → `readings: []`, `raw_count: 0`.

**Min/max downsampling** (peak-preserving, computed entirely in SQL):
the range is split into `(points − 2) // 2` equal time buckets; each bucket
contributes the *actual rows* holding the lowest and highest `soc_pct`
(`DISTINCT ON (bucket)`), plus the exact first and last rows of the range.
`UNION` deduplicates rows picked twice; output is re-sorted chronologically
and never exceeds `points`. Spikes and troughs survive verbatim — this is
not every-nth sampling and not averaging.

```bash
curl -s 'http://localhost:8000/v1/vehicles/VG-00417/history?from=2026-11-20T00:00:00Z&to=2026-11-21T00:00:00Z&points=500'
```

Response: `{"vehicle_id", "from", "to", "raw_count", "returned_count",
"downsampled", "bucket_seconds", "next_cursor", "readings": [...]}`.

### `GET /v1/fleet/summary`

```bash
curl -s http://localhost:8000/v1/fleet/summary
```

```json
{
  "vehicles_total": 2000,
  "vehicles_by_status": {"offline": 12, "charging": 180, "active": 1200, "idle": 608},
  "avg_soc_pct": 63.4,
  "open_alerts_by_rule": {"overheat": 3, "rapid_discharge": 1, "geofence_breach": 2, "offline": 12},
  "readings_total": 5000000,
  "data_completeness_pct": 99.98
}
```

- `avg_soc_pct` — average of current-state SOC across vehicles that have
  reported (null for an empty fleet).
- `open_alerts_by_rule` — alerts with `status = 'open'` (acknowledged alerts
  are not open), all four rules always present.
- `data_completeness_pct` — fleet-wide `received / (max_seq − min_seq + 1)`
  using per-vehicle seq counters maintained transactionally at ingestion
  (`vehicles.min_seq / max_seq / received_count`). Null for an empty fleet.

Two queries total: one aggregate over the small `vehicles` table and one
`GROUP BY` over open alerts (`ix_alerts_rule_type_status`). Cost is
O(vehicles), independent of the readings table size — see DESIGN.md and
`scripts/benchmark_summary.py` for the 5M-row benchmark.

### Fleet summary benchmark (measured)

Reproducible via `scripts/benchmark_summary.py` (explicit operation — never
part of startup or tests):

```bash
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet python scripts/benchmark_summary.py --generate          # ~5M readings, ~3 min
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet python scripts/benchmark_summary.py --bench --samples 200
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet python scripts/benchmark_summary.py --explain           # EXPLAIN ANALYZE
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet python scripts/benchmark_summary.py --cleanup           # remove BENCH-% rows
```

Method: 2,000 vehicles × 2,500 readings (5 s cadence over ~3.5 h, randomized
SOC/temp/speed/position, periodic charging), generated with set-based SQL in
185 s — generation time is separate from measurement. 10 warm-up requests,
then 200 sequential HTTP samples against the Dockerized API.

Environment: PostgreSQL 16.16 (postgres:16-alpine, `shared_buffers=128MB`,
`work_mem=4MB`), Docker Desktop on Windows 10 (AMD64 Family 25, 16 logical
CPUs), API in the same compose network.

| Metric | Result |
|---|---|
| p50 | **8.3 ms** |
| p95 | **11.6 ms** (target < 150 ms) |
| p99 | 15.9 ms |
| min / max / mean | 4.9 / 18.6 / 8.5 ms |

EXPLAIN ANALYZE (5,000,027 readings, 2,008 vehicles):
- vehicles aggregate: **1.37 ms** — seq scan of 2,008 rows, 38 shared buffers.
- open alerts by rule: **0.29 ms** — index-only scan on
  `ix_alerts_rule_type_status`, 0 heap fetches.
- naive alternative (`GROUP BY vehicle_id` over raw readings, **not used**):
  **492 ms** parallel seq scan of 5M rows — 3× over target. This is why
  completeness uses the transactionally maintained seq counters.

### Verbatim `EXPLAIN (ANALYZE, BUFFERS)` — two most important queries (A6)

Captured via `python scripts/benchmark_summary.py --explain` on PostgreSQL 16.16 (`shared_buffers=128MB`, `work_mem=4MB`):

**1. `GET /v1/fleet/summary` (vehicles aggregate + open alerts by rule):**

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

**2. `GET /v1/vehicles/{id}/history?from=&to=&points=200` (24-hour window, 17,280 raw readings → 199 peak/trough-preserving points):**

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

## Observability (A6)

- **Structured JSON logs & `X-Request-ID`** (`app/observability.py`): every HTTP request is assigned a correlation ID (reusing a valid client `X-Request-ID` header or generating a UUID4), returned in the `X-Request-ID` response header, and logged as single-line JSON (`ts`, `level`, `logger`, `request_id`, `method`, `path`, `route`, `status_code`, `duration_ms`, `message`). Secrets and signatures are never logged.
- **Health & readiness:** `GET /health` (liveness) and `GET /ready` (real PostgreSQL connectivity check, `503` on failure).
- **Basic metrics:** `GET /metrics` returns JSON counters and bounded-memory latency stats (`http_requests_total`, `http_errors_total`, `http_request_duration_ms`, `ingestion_readings_total` by `accepted`/`duplicate`/`rejected`, `alerts_total` by transition, `sse_active_connections`). Pass `?format=prometheus` (or `Accept: text/plain`) for Prometheus exposition format.

## Load testing (A6)

Two ready-to-run mixed-workload load test scripts live in `loadtest/` (concurrent HMAC-signed batch ingestion with 60% valid / 40% invalid payloads alongside concurrent operator queries):

```bash
# Option 1: k6
SIM_API_KEY=vg-... SIM_API_SECRET=... k6 run loadtest/k6_load_test.js

# Option 2: zero-extra-dependency async Python runner (seeds & cleans up 24h 17,280-row history)
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet \
  SIM_API_KEY=vg-... SIM_API_SECRET=... python loadtest/run_load_test.py --duration 15
```

Measured mixed-workload run (`15.13 s`, 4 concurrent ingest workers sending 25-reading batches + 6 concurrent query workers against the Dockerized API on Windows Docker Desktop):

| Endpoint | Requests | HTTP 200 | 429/Err | RPS | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `GET /v1/alerts` | 364 | 364 | 0 | 24.1 | 50.4 | 46.9 | 86.9 | 116.3 |
| `GET /v1/fleet/summary` | 366 | 366 | 0 | 24.2 | 53.1 | 50.8 | 74.4 | 93.2 |
| `GET /v1/vehicles` | 367 | 367 | 0 | 24.3 | 45.7 | 44.3 | 62.8 | 95.9 |
| `GET /v1/vehicles/{id}/history` (24h/17,280→200) | 367 | 367 | 0 | 24.3 | 96.0 | 91.9 | 128.0 | 152.0 |
| `POST /v1/telemetry/batch` (25 readings, 60/40) | 33 | 33 | 0 | 2.2 | 1782.9 | 1853.6 | 2121.4 | 2241.9 |

Ingestion verdicts during load test: `accepted=528`, `duplicates=0`, `rejected=297`, `unexpected_invalid_accepted=0` (0% error rate).

## Fleet simulator

`python -m simulator` generates a seeded fleet of virtual two-wheelers and
sends every reading through the **real** signed ingestion API — nothing is
written to PostgreSQL directly. Each vehicle owns its state (seq, device
clock with ±drift, SOC, temperature, speed, position, odometer, charging,
fault codes) and advances it realistically every tick.

### Run it (Docker)

The simulator is an **optional** compose service (`simulation` profile) —
plain `docker compose up` never starts a simulation:

```bash
docker compose up -d --build
docker compose exec api python -m app.manage create-group --name sim   # prints key + secret once
SIM_API_KEY=vg-... SIM_API_SECRET=... docker compose --profile simulation run --rm simulator
```

Defaults: 2,000 vehicles, 60 s, 5 s interval (≈400 readings/s generated),
batch 250, concurrency 8, seed 42. Override via `SIM_VEHICLES`,
`SIM_DURATION`, `SIM_INTERVAL`, `SIM_BATCH_SIZE`, `SIM_CONCURRENCY`,
`SIM_SEED`, or pass CLI flags directly (they replace the default flags):

```bash
SIM_API_KEY=vg-... SIM_API_SECRET=... docker compose --profile simulation run --rm simulator \
  --vehicles 500 --duration 30 --seed 7 --request-timeout 60
```

### Run it (local)

```bash
SIM_API_KEY=vg-... SIM_API_SECRET=... python -m simulator --api-base http://localhost:8000
```

### Configuration (CLI flags, defaults shown)

| Flag | Default | Meaning |
|---|---|---|
| `--vehicles` | 2000 | fleet size |
| `--duration` | 60 | seconds of generation |
| `--interval` | 5 | seconds between ticks per vehicle |
| `--batch-size` | 250 | readings per POST (validated 1–500) |
| `--concurrency` | 8 | simultaneous in-flight requests |
| `--seed` | 42 | RNG seed — same seed + same `now` ⇒ identical readings |
| `--vehicle-prefix` | SIM | vehicle ids are `{prefix}-{i:05d}` |
| `--valid-pct` / `--invalid-pct` | 60 / 40 | configurable valid vs intentionally invalid reading distribution (`SIM_VALID_PCT` / `SIM_INVALID_PCT`) |
| `--malformed-prob` | *(alias)* | legacy `[0, 1]` alias for `--invalid-pct` |
| `--duplicate-prob` | 0.05 | chance a reading is sent twice |
| `--out-of-order-prob` | 0.10 | chance a reading is emitted after its successor |
| `--delayed-prob` | 0.05 | chance a reading is held back |
| `--max-delay` | 60 | max hold-back seconds (raise toward 600 for 10-minute late arrivals) |
| `--network-drop-prob` | 0.01 | chance a request attempt is silently dropped (then retried) |
| `--clock-drift` | 30 | per-vehicle device clock offset bound, ±seconds |
| `--offline-prob` | 0.02 | fraction of vehicles prone to going silent |
| `--offline-silence` | 120 | seconds a triggered silence window lasts |
| `--offline-trigger-prob` | 0.05 | per-tick chance an offline-prone vehicle goes silent |
| `--abnormal-fraction` | 0.05 | fraction with scripted abnormal profiles |
| `--home-lat` / `--home-lng` | 13.05 / 80.25 | fleet home area (keep inside `GEOFENCE_POLYGON`) |
| `--retry-max` | 6 | retries per batch (re-signed with a fresh timestamp) |
| `--retry-backoff` | 0.5 | exponential backoff base seconds (429 honors `Retry-After`) |
| `--request-timeout` | 15 | per-request HTTP timeout seconds |

Credentials come from `SIM_API_KEY` / `SIM_API_SECRET` (or `--api-key` /
`--api-secret`); the simulator refuses to start without them and never
prints them.

### Behavior profiles

Most vehicles are `standard` (random-walk speed, SOC drain while moving,
temperature tracking speed, occasional fault codes). The abnormal fraction
cycles through scripted profiles so alert rules are exercised
deterministically: `overheat` (8 hot ticks > 55 °C, then 8 cool ticks
≤ 50 °C), `discharge` (6-tick hard SOC drain > 8 points inside the 5-minute
window), `breacher` (drives out of the operating polygon and back),
`charger` (alternating drive/charge blocks). `offline_prone` vehicles
randomly go silent for `--offline-silence` seconds, triggering offline
alerts and recovery events.

### Fault injection, data quality, and resilience

By default the simulator generates **60% valid / 40% intentionally invalid**
readings (configurable via `--valid-pct` / `--invalid-pct`) across **20
documented invalid scenarios** (SOC > 100, negative SOC, null/string/boolean
numeric types, invalid coordinates, negative speed/odometer, non-positive or
non-int `seq`, malformed/naive/epoch timestamps, out-of-range battery
temperature, invalid `charging`/`fault_codes`, missing required fields). Valid
readings include abnormal operational profiles (`overheat`, `discharge`,
`breacher`, `charger`, `low_battery`, `offline_prone`). Duplicates,
out-of-order emission, delayed delivery, and simulated network drops are
tracked separately. Every server verdict is audited against expected validity:
if any invalid reading is accepted by the backend, the simulator flags it and
exits with status `1`. Batches retry with exponential backoff and a **fresh
signature**; 401/422 are never retried. SIGINT/SIGTERM drains pending work
before exiting.

### Terminal report

Every number is counted or timed during the run — nothing is estimated:
generated vs injected faults, requests sent/succeeded/failed, HTTP status
distribution, avg/p50/p95/p99 request latency, server-side
accepted/duplicate/rejected totals (from API response bodies), measured
duration, and generated/accepted throughput. With no simulated drops or
failed requests, `accepted + duplicates + rejected` equals
`generated + injected duplicates` exactly.

### 2,000-vehicle run (measured, not estimated)

Executed against the Dockerized API (`docker compose --profile simulation
run --rm simulator`, defaults above, `--request-timeout 60`), Docker
Desktop on Windows, `postgres:16-alpine`, single uvicorn worker, 8
concurrent batches of 250, seed 42:

| Metric | Result |
|---|---|
| Vehicles | 2,000 (1,999 reported; 1 offline-prone vehicle silent for the whole run) |
| Readings generated | 23,900 (12 ticks × 5 s; 100 ticks skipped by offline silence) |
| Faults injected | 1,186 duplicates · 1,975 out-of-order · 1,204 delayed · 478 malformed · 17 offline windows |
| Requests | 128 sent, **128 × HTTP 200**, 0 failed; 2 simulated drops recovered by retry |
| Latency avg / p50 / p95 / p99 | 11,090 / 14,196 / 15,236 / 15,486 ms |
| Server verdicts | 23,422 accepted · 1,166 duplicate · 498 rejected |
| Accounting identity | 23,422 + 1,166 + 498 = 25,086 = 23,900 + 1,186 ✓ |
| Measured duration | 180.46 s (60 s generation + bounded-queue drain) |
| Throughput | 132.4 generated/s · 129.8 accepted/s |

Database cross-check: 23,422 stored readings (== accepted), 29,925 events
(23,422 `telemetry_update`, 6,451 `vehicle_status_change`, 36
`alert_opened`, 14 `vehicle_offline`, 1 `vehicle_recovered`, 1
`alert_resolved`); 22 overheat alerts opened (all 22 overheat-profile
vehicles), 13 offline open + 1 resolved. During the run, SSE delivered
hundreds of events per 5 s window, a `Last-Event-ID` reconnect resumed
exactly (149784 → 149785) with zero gap/reset notices, and a live
`POST /v1/alerts/{id}/ack` produced an `alert_acknowledged` event on the
stream.

**Honest limitation:** sustained accepted throughput (~130 readings/s)
was below the 400 readings/s generation target on this machine — the
per-reading ingestion pipeline (state update + gap report + three rule
evaluations + outbox writes) costs ~14 s per 250-reading batch under 8-way
concurrency. The bounded queue backpressured generation instead of losing
data, and the run drained completely with zero loss. Batch-level
throughput optimization is deferred to the performance phase; no claim
beyond these measured numbers is made. A 60 s run covers overheat
triggering and offline detection/recovery; full trigger→resolve cycles and
geofence crossings need longer runs (they are covered by the test suite).

## Live streaming (SSE)

### `GET /v1/stream`

Server-Sent Events over a **PostgreSQL-backed event log** (`fleet_events`).
No authentication (read/ops surface, like the query API). Events are
written by a transactional outbox: every event row is INSERTed in the same
transaction as the state change it describes, so a rollback emits nothing
and a commit can never lose its event.

```bash
curl -N http://localhost:8000/v1/stream                       # live tail
curl -N -H 'Last-Event-ID: 12345' http://localhost:8000/v1/stream   # resume
curl -N -H 'Last-Event-ID: 0' http://localhost:8000/v1/stream       # replay retained history
```

Event types: `telemetry_update` (new reading stored), `vehicle_status_change`
(derived status transition), `alert_opened`, `alert_acknowledged`,
`alert_resolved`, `vehicle_offline`, `vehicle_recovered`.

Wire format — one consistent envelope, ids are the log's globally ordered
primary key:

```
id: 12346
event: telemetry_update
data: {"id":12346,"type":"telemetry_update","ts":"2026-11-20T15:21:07.123456+00:00","vehicle_id":"SIM-00042","payload":{"seq":7,"ts":"...","soc_pct":63.2,"battery_temp_c":31.4,"speed_kmh":22.1,"lat":13.08,"lng":80.27,"odometer_km":1203.4,"charging":false,"fault_codes":[]}}

```

Control events (no `id:` — they are not part of the resumable log):

- `stream_reset` — on connect with an unusable cursor:
  `unknown_last_event_id` (id ahead of the log; stream continues from the
  top) or `expired_last_event_id` (id deleted by retention; stream
  continues from the oldest retained event, `oldest_available_id` included).
- `stream_gap` — a lagging cursor lost events: `retention_expired`
  (retention trimmed past the cursor) or `events_unavailable` (ids never
  materialized, e.g. rolled-back transactions). Carries `last_delivered_id`
  and `next_available_id`. Loss is always explicit, never silent.
- `: keepalive` comments every `SSE_HEARTBEAT_SECONDS` (15 s).

Invalid `Last-Event-ID` (non-integer or negative) → **400**.

### Resumption and multi-instance

The cursor is the last event id delivered. Because the log lives in
PostgreSQL, any instance can serve any client: reconnect to a **different**
instance with `Last-Event-ID` and continue exactly where you stopped; an
API restart loses nothing. Ids come from a `BIGINT GENERATED ALWAYS AS
IDENTITY` primary key, so ordering is global across instances.

Concurrent ingestion transactions reserve ids in order but can commit out
of order. The stream therefore advances only through **contiguous** ids
and holds a hole near the top of the log for `SSE_GAP_GRACE_SECONDS`
(30 s) before declaring it unavailable — a late-committing transaction is
delivered in order, never skipped and never misreported as a gap.

### Retention

The watchdog trims `fleet_events` each cycle by age
(`EVENT_RETENTION_SECONDS`, 900 s = 15 min) and by count
(`EVENT_RETENTION_MAX_ROWS`, 200,000). Reconnecting with an id older than
the retained range yields an explicit `stream_reset`/`stream_gap`, never a
silent jump.

### Backpressure and coalescing

Per-connection memory is bounded: a coalescer holds at most **2 pending
events per vehicle** (one `telemetry_update`, one `vehicle_status_change`).
Within `SSE_COALESCE_WINDOW_SECONDS` (1 s), replaceable telemetry collapses
to the newest per vehicle/type; **critical events (alert lifecycle,
offline/recovery) are never merged or dropped** and flush pending
replaceable events first so wire order always matches id order. A slow
client stalls only its own socket write — ingestion and other subscribers
are unaffected (readers never take write locks). Setting the window to 0
disables coalescing (every event passes through).

Tunables (environment variables, defaults shown):

```
SSE_POLL_INTERVAL_SECONDS=0.5
SSE_COALESCE_WINDOW_SECONDS=1.0
SSE_BATCH_LIMIT=500
SSE_HEARTBEAT_SECONDS=15.0
SSE_GAP_GRACE_SECONDS=30.0
EVENT_RETENTION_SECONDS=900
EVENT_RETENTION_MAX_ROWS=200000
```

See [DESIGN.md](DESIGN.md) for the outbox/streaming architecture and the
simulator design, and the completion report for measured 2,000-vehicle
results.

## Schema and migrations

Models live in `app/models/` (`Vehicle`, `TelemetryReading`, `Alert`,
`DeviceGroup`, `RequestNonce`, `RateLimitBucket`, `FleetEvent`).

- `vehicles` — current state per vehicle (no history), plus seq counters
  (`min_seq`, `max_seq`, `received_count`) maintained transactionally on
  insert so fleet completeness is an O(vehicles) aggregate.
- `telemetry_readings` — append-only history; `UNIQUE (vehicle_id, seq)`
  (`uq_telemetry_readings_vehicle_seq`) is the foundation for idempotent
  ingestion. Range checks on measurements; JSONB `fault_codes`.
- `alerts` — lifecycle (`open`/`acknowledged`/`resolved` via check constraint),
  `version` for optimistic concurrency, JSONB `details`.
- `device_groups` — `api_key` (unique) + `secret` + name; provisioned via
  `app.manage`, secrets never exposed through any endpoint.
- `request_nonces` — replay protection; primary key is the request signature,
  `expires_at` = first use + 2× timestamp window.
- `rate_limit_buckets` — one token-bucket row per API key
  (`tokens`, `last_refill`); the row lock is the distributed mutex.
- `fleet_events` — the SSE event log (transactional outbox): `BIGINT
  GENERATED ALWAYS AS IDENTITY` primary key = globally ordered event id,
  `event_type`, `vehicle_id`, `ts`, JSONB `payload`. Written in the same
  transaction as the state change; trimmed by the watchdog (age + count).

Indexes:
- `ix_telemetry_readings_vehicle_ts (vehicle_id, ts)` — per-vehicle history by device time.
- `ix_telemetry_readings_received_at` — operational arrival-time scans (ingestion lag).
- `ix_alerts_vehicle_id` — alerts for one vehicle (FK lookups).
- `ix_alerts_rule_type_status (rule_type, status)` — dashboard filters.
- `ix_alerts_open (opened_at) WHERE status = 'open'` — small partial index of open alerts.
- `uq_alerts_active_vehicle_rule (vehicle_id, rule_type) WHERE status <> 'resolved'`
  — partial **unique** index: one active alert per vehicle+rule, resolved
  alerts excluded so recurrence can re-alert.
- `ix_request_nonces_expires_at` — cheap periodic cleanup of expired nonces.
- `ix_fleet_events_ts` — retention cleanup by age.

**Deletion policy:** both foreign keys use `ON DELETE RESTRICT` and the ORM
relationships have no delete cascade — deleting a vehicle that has readings or
alerts fails, so history can never be erased accidentally.

Apply migrations (from the project root, with the database running):

```bash
alembic upgrade head      # or: docker compose exec api alembic upgrade head
alembic downgrade base    # reverse
```

The api container runs `alembic upgrade head` automatically before starting
uvicorn, so a fresh `docker compose up --build` yields a fully migrated database.

Alembic loads the URL from `app/config.py` (`DATABASE_URL`); no credentials in
`alembic.ini`. Generate future migrations with `alembic revision --autogenerate -m "..."`.

## Version choices

Dependencies are pinned to exact versions for reproducible builds:

- `sqlalchemy==2.0.58` — SQLAlchemy 2.x typed-mapping API.
- `psycopg[binary]==3.3.9` — psycopg 3 sync driver; `binary` ships prebuilt wheels.
- `fastapi==0.162.5`, `uvicorn==0.54.0`, `pydantic-settings==2.15.0`,
  `alembic==1.20.1` — current stable releases, verified together by the test suite.
- `httpx2==2.14.0` — runtime dependency of the simulator (async HTTP client)
  and the test suite's streaming/SSE client.
- Dev: `pytest==8.4.2`.