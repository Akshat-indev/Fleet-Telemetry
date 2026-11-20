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

## Run with Docker

```bash
docker compose up --build
```

- API: http://localhost:8000 (`GET /health`, `GET /ready`)
- PostgreSQL: `db` service, port 5432, data persisted in the `pgdata` named volume.
- The API waits for the PostgreSQL health check before starting.

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

Start the database and run the tests:

**Git Bash / Linux / macOS**

```bash
docker compose up -d db
DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet pytest
```

**PowerShell**

```powershell
docker compose up -d db
$env:DATABASE_URL = "postgresql+psycopg://fleet:fleet@localhost:5432/fleet"
pytest
```

**CMD**

```bat
docker compose up -d db
set DATABASE_URL=postgresql+psycopg://fleet:fleet@localhost:5432/fleet
pytest
```

The `/ready` integration test hits a real PostgreSQL — no mocks. It is skipped
with a message when `DATABASE_URL` is not set. The 503 failure case is covered
by a separate unit test that always runs.

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

### `GET /v1/vehicles/{vehicle_id}/telemetry`

Chronological history (`ts, seq` ascending) served by the
`(vehicle_id, ts)` index.

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
curl -s 'http://localhost:8000/v1/vehicles/VG-00417/telemetry?from=2026-11-20T00:00:00Z&to=2026-11-21T00:00:00Z&points=500'
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
python scripts/benchmark_summary.py --generate          # ~5M readings, ~3 min
python scripts/benchmark_summary.py --bench --samples 200
python scripts/benchmark_summary.py --explain           # EXPLAIN ANALYZE
python scripts/benchmark_summary.py --cleanup           # remove BENCH-% rows
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

The full test suite (206 tests) also passes with the 5M-row fleet present.

## Schema and migrations

Models live in `app/models/` (`Vehicle`, `TelemetryReading`, `Alert`,
`DeviceGroup`, `RequestNonce`, `RateLimitBucket`).

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
- Dev: `pytest==8.4.2`, `httpx2==2.14.0` (Starlette's preferred TestClient backend).