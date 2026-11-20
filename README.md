# VoltGrid Fleet Telemetry

FastAPI + PostgreSQL (SQLAlchemy 2.x, psycopg 3, Alembic), Docker Compose.

- **Phase 1:** app foundation, `/health` and `/ready`, Docker Compose.
- **Phase 2:** schema (`vehicles`, `telemetry_readings`, `alerts`) and Alembic migrations.
- **Phase 3:** `POST /v1/telemetry/batch` — idempotent ingestion with per-reading
  results, device-time current state, and sequence-gap reporting.
- **Phase 4:** alert engine — overheat, rapid discharge, geofence breach,
  offline detection; alert lifecycle with optimistic-concurrency acknowledge.

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
- **422** — invalid envelope: empty batch, more than 500 readings, malformed JSON.
- **503** — database unavailable. Readings already committed stay committed;
  retrying the batch is safe (idempotent).

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

Example:

```bash
curl -s http://localhost:8000/v1/telemetry/batch \
  -H 'Content-Type: application/json' \
  -d '{"readings":[{"vehicle_id":"VG-00417","seq":1,"ts":"2026-11-18T09:14:05.250Z","soc_pct":71.4,"battery_temp_c":38.2,"speed_kmh":23.5,"lat":13.0827,"lng":80.2707,"odometer_km":1203.4,"charging":false,"fault_codes":[]}]}'
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

## Schema and migrations

Models live in `app/models/` (`Vehicle`, `TelemetryReading`, `Alert`).

- `vehicles` — current state per vehicle (no history).
- `telemetry_readings` — append-only history; `UNIQUE (vehicle_id, seq)`
  (`uq_telemetry_readings_vehicle_seq`) is the foundation for idempotent
  ingestion. Range checks on measurements; JSONB `fault_codes`.
- `alerts` — lifecycle (`open`/`acknowledged`/`resolved` via check constraint),
  `version` for optimistic concurrency, JSONB `details`.

Indexes:
- `ix_telemetry_readings_vehicle_ts (vehicle_id, ts)` — per-vehicle history by device time.
- `ix_telemetry_readings_received_at` — operational arrival-time scans (ingestion lag).
- `ix_alerts_vehicle_id` — alerts for one vehicle (FK lookups).
- `ix_alerts_rule_type_status (rule_type, status)` — dashboard filters.
- `ix_alerts_open (opened_at) WHERE status = 'open'` — small partial index of open alerts.
- `uq_alerts_active_vehicle_rule (vehicle_id, rule_type) WHERE status <> 'resolved'`
  — partial **unique** index: one active alert per vehicle+rule, resolved
  alerts excluded so recurrence can re-alert.

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