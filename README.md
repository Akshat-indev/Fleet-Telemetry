# VoltGrid Fleet Telemetry

FastAPI + PostgreSQL (SQLAlchemy 2.x, psycopg 3, Alembic), Docker Compose.

- **Phase 1:** app foundation, `/health` and `/ready`, Docker Compose.
- **Phase 2:** schema (`vehicles`, `telemetry_readings`, `alerts`) and Alembic migrations.

## Run with Docker

```bash
docker compose up --build
```

- API: http://localhost:8000 (`GET /health`, `GET /ready`)
- PostgreSQL: `db` service, port 5432, data persisted in the `pgdata` named volume.
- The API waits for the PostgreSQL health check before starting.

Credentials are configurable via `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`,
`POSTGRES_PORT` (see `.env.example`). Defaults are for local development only.

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

- `sqlalchemy>=2.0,<2.1` — SQLAlchemy 2.x API as required.
- `psycopg[binary]>=3.2,<4` — psycopg 3 sync driver; `binary` ships prebuilt wheels.
- `fastapi<1.0`, `uvicorn<1.0`, `pydantic-settings>=2.4,<3` — current stable lines,
  upper-bounded to avoid breaking major releases.