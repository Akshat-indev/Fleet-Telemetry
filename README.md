# VoltGrid Fleet Telemetry

Phase 1 foundation: FastAPI + PostgreSQL (SQLAlchemy 2.x, psycopg 3), Docker Compose.

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

## Version choices

- `sqlalchemy>=2.0,<2.1` — SQLAlchemy 2.x API as required.
- `psycopg[binary]>=3.2,<4` — psycopg 3 sync driver; `binary` ships prebuilt wheels.
- `fastapi<1.0`, `uvicorn<1.0`, `pydantic-settings>=2.4,<3` — current stable lines,
  upper-bounded to avoid breaking major releases.