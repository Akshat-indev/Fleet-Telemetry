import os

import pytest

_REAL_DATABASE_URL = os.environ.get("DATABASE_URL")

# Unit tests never connect; integration tests require a real DATABASE_URL.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://test:test@localhost:5432/test"
)

from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import app


@pytest.fixture(autouse=True)
def _default_alert_settings(monkeypatch):
    """Pin alert tunables to the assignment defaults so a developer's .env
    (e.g. a custom GEOFENCE_POLYGON) cannot change test expectations.
    Real environment variables take precedence over .env in pydantic-settings."""
    monkeypatch.setenv("OFFLINE_AFTER_SECONDS", "90")
    monkeypatch.setenv("OFFLINE_CHECK_INTERVAL_SECONDS", "15")
    monkeypatch.setenv("DISCHARGE_DROP_PCT", "8")
    monkeypatch.setenv("DISCHARGE_WINDOW_SECONDS", "300")
    monkeypatch.setenv(
        "GEOFENCE_POLYGON", "[[12.8,79.9],[13.3,79.9],[13.3,80.5],[12.8,80.5]]"
    )
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def real_database():
    if not _REAL_DATABASE_URL:
        pytest.skip(
            "DATABASE_URL is not set; point it at a real PostgreSQL "
            "to run the integration test"
        )


@pytest.fixture
def db_session(real_database):
    """Session bound to an outer transaction that is rolled back,
    so tests never leave data behind."""
    from sqlalchemy.orm import Session

    from app.database import engine

    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture
def vehicle(db_session):
    from app.models import Vehicle

    v = Vehicle(vehicle_id="VH-001")
    db_session.add(v)
    db_session.flush()
    return v


@pytest.fixture
def make_reading():
    from datetime import datetime, timezone
    from decimal import Decimal

    from app.models import TelemetryReading

    def _make(vehicle_id, seq=1, **overrides):
        values = dict(
            vehicle_id=vehicle_id,
            seq=seq,
            ts=datetime.now(timezone.utc),
            soc_pct=Decimal("55.50"),
            battery_temp_c=Decimal("25.00"),
            speed_kmh=Decimal("60.00"),
            lat=Decimal("12.971599"),
            lng=Decimal("77.594566"),
            odometer_km=Decimal("1234.56"),
            charging=False,
        )
        values.update(overrides)
        return TelemetryReading(**values)

    return _make


@pytest.fixture
def ingest(real_database):
    """TestClient plus a unique vehicle-id prefix. Ingestion commits real
    rows, so cleanup deletes everything created under the prefix."""
    import uuid

    from sqlalchemy import text

    from app.database import engine
    from app.main import app

    prefix = f"TST-{uuid.uuid4().hex[:10]}-"
    yield TestClient(app), prefix

    with engine.begin() as conn:
        for table in ("telemetry_readings", "alerts"):
            conn.execute(
                text(
                    f"DELETE FROM {table} WHERE vehicle_id IN "
                    "(SELECT id FROM vehicles WHERE vehicle_id LIKE :pattern)"
                ),
                {"pattern": prefix + "%"},
            )
        conn.execute(
            text("DELETE FROM vehicles WHERE vehicle_id LIKE :pattern"),
            {"pattern": prefix + "%"},
        )


@pytest.fixture
def payload():
    from datetime import datetime, timezone

    def _payload(vehicle_id, seq, ts=None, **overrides):
        if ts is None:
            ts = datetime.now(timezone.utc)
        if not isinstance(ts, str):
            ts = ts.isoformat()
        reading = dict(
            vehicle_id=vehicle_id,
            seq=seq,
            ts=ts,
            soc_pct=71.4,
            battery_temp_c=38.2,
            speed_kmh=23.5,
            lat=13.0827,
            lng=80.2707,
            odometer_km=1203.4,
            charging=False,
            fault_codes=[],
        )
        reading.update(overrides)
        return reading

    return _payload


@pytest.fixture
def vehicle_state(real_database):
    from sqlalchemy import text

    from app.database import engine

    def _state(vehicle_id):
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT * FROM vehicles WHERE vehicle_id = :v"),
                {"v": vehicle_id},
            ).mappings().first()

    return _state


@pytest.fixture
def reading_count(real_database):
    from sqlalchemy import text

    from app.database import engine

    def _count(vehicle_id):
        with engine.connect() as conn:
            return conn.execute(
                text(
                    "SELECT count(*) FROM telemetry_readings t "
                    "JOIN vehicles v ON v.id = t.vehicle_id "
                    "WHERE v.vehicle_id = :v"
                ),
                {"v": vehicle_id},
            ).scalar_one()

    return _count


@pytest.fixture
def alerts_for(ingest):
    client, _ = ingest

    def _alerts(vehicle_id, **params):
        response = client.get(
            "/v1/alerts", params={"vehicle_id": vehicle_id, **params}
        )
        assert response.status_code == 200
        return response.json()

    return _alerts


@pytest.fixture
def backdate_last_seen(real_database):
    from sqlalchemy import text

    from app.database import engine

    def _backdate(vehicle_id, seconds):
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE vehicles SET last_seen_at = "
                    "now() - make_interval(secs => :seconds) WHERE vehicle_id = :v"
                ),
                {"seconds": seconds, "v": vehicle_id},
            )

    return _backdate