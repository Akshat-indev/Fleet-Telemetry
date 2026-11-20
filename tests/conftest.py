import os

import pytest

_REAL_DATABASE_URL = os.environ.get("DATABASE_URL")

# Unit tests never connect; integration tests require a real DATABASE_URL.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg://test:test@localhost:5432/test"
)

from fastapi.testclient import TestClient

from app.main import app


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