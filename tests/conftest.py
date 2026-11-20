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