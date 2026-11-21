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
    """Pin alert and security tunables to the assignment defaults so a
    developer's .env (e.g. a custom GEOFENCE_POLYGON) cannot change test
    expectations. Real environment variables take precedence over .env
    in pydantic-settings."""
    monkeypatch.setenv("OFFLINE_AFTER_SECONDS", "90")
    monkeypatch.setenv("OFFLINE_CHECK_INTERVAL_SECONDS", "15")
    monkeypatch.setenv("DISCHARGE_DROP_PCT", "8")
    monkeypatch.setenv("DISCHARGE_WINDOW_SECONDS", "300")
    monkeypatch.setenv(
        "GEOFENCE_POLYGON", "[[12.8,79.9],[13.3,79.9],[13.3,80.5],[12.8,80.5]]"
    )
    monkeypatch.setenv("AUTH_TIMESTAMP_WINDOW_SECONDS", "300")
    monkeypatch.setenv("RATE_LIMIT_CAPACITY", "120")
    monkeypatch.setenv("RATE_LIMIT_REFILL_PER_SECOND", "20.0")
    # Fast polling and a zero coalesce window make SSE tests deterministic:
    # every event passes through individually unless a test opts otherwise.
    monkeypatch.setenv("SSE_POLL_INTERVAL_SECONDS", "0.05")
    monkeypatch.setenv("SSE_COALESCE_WINDOW_SECONDS", "0")
    monkeypatch.setenv("SSE_BATCH_LIMIT", "500")
    monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "0.2")
    monkeypatch.setenv("SSE_GAP_GRACE_SECONDS", "10")
    monkeypatch.setenv("EVENT_RETENTION_SECONDS", "900")
    monkeypatch.setenv("EVENT_RETENTION_MAX_ROWS", "200000")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def make_signed_client(engine):
    """TestClient whose POSTs to /v1/telemetry/batch are auto-signed with a
    fresh device group created in `engine`. Credentials are exposed as
    client.api_key / client.api_secret for tests that build requests by hand.
    Explicit X-Signature headers are never overwritten."""
    import secrets
    import time
    import uuid

    from sqlalchemy import text

    from app.security import compute_signature

    api_key = "vg-test-" + uuid.uuid4().hex[:16]
    secret = secrets.token_hex(32)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO device_groups (api_key, secret, name) "
                "VALUES (:key, :secret, :name)"
            ),
            {"key": api_key, "secret": secret, "name": "test-group"},
        )

    def sign(request):
        if (
            request.method == "POST"
            and request.url.path == "/v1/telemetry/batch"
            and "X-Signature" not in request.headers
        ):
            request.read()
            ts = str(int(time.time()))
            request.headers["X-API-Key"] = api_key
            request.headers["X-Timestamp"] = ts
            request.headers["X-Signature"] = compute_signature(
                secret, api_key, ts, request.content
            )

    client = TestClient(app)
    client.event_hooks["request"].append(sign)
    client.api_key = api_key
    client.api_secret = secret
    return client


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
    client = make_signed_client(engine)
    yield client, prefix

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
        conn.execute(
            text("DELETE FROM fleet_events WHERE vehicle_id LIKE :pattern"),
            {"pattern": prefix + "%"},
        )
        for table in ("request_nonces", "rate_limit_buckets", "device_groups"):
            conn.execute(
                text(f"DELETE FROM {table} WHERE api_key = :key"),
                {"key": client.api_key},
            )


@pytest.fixture
def stream_server(isolated_client):
    """One real uvicorn instance over the scratch DB, for SSE tests that
    need genuine streaming sockets (TestClient buffers whole responses)."""
    from types import SimpleNamespace

    from tests.sse_harness import start_instances, stop_instances

    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    procs, bases = start_instances(db_url, 1)
    try:
        yield SimpleNamespace(
            base=bases[0], api_key=client.api_key,
            secret=client.api_secret, engine=engine,
        )
    finally:
        stop_instances(procs)


@pytest.fixture
def two_stream_instances(isolated_client):
    """Two independent uvicorn instances sharing the scratch DB."""
    from types import SimpleNamespace

    from tests.sse_harness import start_instances, stop_instances

    client, engine = isolated_client
    db_url = engine.url.render_as_string(hide_password=False)
    procs, bases = start_instances(db_url, 2)
    try:
        yield SimpleNamespace(
            bases=bases, procs=procs, api_key=client.api_key,
            secret=client.api_secret, engine=engine,
        )
    finally:
        stop_instances(procs)


@pytest.fixture
def auth_env(isolated_client):
    """Scratch DB + auto-signed client + unsigned client + helpers for
    building auth headers over arbitrary bodies/timestamps."""
    import json
    import time
    from types import SimpleNamespace

    from app.security import compute_signature

    client, engine = isolated_client
    plain = TestClient(app)

    def body(readings):
        return json.dumps({"readings": readings}).encode()

    def headers_for(raw, *, api_key=None, secret=None, timestamp=None):
        key = client.api_key if api_key is None else api_key
        sec = client.api_secret if secret is None else secret
        ts = str(int(time.time())) if timestamp is None else timestamp
        return {
            "X-API-Key": key,
            "X-Timestamp": ts,
            "X-Signature": compute_signature(sec, key, ts, raw),
        }

    return SimpleNamespace(
        client=client,
        plain=plain,
        engine=engine,
        api_key=client.api_key,
        secret=client.api_secret,
        body=body,
        headers_for=headers_for,
    )


@pytest.fixture
def signed_post():
    """Post a batch signed with an explicit timestamp offset. Replay
    protection rejects byte-identical requests signed in the same second,
    so tests that resend the same batch to exercise DATA-level dedup must
    make each resend a distinct signed request (fresh timestamp)."""
    import json
    import time

    from app.security import compute_signature

    # One fixed base per test: offsets must yield distinct timestamps even
    # when two calls straddle a wall-clock second boundary.
    base = int(time.time())

    def _post(client, readings, *, offset=0):
        body = json.dumps({"readings": readings}).encode()
        ts = str(base + offset)
        headers = {
            "X-API-Key": client.api_key,
            "X-Timestamp": ts,
            "X-Signature": compute_signature(
                client.api_secret, client.api_key, ts, body
            ),
        }
        return client.post("/v1/telemetry/batch", content=body, headers=headers)

    return _post


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


@pytest.fixture
def isolated_client(real_database, monkeypatch):
    """A fresh scratch PostgreSQL database migrated to head, wired into the
    app's session factories. A real database, fully isolated, so empty-fleet
    and exact-aggregate assertions are deterministic. Dropped on teardown."""
    import uuid
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url
    from sqlalchemy.orm import sessionmaker

    import app.alerts.worker as worker_module
    import app.ingestion as ingestion_module
    import app.main as main_module
    import app.stream as stream_module

    root = Path(__file__).resolve().parents[1]
    base_url = make_url(_REAL_DATABASE_URL)
    db_name = f"fleet_query_test_{uuid.uuid4().hex[:8]}"
    test_url = base_url.set(database=db_name)
    admin = create_engine(
        base_url.set(database="postgres"), isolation_level="AUTOCOMMIT"
    )
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    cfg.set_main_option(
        "sqlalchemy.url", test_url.render_as_string(hide_password=False)
    )
    with admin.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {db_name}"))
    engine = create_engine(test_url)
    try:
        command.upgrade(cfg, "head")
        factory = sessionmaker(bind=engine)
        monkeypatch.setattr(main_module, "SessionLocal", factory)
        monkeypatch.setattr(ingestion_module, "SessionLocal", factory)
        monkeypatch.setattr(worker_module, "SessionLocal", factory)
        monkeypatch.setattr(stream_module, "SessionLocal", factory)
        yield make_signed_client(engine), engine
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f"DROP DATABASE {db_name} WITH (FORCE)"))
        admin.dispose()