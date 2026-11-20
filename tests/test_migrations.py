"""Migration test: creates a scratch database, applies upgrade head and
downgrade base with Alembic, then drops the scratch database."""

import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_DB = "fleet_migration_test"
APP_TABLES = {"vehicles", "telemetry_readings", "alerts"}


def test_migration_upgrade_and_downgrade_on_empty_database(real_database):
    base_url = make_url(os.environ["DATABASE_URL"])
    admin_engine = create_engine(
        base_url.set(database="postgres"), isolation_level="AUTOCOMMIT"
    )
    test_url = base_url.set(database=TEST_DB)

    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    cfg.set_main_option(
        "sqlalchemy.url", test_url.render_as_string(hide_password=False)
    )

    created = False
    test_engine = create_engine(test_url)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {TEST_DB}"))
        created = True

        command.upgrade(cfg, "head")

        inspector = inspect(test_engine)
        assert APP_TABLES <= set(inspector.get_table_names())
        unique_names = {
            c["name"] for c in inspector.get_unique_constraints("telemetry_readings")
        }
        assert "uq_telemetry_readings_vehicle_seq" in unique_names
        index_names = {i["name"] for i in inspector.get_indexes("telemetry_readings")}
        assert "ix_telemetry_readings_vehicle_ts" in index_names
        fk_names = {fk["name"] for fk in inspector.get_foreign_keys("alerts")}
        assert "fk_alerts_vehicle_id_vehicles" in fk_names

        command.downgrade(cfg, "base")

        inspector = inspect(test_engine)
        assert not APP_TABLES & set(inspector.get_table_names())
    finally:
        test_engine.dispose()
        if created:
            with admin_engine.connect() as conn:
                conn.execute(text(f"DROP DATABASE {TEST_DB}"))
        admin_engine.dispose()