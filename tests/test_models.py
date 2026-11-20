from datetime import datetime, timezone

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Alert, Vehicle


def test_create_vehicle(db_session):
    v = Vehicle(vehicle_id="VH-100")
    db_session.add(v)
    db_session.flush()

    assert v.id is not None
    assert v.created_at is not None and v.created_at.tzinfo is not None
    assert v.updated_at is not None
    assert v.soc_pct is None and v.latest_seq is None


def test_insert_telemetry_reading(db_session, vehicle, make_reading):
    reading = make_reading(vehicle.id, seq=42, fault_codes=["P0A80"])
    db_session.add(reading)
    db_session.flush()

    assert reading.id is not None
    assert reading.received_at is not None and reading.received_at.tzinfo is not None
    assert reading.fault_codes == ["P0A80"]


def test_fault_codes_default_empty(db_session, vehicle, make_reading):
    reading = make_reading(vehicle.id)
    db_session.add(reading)
    db_session.flush()

    assert reading.fault_codes == []


def test_insert_alert(db_session, vehicle):
    alert = Alert(vehicle_id=vehicle.id, rule_type="low_soc", details={"soc_pct": 12})
    db_session.add(alert)
    db_session.flush()

    assert alert.id is not None
    assert alert.status == "open"
    assert alert.version == 1
    assert alert.opened_at is not None and alert.opened_at.tzinfo is not None
    assert alert.details == {"soc_pct": 12}
    assert alert.acknowledged_at is None and alert.resolved_at is None


def test_vehicle_telemetry_relationship(db_session, vehicle, make_reading):
    reading = make_reading(vehicle.id, ts=datetime.now(timezone.utc))
    db_session.add(reading)
    db_session.flush()

    assert reading.vehicle is vehicle
    assert reading in vehicle.telemetry_readings


def test_vehicle_alert_relationship(db_session, vehicle):
    alert = Alert(vehicle_id=vehicle.id, rule_type="high_temp")
    db_session.add(alert)
    db_session.flush()

    assert alert.vehicle is vehicle
    assert alert in vehicle.alerts


def test_vehicle_with_history_cannot_be_deleted(db_session, vehicle, make_reading):
    db_session.add(make_reading(vehicle.id))
    db_session.flush()

    with pytest.raises(IntegrityError):
        with db_session.begin_nested():
            db_session.delete(vehicle)
            db_session.flush()