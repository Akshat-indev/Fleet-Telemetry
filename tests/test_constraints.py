from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Alert, Vehicle


def test_duplicate_vehicle_seq_rejected(db_session, vehicle, make_reading):
    db_session.add(make_reading(vehicle.id, seq=1))
    db_session.flush()

    with pytest.raises(IntegrityError, match="uq_telemetry_readings_vehicle_seq"):
        with db_session.begin_nested():
            db_session.add(make_reading(vehicle.id, seq=1))
            db_session.flush()


def test_same_seq_allowed_for_different_vehicles(db_session, make_reading):
    v1 = Vehicle(vehicle_id="VH-A")
    v2 = Vehicle(vehicle_id="VH-B")
    db_session.add_all([v1, v2])
    db_session.flush()

    db_session.add_all([make_reading(v1.id, seq=7), make_reading(v2.id, seq=7)])
    db_session.flush()


def test_duplicate_external_vehicle_id_rejected(db_session, vehicle):
    with pytest.raises(IntegrityError, match="uq_vehicles_vehicle_id"):
        with db_session.begin_nested():
            db_session.add(Vehicle(vehicle_id=vehicle.vehicle_id))
            db_session.flush()


@pytest.mark.parametrize(
    "field,value",
    [
        ("soc_pct", Decimal("101")),
        ("soc_pct", Decimal("-1")),
        ("battery_temp_c", Decimal("200")),
        ("speed_kmh", Decimal("-5")),
        ("lat", Decimal("91")),
        ("lng", Decimal("181")),
        ("odometer_km", Decimal("-1")),
    ],
)
def test_check_constraints_reject_invalid_values(
    db_session, vehicle, make_reading, field, value
):
    with pytest.raises(IntegrityError, match="ck_telemetry_readings_"):
        with db_session.begin_nested():
            db_session.add(make_reading(vehicle.id, **{field: value}))
            db_session.flush()


def test_invalid_alert_status_rejected(db_session, vehicle):
    with pytest.raises(IntegrityError, match="ck_alerts_status_valid"):
        with db_session.begin_nested():
            db_session.add(
                Alert(vehicle_id=vehicle.id, rule_type="low_soc", status="closed")
            )
            db_session.flush()


def test_reading_requires_existing_vehicle(db_session, make_reading):
    with pytest.raises(IntegrityError, match="fk_telemetry_readings_vehicle_id"):
        with db_session.begin_nested():
            db_session.add(make_reading(999_999))
            db_session.flush()


def test_alert_requires_existing_vehicle(db_session):
    with pytest.raises(IntegrityError, match="fk_alerts_vehicle_id"):
        with db_session.begin_nested():
            db_session.add(Alert(vehicle_id=999_999, rule_type="low_soc"))
            db_session.flush()