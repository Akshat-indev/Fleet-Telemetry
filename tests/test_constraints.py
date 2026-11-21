from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import text
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
    "field,value,constraint",
    [
        ("soc_pct", Decimal("101"), "ck_telemetry_readings_soc_pct_range"),
        ("soc_pct", Decimal("-1"), "ck_telemetry_readings_soc_pct_range"),
        ("battery_temp_c", Decimal("151"), "ck_telemetry_readings_battery_temp_c_range"),
        ("battery_temp_c", Decimal("-51"), "ck_telemetry_readings_battery_temp_c_range"),
        ("speed_kmh", Decimal("-5"), "ck_telemetry_readings_speed_kmh_nonnegative"),
        ("lat", Decimal("91"), "ck_telemetry_readings_lat_range"),
        ("lat", Decimal("-91"), "ck_telemetry_readings_lat_range"),
        ("lng", Decimal("181"), "ck_telemetry_readings_lng_range"),
        ("lng", Decimal("-181"), "ck_telemetry_readings_lng_range"),
        ("odometer_km", Decimal("-1"), "ck_telemetry_readings_odometer_km_nonnegative"),
        ("seq", 0, "ck_telemetry_readings_seq_positive"),
        ("seq", -5, "ck_telemetry_readings_seq_positive"),
    ],
)
def test_check_constraints_reject_invalid_values(
    db_session, vehicle, make_reading, field, value, constraint
):
    with pytest.raises(IntegrityError, match=constraint):
        with db_session.begin_nested():
            db_session.add(make_reading(vehicle.id, **{field: value}))
            db_session.flush()


@pytest.mark.parametrize(
    "overrides,constraint",
    [
        ({"soc_pct": Decimal("101")}, "ck_telemetry_readings_soc_pct_range"),
        ({"soc_pct": Decimal("-0.01")}, "ck_telemetry_readings_soc_pct_range"),
        ({"lat": Decimal("90.000001")}, "ck_telemetry_readings_lat_range"),
        ({"lat": Decimal("-90.000001")}, "ck_telemetry_readings_lat_range"),
        ({"lng": Decimal("180.000001")}, "ck_telemetry_readings_lng_range"),
        ({"lng": Decimal("-180.000001")}, "ck_telemetry_readings_lng_range"),
        ({"speed_kmh": Decimal("-0.01")}, "ck_telemetry_readings_speed_kmh_nonnegative"),
        ({"odometer_km": Decimal("-0.01")}, "ck_telemetry_readings_odometer_km_nonnegative"),
        ({"seq": 0}, "ck_telemetry_readings_seq_positive"),
        ({"seq": -10}, "ck_telemetry_readings_seq_positive"),
        ({"battery_temp_c": Decimal("150.01")}, "ck_telemetry_readings_battery_temp_c_range"),
        ({"battery_temp_c": Decimal("-50.01")}, "ck_telemetry_readings_battery_temp_c_range"),
        ({"fault_codes": '"P0A80"'}, "ck_telemetry_readings_fault_codes_array"),
    ],
)
def test_direct_sql_telemetry_insert_violating_constraints_fails(
    db_session, vehicle, overrides, constraint
):
    """Bypass Pydantic and SQLAlchemy ORM completely with raw SQL INSERT."""
    params = {
        "vehicle_id": vehicle.id,
        "seq": 1,
        "ts": datetime.now(timezone.utc),
        "soc_pct": Decimal("50.00"),
        "battery_temp_c": Decimal("30.00"),
        "speed_kmh": Decimal("20.00"),
        "lat": Decimal("13.000000"),
        "lng": Decimal("80.000000"),
        "odometer_km": Decimal("100.00"),
        "charging": False,
        "fault_codes": "[]",
    }
    params.update(overrides)
    sql = text("""
        INSERT INTO telemetry_readings
            (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
             lat, lng, odometer_km, charging, fault_codes)
        VALUES
            (:vehicle_id, :seq, :ts, :soc_pct, :battery_temp_c, :speed_kmh,
             :lat, :lng, :odometer_km, :charging, CAST(:fault_codes AS jsonb))
    """)
    with pytest.raises(IntegrityError, match=constraint):
        with db_session.begin_nested():
            db_session.execute(sql, params)


@pytest.mark.parametrize(
    "column,value,constraint",
    [
        ("soc_pct", Decimal("101"), "ck_vehicles_soc_pct_range"),
        ("soc_pct", Decimal("-1"), "ck_vehicles_soc_pct_range"),
        ("battery_temp_c", Decimal("151"), "ck_vehicles_battery_temp_c_range"),
        ("battery_temp_c", Decimal("-51"), "ck_vehicles_battery_temp_c_range"),
        ("speed_kmh", Decimal("-1"), "ck_vehicles_speed_kmh_nonnegative"),
        ("lat", Decimal("91"), "ck_vehicles_lat_range"),
        ("lng", Decimal("-181"), "ck_vehicles_lng_range"),
        ("odometer_km", Decimal("-1"), "ck_vehicles_odometer_km_nonnegative"),
        ("latest_seq", 0, "ck_vehicles_latest_seq_positive"),
        ("min_seq", 0, "ck_vehicles_min_seq_positive"),
        ("max_seq", -1, "ck_vehicles_max_seq_positive"),
        ("received_count", -1, "ck_vehicles_received_count_nonnegative"),
        ("vehicle_id", "   ", "ck_vehicles_vehicle_id_nonblank"),
    ],
)
def test_direct_sql_vehicle_state_update_violating_constraints_fails(
    db_session, vehicle, column, value, constraint
):
    """Current vehicle state table independently rejects invalid values at DB level."""
    sql = text(f"UPDATE vehicles SET {column} = :val WHERE id = :id")
    with pytest.raises(IntegrityError, match=constraint):
        with db_session.begin_nested():
            db_session.execute(sql, {"val": value, "id": vehicle.id})


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
