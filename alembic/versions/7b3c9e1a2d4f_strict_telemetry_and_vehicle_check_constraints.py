"""strict telemetry and vehicle check constraints

Revision ID: 7b3c9e1a2d4f
Revises: 4a5d6df6234c
Create Date: 2026-11-21 09:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "7b3c9e1a2d4f"
down_revision: Union[str, None] = "4a5d6df6234c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TELEMETRY_CHECKS = (
    ("ck_telemetry_readings_seq_positive", "seq > 0"),
    ("ck_telemetry_readings_fault_codes_array", "jsonb_typeof(fault_codes) = 'array'"),
)

_VEHICLE_CHECKS = (
    ("ck_vehicles_vehicle_id_nonblank", "length(btrim(vehicle_id)) > 0"),
    ("ck_vehicles_soc_pct_range", "soc_pct IS NULL OR (soc_pct >= 0 AND soc_pct <= 100)"),
    (
        "ck_vehicles_battery_temp_c_range",
        "battery_temp_c IS NULL OR (battery_temp_c >= -50 AND battery_temp_c <= 150)",
    ),
    ("ck_vehicles_speed_kmh_nonnegative", "speed_kmh IS NULL OR speed_kmh >= 0"),
    ("ck_vehicles_lat_range", "lat IS NULL OR (lat >= -90 AND lat <= 90)"),
    ("ck_vehicles_lng_range", "lng IS NULL OR (lng >= -180 AND lng <= 180)"),
    ("ck_vehicles_odometer_km_nonnegative", "odometer_km IS NULL OR odometer_km >= 0"),
    ("ck_vehicles_latest_seq_positive", "latest_seq IS NULL OR latest_seq > 0"),
    ("ck_vehicles_min_seq_positive", "min_seq IS NULL OR min_seq > 0"),
    ("ck_vehicles_max_seq_positive", "max_seq IS NULL OR max_seq > 0"),
    ("ck_vehicles_received_count_nonnegative", "received_count >= 0"),
)


def upgrade() -> None:
    for name, condition in _TELEMETRY_CHECKS:
        op.create_check_constraint(name, "telemetry_readings", condition)
    for name, condition in _VEHICLE_CHECKS:
        op.create_check_constraint(name, "vehicles", condition)


def downgrade() -> None:
    for name, _ in reversed(_VEHICLE_CHECKS):
        op.drop_constraint(name, "vehicles", type_="check")
    for name, _ in reversed(_TELEMETRY_CHECKS):
        op.drop_constraint(name, "telemetry_readings", type_="check")
