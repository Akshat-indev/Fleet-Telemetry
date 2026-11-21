from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Integer,
    Numeric,
    String,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.alert import Alert
    from app.models.telemetry import TelemetryReading


class Vehicle(Base):
    """Current state per vehicle. History lives in telemetry_readings."""

    __tablename__ = "vehicles"
    __table_args__ = (
        CheckConstraint("length(btrim(vehicle_id)) > 0", name="vehicle_id_nonblank"),
        CheckConstraint(
            "soc_pct IS NULL OR (soc_pct >= 0 AND soc_pct <= 100)",
            name="soc_pct_range",
        ),
        CheckConstraint(
            "battery_temp_c IS NULL OR (battery_temp_c >= -50 AND battery_temp_c <= 150)",
            name="battery_temp_c_range",
        ),
        CheckConstraint(
            "speed_kmh IS NULL OR speed_kmh >= 0",
            name="speed_kmh_nonnegative",
        ),
        CheckConstraint(
            "lat IS NULL OR (lat >= -90 AND lat <= 90)",
            name="lat_range",
        ),
        CheckConstraint(
            "lng IS NULL OR (lng >= -180 AND lng <= 180)",
            name="lng_range",
        ),
        CheckConstraint(
            "odometer_km IS NULL OR odometer_km >= 0",
            name="odometer_km_nonnegative",
        ),
        CheckConstraint(
            "latest_seq IS NULL OR latest_seq > 0",
            name="latest_seq_positive",
        ),
        CheckConstraint(
            "min_seq IS NULL OR min_seq > 0",
            name="min_seq_positive",
        ),
        CheckConstraint(
            "max_seq IS NULL OR max_seq > 0",
            name="max_seq_positive",
        ),
        CheckConstraint(
            "received_count >= 0",
            name="received_count_nonnegative",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    vehicle_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    latest_seq: Mapped[int | None] = mapped_column(BigInteger)
    latest_device_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Sequence counters maintained transactionally on insert so fleet-wide
    # completeness is an O(vehicles) aggregate, not a scan of all readings.
    min_seq: Mapped[int | None] = mapped_column(BigInteger)
    max_seq: Mapped[int | None] = mapped_column(BigInteger)
    received_count: Mapped[int] = mapped_column(
        BigInteger, server_default=text("0"), nullable=False
    )

    soc_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2))
    battery_temp_c: Mapped[Decimal | None] = mapped_column(Numeric(5, 2))
    speed_kmh: Mapped[Decimal | None] = mapped_column(Numeric(6, 2))
    lat: Mapped[Decimal | None] = mapped_column(Numeric(9, 6))
    lng: Mapped[Decimal | None] = mapped_column(Numeric(9, 6))
    odometer_km: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    charging: Mapped[bool | None] = mapped_column(Boolean)

    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # No delete cascade: removing a vehicle must never erase its history.
    # The FK's ON DELETE RESTRICT blocks deletion while readings/alerts exist.
    telemetry_readings: Mapped[list["TelemetryReading"]] = relationship(
        back_populates="vehicle", passive_deletes=True
    )
    alerts: Mapped[list["Alert"]] = relationship(
        back_populates="vehicle", passive_deletes=True
    )