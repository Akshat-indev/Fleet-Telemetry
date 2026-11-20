from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.alert import Alert
    from app.models.telemetry import TelemetryReading


class Vehicle(Base):
    """Current state per vehicle. History lives in telemetry_readings."""

    __tablename__ = "vehicles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    vehicle_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    latest_seq: Mapped[int | None] = mapped_column(BigInteger)
    latest_device_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

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