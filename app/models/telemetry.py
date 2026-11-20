from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Numeric,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.vehicle import Vehicle


class TelemetryReading(Base):
    """Append-only history. (vehicle_id, seq) uniqueness makes future
    ingestion idempotent via INSERT ... ON CONFLICT DO NOTHING."""

    __tablename__ = "telemetry_readings"
    __table_args__ = (
        UniqueConstraint("vehicle_id", "seq", name="uq_telemetry_readings_vehicle_seq"),
        CheckConstraint("soc_pct >= 0 AND soc_pct <= 100", name="soc_pct_range"),
        CheckConstraint(
            "battery_temp_c >= -50 AND battery_temp_c <= 150", name="battery_temp_c_range"
        ),
        CheckConstraint("speed_kmh >= 0", name="speed_kmh_nonnegative"),
        CheckConstraint("lat >= -90 AND lat <= 90", name="lat_range"),
        CheckConstraint("lng >= -180 AND lng <= 180", name="lng_range"),
        CheckConstraint("odometer_km >= 0", name="odometer_km_nonnegative"),
        # History retrieval: readings for one vehicle ordered by device time.
        Index("ix_telemetry_readings_vehicle_ts", "vehicle_id", "ts"),
        # Operational queries: arrival-time scans (ingestion lag, replay windows).
        Index("ix_telemetry_readings_received_at", "received_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    vehicle_id: Mapped[int] = mapped_column(
        ForeignKey("vehicles.id", ondelete="RESTRICT"), nullable=False
    )
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    soc_pct: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    battery_temp_c: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    speed_kmh: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    lat: Mapped[Decimal] = mapped_column(Numeric(9, 6), nullable=False)
    lng: Mapped[Decimal] = mapped_column(Numeric(9, 6), nullable=False)
    odometer_km: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    charging: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fault_codes: Mapped[list] = mapped_column(
        JSONB, server_default=text("'[]'::jsonb"), nullable=False
    )

    vehicle: Mapped["Vehicle"] = relationship(back_populates="telemetry_readings")