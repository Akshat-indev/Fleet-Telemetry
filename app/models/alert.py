from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.vehicle import Vehicle

ALERT_STATUSES = ("open", "acknowledged", "resolved")


class Alert(Base):
    """Alert lifecycle. `version` supports optimistic concurrency for
    future acknowledge/resolve transitions."""

    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint(
            "status IN ('open', 'acknowledged', 'resolved')", name="status_valid"
        ),
        # FK lookups: all alerts for one vehicle.
        Index("ix_alerts_vehicle_id", "vehicle_id"),
        # Dashboard filters by rule type and/or status (leftmost prefix
        # also serves rule-type-only queries).
        Index("ix_alerts_rule_type_status", "rule_type", "status"),
        # Partial index: only open alerts, ordered by age. Stays small as
        # resolved history grows.
        Index(
            "ix_alerts_open",
            "opened_at",
            postgresql_where=text("status = 'open'"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    vehicle_id: Mapped[int] = mapped_column(
        ForeignKey("vehicles.id", ondelete="RESTRICT"), nullable=False
    )
    rule_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), server_default=text("'open'"), nullable=False
    )
    version: Mapped[int] = mapped_column(
        Integer, server_default=text("1"), nullable=False
    )
    details: Mapped[dict | None] = mapped_column(JSONB)

    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    vehicle: Mapped["Vehicle"] = relationship(back_populates="alerts")