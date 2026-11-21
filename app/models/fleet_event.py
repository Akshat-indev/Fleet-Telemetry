from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Identity, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models import Base


class FleetEvent(Base):
    """Persisted fleet event stream (A5).

    The identity column gives every event a unique, totally ordered id
    across all API instances — it is the SSE event id and the resumption
    cursor. Rows are written in the same transaction as the state change
    they describe (transactional outbox), so no event can exist without
    its cause, and any instance can serve any client's replay.
    """

    __tablename__ = "fleet_events"
    __table_args__ = (Index("ix_fleet_events_ts", "ts"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    vehicle_id: Mapped[str] = mapped_column(String(64), nullable=False)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)