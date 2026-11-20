from datetime import datetime

from sqlalchemy import DateTime, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models import Base


class RequestNonce(Base):
    """Replay protection: the request signature is the unique identifier.

    The signature covers api_key + timestamp + exact body, so an identical
    signature is an identical request. The primary key makes concurrent
    replays race-safe via INSERT ... ON CONFLICT DO NOTHING.
    """

    __tablename__ = "request_nonces"
    __table_args__ = (Index("ix_request_nonces_expires_at", "expires_at"),)

    signature: Mapped[str] = mapped_column(String(64), primary_key=True)
    api_key: Mapped[str] = mapped_column(String(64), nullable=False)
    seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)