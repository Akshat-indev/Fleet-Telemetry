from datetime import datetime

from sqlalchemy import DateTime, Float, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models import Base


class RateLimitBucket(Base):
    """Shared token bucket per API key. All state lives in PostgreSQL so
    every API instance refills and consumes from the same row."""

    __tablename__ = "rate_limit_buckets"

    api_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    tokens: Mapped[float] = mapped_column(Float, nullable=False)
    last_refill: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)