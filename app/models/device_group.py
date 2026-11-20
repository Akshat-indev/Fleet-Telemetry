from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models import Base


class DeviceGroup(Base):
    """A fleet operator's device group: one API key + shared HMAC secret.

    The secret must be recoverable verbatim (HMAC verification recomputes
    the signature), so it cannot be password-hashed. It is never returned
    by any endpoint and never logged.
    """

    __tablename__ = "device_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    secret: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )