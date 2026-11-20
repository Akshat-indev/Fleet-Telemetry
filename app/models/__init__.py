from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


from app.models.vehicle import Vehicle
from app.models.telemetry import TelemetryReading
from app.models.alert import Alert
from app.models.device_group import DeviceGroup
from app.models.request_nonce import RequestNonce
from app.models.rate_limit_bucket import RateLimitBucket

__all__ = [
    "Base", "Vehicle", "TelemetryReading", "Alert",
    "DeviceGroup", "RequestNonce", "RateLimitBucket",
]