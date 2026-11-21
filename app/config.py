from decimal import Decimal
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str

    offline_after_seconds: int = 90
    offline_check_interval_seconds: int = 15
    discharge_drop_pct: Decimal = Decimal("8")
    discharge_window_seconds: int = 300
    # Operating polygon as [lat, lng] vertices; override via GEOFENCE_POLYGON JSON.
    geofence_polygon: list[list[float]] = [
        [12.8, 79.9],
        [13.3, 79.9],
        [13.3, 80.5],
        [12.8, 80.5],
    ]

    auth_timestamp_window_seconds: int = 300
    rate_limit_capacity: int = 120
    rate_limit_refill_per_second: float = 20.0

    sse_poll_interval_seconds: float = 0.5
    sse_coalesce_window_seconds: float = 1.0
    sse_batch_limit: int = 500
    sse_heartbeat_seconds: float = 15.0
    sse_gap_grace_seconds: float = 30.0
    event_retention_seconds: int = 900
    event_retention_max_rows: int = 200_000

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()