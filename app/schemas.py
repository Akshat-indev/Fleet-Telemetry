import math
import re
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
)

# Explicit, documented validation bounds shared with PostgreSQL constraints.
# - soc_pct: percentage of full battery capacity, strictly [0, 100].
# - battery_temp_c: EV pack thermistor sensor envelope [-50, 150] °C.
#   Accepts arctic cold (-25 °C) and abnormal overheating above the 55 °C
#   alert threshold (e.g. 60-120 °C) so the alert engine detects overheating;
#   rejects physically impossible sensor faults outside [-50, 150] °C.
# - lat / lng: WGS-84 geographic bounds [-90, 90] and [-180, 180].
# - speed_kmh / odometer_km: non-negative physical quantities capped at the
#   PostgreSQL Numeric(6, 2) and Numeric(12, 2) column ceilings so no
#   schema-valid value can cause a numeric overflow in the database.
# - seq: positive signed 64-bit integer matching PostgreSQL BIGINT.
SOC_MIN = Decimal("0")
SOC_MAX = Decimal("100")
BATTERY_TEMP_MIN_C = Decimal("-50")
BATTERY_TEMP_MAX_C = Decimal("150")
SPEED_MIN_KMH = Decimal("0")
SPEED_MAX_KMH = Decimal("9999.99")
LAT_MIN = Decimal("-90")
LAT_MAX = Decimal("90")
LNG_MIN = Decimal("-180")
LNG_MAX = Decimal("180")
ODOMETER_MIN_KM = Decimal("0")
ODOMETER_MAX_KM = Decimal("9999999999.99")
SEQ_MIN = 1
SEQ_MAX = 2**63 - 1

_ISO_DATETIME_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}")


def _strict_finite_decimal(value: Any) -> Decimal:
    """Accept only real, finite numeric values (int, float, Decimal).

    Rejects booleans (which subclass int in Python), numeric strings, None,
    NaN, and +/-Infinity so malformed types never bypass numeric validation.
    """
    if isinstance(value, bool):
        raise ValueError("must be a finite number, not a boolean")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("must be a finite number")
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("must be a finite number")
        return Decimal(str(value))
    raise ValueError("must be a finite number")


def _strict_iso_timestamp(value: Any) -> Any:
    """Require an ISO-8601 timestamp string (or datetime) — reject epoch ints/floats/bools."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        if not _ISO_DATETIME_PREFIX.match(value):
            raise ValueError("ts must be a timezone-aware ISO-8601 timestamp string")
        return value
    raise ValueError("ts must be a timezone-aware ISO-8601 timestamp string")


StrictFiniteDecimal = Annotated[Decimal, BeforeValidator(_strict_finite_decimal)]
StrictAwareDatetime = Annotated[AwareDatetime, BeforeValidator(_strict_iso_timestamp)]


class TelemetryReadingIn(BaseModel):
    vehicle_id: StrictStr = Field(min_length=1, max_length=64)
    seq: StrictInt = Field(ge=SEQ_MIN, le=SEQ_MAX)
    ts: StrictAwareDatetime
    soc_pct: StrictFiniteDecimal = Field(ge=SOC_MIN, le=SOC_MAX)
    battery_temp_c: StrictFiniteDecimal = Field(
        ge=BATTERY_TEMP_MIN_C, le=BATTERY_TEMP_MAX_C
    )
    speed_kmh: StrictFiniteDecimal = Field(ge=SPEED_MIN_KMH, le=SPEED_MAX_KMH)
    lat: StrictFiniteDecimal = Field(ge=LAT_MIN, le=LAT_MAX)
    lng: StrictFiniteDecimal = Field(ge=LNG_MIN, le=LNG_MAX)
    odometer_km: StrictFiniteDecimal = Field(ge=ODOMETER_MIN_KM, le=ODOMETER_MAX_KM)
    charging: StrictBool
    fault_codes: list[StrictStr] = Field(default_factory=list)

    @field_validator("vehicle_id")
    @classmethod
    def _vehicle_id_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("vehicle_id must not be blank")
        return v

    @field_validator("fault_codes")
    @classmethod
    def _fault_codes_valid(cls, codes: list[str]) -> list[str]:
        for code in codes:
            if not code.strip():
                raise ValueError("fault_codes entries must not be blank")
            if len(code) > 64:
                raise ValueError("fault_codes entries must be at most 64 characters")
        return codes


class TelemetryBatchIn(BaseModel):
    # Items stay untyped so one malformed reading never rejects the batch;
    # each item is validated individually in the ingestion service.
    readings: list[Any] = Field(min_length=1, max_length=500)


class ReadingResult(BaseModel):
    index: int
    vehicle_id: str | None = None
    seq: int | None = None
    status: Literal["accepted", "duplicate", "rejected"]
    errors: list[dict[str, str]] | None = None


class VehicleGapSummary(BaseModel):
    vehicle_id: str
    min_seq: int
    max_seq: int
    received_count: int
    missing_count: int
    missing_seqs: list[int]
    missing_seqs_truncated: bool
    gaps_filled: list[int]
    completeness_pct: float


class BatchResponse(BaseModel):
    accepted: int
    duplicates: int
    rejected: int
    results: list[ReadingResult]
    vehicles: list[VehicleGapSummary]


class AlertOut(BaseModel):
    id: int
    vehicle_id: str
    rule_type: str
    status: Literal["open", "acknowledged", "resolved"]
    version: int
    details: dict | None = None
    opened_at: datetime
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None


class AckRequest(BaseModel):
    version: int = Field(gt=0)


class VehicleOut(BaseModel):
    vehicle_id: str
    status: Literal["offline", "charging", "active", "idle"]
    latest_seq: int | None = None
    latest_device_ts: datetime | None = None
    soc_pct: float | None = None
    battery_temp_c: float | None = None
    speed_kmh: float | None = None
    lat: float | None = None
    lng: float | None = None
    odometer_km: float | None = None
    charging: bool | None = None
    last_seen_at: datetime | None = None
    min_seq: int | None = None
    max_seq: int | None = None
    received_count: int


class VehicleListResponse(BaseModel):
    vehicles: list[VehicleOut]
    limit: int
    has_more: bool
    next_cursor: str | None = None


class TelemetryPointOut(BaseModel):
    seq: int
    ts: datetime
    soc_pct: float
    battery_temp_c: float
    speed_kmh: float
    lat: float
    lng: float
    odometer_km: float
    charging: bool
    fault_codes: list[str]


class HistoryResponse(BaseModel):
    vehicle_id: str
    from_ts: datetime = Field(serialization_alias="from")
    to_ts: datetime = Field(serialization_alias="to")
    raw_count: int
    returned_count: int
    downsampled: bool
    bucket_seconds: float | None = None
    next_cursor: str | None = None
    readings: list[TelemetryPointOut]

    model_config = {"populate_by_name": True}


class FleetSummaryResponse(BaseModel):
    vehicles_total: int
    vehicles_by_status: dict[str, int]
    avg_soc_pct: float | None = None
    open_alerts_by_rule: dict[str, int]
    readings_total: int
    data_completeness_pct: float | None = None