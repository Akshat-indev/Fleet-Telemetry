from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, Field


class TelemetryReadingIn(BaseModel):
    vehicle_id: str = Field(min_length=1, max_length=64)
    seq: int = Field(gt=0, lt=2**63)
    ts: AwareDatetime
    soc_pct: Decimal = Field(ge=0, le=100)
    battery_temp_c: Decimal = Field(ge=-50, le=150)
    speed_kmh: Decimal = Field(ge=0, lt=10_000)
    lat: Decimal = Field(ge=-90, le=90)
    lng: Decimal = Field(ge=-180, le=180)
    odometer_km: Decimal = Field(ge=0, lt=10**10)
    charging: bool
    fault_codes: list[str] = Field(default_factory=list)


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