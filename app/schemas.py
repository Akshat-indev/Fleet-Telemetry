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