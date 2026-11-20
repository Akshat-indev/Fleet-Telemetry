import json
from typing import Any

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.alerts.engine import evaluate_reading, resolve_offline
from app.database import SessionLocal
from app.schemas import (
    BatchResponse,
    ReadingResult,
    TelemetryReadingIn,
    VehicleGapSummary,
)

# Listing individual missing seqs scans the whole seq range; only do it for
# bounded ranges. Counts stay exact at any scale via min/max/count aggregates.
_MISSING_LIST_MAX_RANGE = 10_000
_MISSING_LIST_LIMIT = 100

# DO UPDATE (a no-op) instead of DO NOTHING: it waits for a concurrent creator
# and always returns the id. DO NOTHING + SELECT can race an uncommitted insert.
_UPSERT_VEHICLE = text("""
    INSERT INTO vehicles (vehicle_id)
    VALUES (:vehicle_id)
    ON CONFLICT (vehicle_id) DO UPDATE SET vehicle_id = EXCLUDED.vehicle_id
    RETURNING id
""")

_INSERT_READING = text("""
    INSERT INTO telemetry_readings
        (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
         lat, lng, odometer_km, charging, fault_codes)
    VALUES
        (:vehicle_id, :seq, :ts, :soc_pct, :battery_temp_c, :speed_kmh,
         :lat, :lng, :odometer_km, :charging, CAST(:fault_codes AS jsonb))
    ON CONFLICT ON CONSTRAINT uq_telemetry_readings_vehicle_seq DO NOTHING
    RETURNING id
""")

# Atomic compare-and-set: the WHERE clause is re-evaluated under the row lock,
# so a late reading can never overwrite newer state even under concurrency.
_UPDATE_STATE_IF_NEWER = text("""
    UPDATE vehicles
    SET latest_seq = :seq,
        latest_device_ts = :ts,
        soc_pct = :soc_pct,
        battery_temp_c = :battery_temp_c,
        speed_kmh = :speed_kmh,
        lat = :lat,
        lng = :lng,
        odometer_km = :odometer_km,
        charging = :charging,
        updated_at = now()
    WHERE id = :id
      AND (latest_device_ts IS NULL OR latest_device_ts < :ts)
""")

_TOUCH_LAST_SEEN = text("""
    UPDATE vehicles SET last_seen_at = now(), updated_at = now() WHERE id = :id
""")

# max(seq) is an O(1) backward scan of the (vehicle_id, seq) unique index.
_MAX_SEQ = text("""
    SELECT max(seq) FROM telemetry_readings WHERE vehicle_id = :id
""")

_SEQ_STATS = text("""
    SELECT min(seq) AS min_seq, max(seq) AS max_seq, count(*) AS received
    FROM telemetry_readings WHERE vehicle_id = :id
""")

_MISSING_SEQS = text("""
    SELECT s.seq
    FROM generate_series(CAST(:min_seq AS bigint), CAST(:max_seq AS bigint)) AS s(seq)
    WHERE NOT EXISTS (
        SELECT 1 FROM telemetry_readings t
        WHERE t.vehicle_id = :id AND t.seq = s.seq
    )
    ORDER BY s.seq
    LIMIT :limit
""")


def _reading_params(db_id: int, reading: TelemetryReadingIn) -> dict:
    return {
        "vehicle_id": db_id,
        "seq": reading.seq,
        "ts": reading.ts,
        "soc_pct": reading.soc_pct,
        "battery_temp_c": reading.battery_temp_c,
        "speed_kmh": reading.speed_kmh,
        "lat": reading.lat,
        "lng": reading.lng,
        "odometer_km": reading.odometer_km,
        "charging": reading.charging,
        "fault_codes": json.dumps(reading.fault_codes),
    }


def _validation_errors(exc: ValidationError) -> list[dict[str, str]]:
    return [
        {
            "field": ".".join(str(part) for part in err["loc"]) or "reading",
            "message": err["msg"],
        }
        for err in exc.errors(include_url=False, include_context=False, include_input=False)
    ]


def _process_reading(
    session: Session, reading: TelemetryReadingIn, touched: dict
) -> str:
    db_id = session.execute(
        _UPSERT_VEHICLE, {"vehicle_id": reading.vehicle_id}
    ).scalar_one()

    info = touched.get(reading.vehicle_id)
    if info is None:
        info = {
            "db_id": db_id,
            "batch_start_max": session.execute(_MAX_SEQ, {"id": db_id}).scalar_one(),
            "filled": [],
        }
        touched[reading.vehicle_id] = info

    params = _reading_params(db_id, reading)
    inserted = session.execute(_INSERT_READING, params).first() is not None

    if inserted:
        session.execute(_UPDATE_STATE_IF_NEWER, {**params, "id": db_id})
        prior_max = info["batch_start_max"]
        # An inserted seq at or below the pre-batch max was a hole, now filled.
        if prior_max is not None and reading.seq <= prior_max:
            info["filled"].append(reading.seq)

    # Liveness uses server time for every valid reading, including duplicates
    # and late arrivals, and never touches the device-time current state.
    session.execute(_TOUCH_LAST_SEEN, {"id": db_id})
    resolve_offline(session, db_id)
    # Duplicates are never re-evaluated; only a genuinely new reading can
    # change rule state. Runs in this transaction, under the vehicle row lock.
    if inserted:
        evaluate_reading(session, db_id)
    return "accepted" if inserted else "duplicate"


def _gap_summary(session: Session, vehicle_id: str, info: dict) -> VehicleGapSummary:
    stats = session.execute(_SEQ_STATS, {"id": info["db_id"]}).mappings().one()
    min_seq, max_seq, received = stats["min_seq"], stats["max_seq"], stats["received"]
    span = max_seq - min_seq + 1
    missing_count = span - received

    missing_seqs: list[int] = []
    if 0 < missing_count and span <= _MISSING_LIST_MAX_RANGE:
        missing_seqs = [
            row[0]
            for row in session.execute(
                _MISSING_SEQS,
                {"id": info["db_id"], "min_seq": min_seq, "max_seq": max_seq,
                 "limit": _MISSING_LIST_LIMIT},
            )
        ]

    return VehicleGapSummary(
        vehicle_id=vehicle_id,
        min_seq=min_seq,
        max_seq=max_seq,
        received_count=received,
        missing_count=missing_count,
        missing_seqs=missing_seqs,
        missing_seqs_truncated=missing_count > len(missing_seqs),
        gaps_filled=sorted(info["filled"]),
        completeness_pct=round(100.0 * received / span, 2),
    )


def ingest_batch(raw_readings: list[Any]) -> BatchResponse:
    results: list[ReadingResult] = []
    touched: dict[str, dict] = {}
    counts = {"accepted": 0, "duplicate": 0, "rejected": 0}

    with SessionLocal() as session:
        for index, raw in enumerate(raw_readings):
            vehicle_id = raw.get("vehicle_id") if isinstance(raw, dict) else None
            seq = raw.get("seq") if isinstance(raw, dict) else None
            result = ReadingResult(
                index=index,
                vehicle_id=vehicle_id if isinstance(vehicle_id, str) else None,
                seq=seq if isinstance(seq, int) and not isinstance(seq, bool) else None,
                status="rejected",
            )

            if not isinstance(raw, dict):
                result.errors = [
                    {"field": "reading", "message": "reading must be a JSON object"}
                ]
            else:
                try:
                    reading = TelemetryReadingIn.model_validate(raw)
                except ValidationError as exc:
                    result.errors = _validation_errors(exc)
                else:
                    # One transaction per reading: a failure rolls back only
                    # this reading and leaves the session usable.
                    with session.begin():
                        result.status = _process_reading(session, reading, touched)
                    result.errors = None

            counts[result.status] += 1
            results.append(result)

        vehicles = []
        if touched:
            with session.begin():
                vehicles = [
                    _gap_summary(session, vehicle_id, info)
                    for vehicle_id, info in touched.items()
                ]

    return BatchResponse(
        accepted=counts["accepted"],
        duplicates=counts["duplicate"],
        rejected=counts["rejected"],
        results=results,
        vehicles=vehicles,
    )