import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import Session

from app.alerts.engine import evaluate_reading, resolve_offline
from app.config import get_settings
from app.database import SessionLocal
from app.events import (
    EVENT_STATUS_CHANGE,
    EVENT_TELEMETRY_UPDATE,
    derive_status,
    publish,
)
from app.observability import metrics
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

# Bulk upsert vehicles in canonical sorted order so concurrent batches always
# acquire row locks in the same order (deadlock-free) and return pre-batch
# sequence bounds + status snapshot in a single round-trip.
_BULK_UPSERT_VEHICLES = text("""
    INSERT INTO vehicles (vehicle_id)
    SELECT v FROM unnest(CAST(:vehicle_ids AS varchar[])) AS v
    ON CONFLICT (vehicle_id) DO UPDATE SET vehicle_id = EXCLUDED.vehicle_id
    RETURNING id, vehicle_id, min_seq, max_seq, received_count,
              last_seen_at, charging, speed_kmh
""")

_BULK_INSERT_READINGS = text("""
    INSERT INTO telemetry_readings
        (vehicle_id, seq, ts, soc_pct, battery_temp_c, speed_kmh,
         lat, lng, odometer_km, charging, fault_codes)
    SELECT t.vid, t.seq, t.ts, t.soc, t.temp, t.speed,
           t.lat, t.lng, t.odo, t.chg, CAST(t.fc AS jsonb)
    FROM unnest(
        CAST(:vehicle_ids AS integer[]),
        CAST(:seqs AS bigint[]),
        CAST(:tss AS timestamptz[]),
        CAST(:soc_pcts AS numeric[]),
        CAST(:battery_temp_cs AS numeric[]),
        CAST(:speed_kmhs AS numeric[]),
        CAST(:lats AS numeric[]),
        CAST(:lngs AS numeric[]),
        CAST(:odometer_kms AS numeric[]),
        CAST(:chargings AS boolean[]),
        CAST(:fault_codes AS text[])
    ) AS t(vid, seq, ts, soc, temp, speed, lat, lng, odo, chg, fc)
    ON CONFLICT ON CONSTRAINT uq_telemetry_readings_vehicle_seq DO NOTHING
    RETURNING vehicle_id, seq
""")

# Fallback per-reading insert used only if a DB constraint triggers despite
# Pydantic validation (e.g. defense-in-depth / bypass tests).
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

_BULK_INSERT_TELEMETRY_EVENTS = text("""
    INSERT INTO fleet_events (event_type, vehicle_id, payload)
    SELECT :event_type, t.vid, CAST(t.payload AS jsonb)
    FROM unnest(
        CAST(:vehicle_ids AS varchar[]),
        CAST(:payloads AS text[])
    ) AS t(vid, payload)
""")

# Combined atomic per-vehicle update: compare-and-set current state on highest
# (device_ts, seq), update transactional seq counters, and touch server
# last_seen_at in a single statement.
_UPDATE_VEHICLE_AFTER_INSERT = text("""
    UPDATE vehicles
    SET latest_seq = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :seq ELSE latest_seq END,
        latest_device_ts = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :ts ELSE latest_device_ts END,
        soc_pct = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :soc_pct ELSE soc_pct END,
        battery_temp_c = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :battery_temp_c ELSE battery_temp_c END,
        speed_kmh = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :speed_kmh ELSE speed_kmh END,
        lat = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :lat ELSE lat END,
        lng = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :lng ELSE lng END,
        odometer_km = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :odometer_km ELSE odometer_km END,
        charging = CASE WHEN (
            latest_device_ts IS NULL
            OR latest_device_ts < :ts
            OR (latest_device_ts = :ts AND (latest_seq IS NULL OR latest_seq < :seq))
        ) THEN :charging ELSE charging END,
        received_count = received_count + :new_count,
        min_seq = LEAST(COALESCE(min_seq, :batch_min_seq), :batch_min_seq),
        max_seq = GREATEST(COALESCE(max_seq, :batch_max_seq), :batch_max_seq),
        last_seen_at = now(),
        updated_at = now()
    WHERE id = :id
    RETURNING (latest_seq = :seq AND latest_device_ts = :ts) AS state_updated,
              min_seq, max_seq, received_count
""")

_TOUCH_LAST_SEEN = text("""
    UPDATE vehicles
    SET last_seen_at = now(), updated_at = now()
    WHERE id = :id
    RETURNING min_seq, max_seq, received_count
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


def _telemetry_event_payload(reading: TelemetryReadingIn) -> str:
    return json.dumps(
        {
            "seq": reading.seq,
            "ts": reading.ts.isoformat(),
            "soc_pct": float(reading.soc_pct),
            "battery_temp_c": float(reading.battery_temp_c),
            "speed_kmh": float(reading.speed_kmh),
            "lat": float(reading.lat),
            "lng": float(reading.lng),
            "odometer_km": float(reading.odometer_km),
            "charging": reading.charging,
            "fault_codes": reading.fault_codes,
        },
        separators=(",", ":"),
    )


def _validation_errors(exc: ValidationError) -> list[dict[str, str]]:
    return [
        {
            "field": ".".join(str(part) for part in err["loc"]) or "reading",
            "message": err["msg"],
        }
        for err in exc.errors(include_url=False, include_context=False, include_input=False)
    ]


def _db_rejection_errors(exc: IntegrityError | DataError) -> list[dict[str, str]]:
    diag = getattr(exc.orig, "diag", None)
    constraint = getattr(diag, "constraint_name", None)
    if constraint:
        message = f"violated database constraint {constraint}"
    else:
        message = "invalid telemetry value rejected by database"
    return [{"field": "reading", "message": message}]


def _gap_summary(
    session: Session,
    vehicle_id: str,
    db_id: int,
    min_seq: int,
    max_seq: int,
    received: int,
    filled: list[int],
) -> VehicleGapSummary:
    span = max_seq - min_seq + 1
    missing_count = span - received

    missing_seqs: list[int] = []
    if 0 < missing_count and span <= _MISSING_LIST_MAX_RANGE:
        missing_seqs = [
            row[0]
            for row in session.execute(
                _MISSING_SEQS,
                {
                    "id": db_id,
                    "min_seq": min_seq,
                    "max_seq": max_seq,
                    "limit": _MISSING_LIST_LIMIT,
                },
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
        gaps_filled=sorted(filled),
        completeness_pct=round(100.0 * received / span, 2),
    )


def ingest_batch(raw_readings: list[Any]) -> BatchResponse:
    results: list[ReadingResult] = []
    valid_items: list[tuple[int, TelemetryReadingIn]] = []
    counts = {"accepted": 0, "duplicate": 0, "rejected": 0}

    # 1. Validate every reading individually in Python; invalid readings never
    #    touch PostgreSQL and never prevent valid readings from succeeding.
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
            counts["rejected"] += 1
        else:
            try:
                reading = TelemetryReadingIn.model_validate(raw)
            except ValidationError as exc:
                result.errors = _validation_errors(exc)
                counts["rejected"] += 1
            else:
                valid_items.append((index, reading))
        results.append(result)

    vehicles: list[VehicleGapSummary] = []

    if valid_items:
        # Canonical sorted vehicle list prevents cross-batch lock-order deadlocks.
        sorted_vids = sorted({r.vehicle_id for _, r in valid_items})
        settings = get_settings()

        with SessionLocal() as session:
            with session.begin():
                # 2. Bulk upsert + lock all touched vehicles in one round-trip.
                veh_rows = session.execute(
                    _BULK_UPSERT_VEHICLES, {"vehicle_ids": sorted_vids}
                ).mappings().all()
                veh_map = {row["vehicle_id"]: dict(row) for row in veh_rows}

                # 3. Bulk insert all valid readings in one UNNEST statement
                #    (savepoint fallback to per-reading inserts only on rare DB
                #    constraint errors).
                inserted_by_vehicle: dict[str, list[TelemetryReadingIn]] = defaultdict(list)
                touched_valid_by_vehicle: dict[str, list[TelemetryReadingIn]] = defaultdict(list)
                inserted_events_vids: list[str] = []
                inserted_events_payloads: list[str] = []

                try:
                    with session.begin_nested():
                        # Deduplicate within the batch before UNNEST so the
                        # first occurrence claims the insert cleanly.
                        seen_in_batch: set[tuple[int, int]] = set()
                        unique_items: list[tuple[int, int, TelemetryReadingIn]] = []
                        for idx, reading in valid_items:
                            db_id = veh_map[reading.vehicle_id]["id"]
                            key = (db_id, reading.seq)
                            if key not in seen_in_batch:
                                seen_in_batch.add(key)
                                unique_items.append((idx, db_id, reading))

                        inserted_keys = set(
                            session.execute(
                                _BULK_INSERT_READINGS,
                                {
                                    "vehicle_ids": [db_id for _, db_id, _ in unique_items],
                                    "seqs": [r.seq for _, _, r in unique_items],
                                    "tss": [r.ts for _, _, r in unique_items],
                                    "soc_pcts": [r.soc_pct for _, _, r in unique_items],
                                    "battery_temp_cs": [r.battery_temp_c for _, _, r in unique_items],
                                    "speed_kmhs": [r.speed_kmh for _, _, r in unique_items],
                                    "lats": [r.lat for _, _, r in unique_items],
                                    "lngs": [r.lng for _, _, r in unique_items],
                                    "odometer_kms": [r.odometer_km for _, _, r in unique_items],
                                    "chargings": [r.charging for _, _, r in unique_items],
                                    "fault_codes": [json.dumps(r.fault_codes) for _, _, r in unique_items],
                                },
                            ).all()
                        )
                except (IntegrityError, DataError):
                    # Defense-in-depth fallback: isolate the offending reading
                    # via per-reading savepoints while keeping valid ones.
                    for idx, reading in valid_items:
                        db_id = veh_map[reading.vehicle_id]["id"]
                        try:
                            with session.begin_nested():
                                inserted = (
                                    session.execute(
                                        _INSERT_READING, _reading_params(db_id, reading)
                                    ).first()
                                    is not None
                                )
                        except (IntegrityError, DataError) as exc:
                            results[idx].status = "rejected"
                            results[idx].errors = _db_rejection_errors(exc)
                            counts["rejected"] += 1
                        else:
                            status = "accepted" if inserted else "duplicate"
                            results[idx].status = status
                            results[idx].errors = None
                            counts[status] += 1
                            touched_valid_by_vehicle[reading.vehicle_id].append(reading)
                            if inserted:
                                inserted_by_vehicle[reading.vehicle_id].append(reading)
                                inserted_events_vids.append(reading.vehicle_id)
                                inserted_events_payloads.append(_telemetry_event_payload(reading))
                else:
                    claimed: set[tuple[int, int]] = set()
                    for idx, reading in valid_items:
                        db_id = veh_map[reading.vehicle_id]["id"]
                        key = (db_id, reading.seq)
                        inserted = key in inserted_keys and key not in claimed
                        if inserted:
                            claimed.add(key)
                        status = "accepted" if inserted else "duplicate"
                        results[idx].status = status
                        results[idx].errors = None
                        counts[status] += 1
                        touched_valid_by_vehicle[reading.vehicle_id].append(reading)
                        if inserted:
                            inserted_by_vehicle[reading.vehicle_id].append(reading)
                            inserted_events_vids.append(reading.vehicle_id)
                            inserted_events_payloads.append(_telemetry_event_payload(reading))

                # 4. Bulk-publish telemetry_update outbox events in one statement.
                if inserted_events_vids:
                    session.execute(
                        _BULK_INSERT_TELEMETRY_EVENTS,
                        {
                            "event_type": EVENT_TELEMETRY_UPDATE,
                            "vehicle_ids": inserted_events_vids,
                            "payloads": inserted_events_payloads,
                        },
                    )

                # 5. Per touched vehicle: one combined state/counter/liveness
                #    UPDATE, offline resolution, frontier alert evaluation, and
                #    gap summary — all inside the same batch transaction.
                now = datetime.now(timezone.utc)
                for vid in sorted_vids:
                    v_all = touched_valid_by_vehicle.get(vid)
                    if not v_all:
                        continue
                    v_info = veh_map[vid]
                    db_id = v_info["id"]
                    v_inserted = inserted_by_vehicle.get(vid)

                    if v_inserted:
                        best = max(v_inserted, key=lambda r: (r.ts, r.seq))
                        batch_min_seq = min(r.seq for r in v_inserted)
                        batch_max_seq = max(r.seq for r in v_inserted)
                        upd = session.execute(
                            _UPDATE_VEHICLE_AFTER_INSERT,
                            {
                                **_reading_params(db_id, best),
                                "id": db_id,
                                "new_count": len(v_inserted),
                                "batch_min_seq": batch_min_seq,
                                "batch_max_seq": batch_max_seq,
                            },
                        ).mappings().one()

                        if upd["state_updated"]:
                            old_status = derive_status(
                                v_info["last_seen_at"],
                                v_info["charging"],
                                v_info["speed_kmh"],
                                now,
                                settings.offline_after_seconds,
                            )
                            new_status = derive_status(
                                now,
                                best.charging,
                                best.speed_kmh,
                                now,
                                settings.offline_after_seconds,
                            )
                            if old_status != new_status:
                                publish(
                                    session,
                                    EVENT_STATUS_CHANGE,
                                    vid,
                                    {
                                        "from": old_status,
                                        "to": new_status,
                                        "seq": best.seq,
                                    },
                                )

                        # Only sequences strictly inside the pre-batch [min, max]
                        # range filled an existing gap; sequences below prior_min
                        # extend the lower bound rather than filling a hole.
                        prior_min = v_info["min_seq"]
                        prior_max = v_info["max_seq"]
                        filled = [
                            r.seq
                            for r in v_inserted
                            if prior_min is not None
                            and prior_max is not None
                            and prior_min < r.seq < prior_max
                        ]
                        min_seq, max_seq, received = (
                            upd["min_seq"],
                            upd["max_seq"],
                            upd["received_count"],
                        )
                    else:
                        upd = session.execute(
                            _TOUCH_LAST_SEEN, {"id": db_id}
                        ).mappings().one()
                        filled = []
                        min_seq, max_seq, received = (
                            upd["min_seq"],
                            upd["max_seq"],
                            upd["received_count"],
                        )

                    last_valid = v_all[-1]
                    resolve_offline(session, db_id, vid, last_valid.seq, last_valid.ts)
                    if v_inserted:
                        evaluate_reading(session, db_id, vid)

                    if min_seq is not None and max_seq is not None:
                        vehicles.append(
                            _gap_summary(
                                session, vid, db_id, min_seq, max_seq, received, filled
                            )
                        )

    metrics.record_ingestion(
        counts["accepted"], counts["duplicate"], counts["rejected"]
    )
    return BatchResponse(
        accepted=counts["accepted"],
        duplicates=counts["duplicate"],
        rejected=counts["rejected"],
        results=results,
        vehicles=vehicles,
    )
