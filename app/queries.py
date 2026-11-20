"""Read-side query services (A4).

All aggregation happens in PostgreSQL; Python only shapes responses.
The vehicles table is small (one row per vehicle), so listing and summary
touch it alone — never the readings table — except telemetry history,
which is served by ix_telemetry_readings_vehicle_ts.
"""

import base64
import binascii
import json
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.alerts.engine import (
    RULE_GEOFENCE,
    RULE_OFFLINE,
    RULE_OVERHEAT,
    RULE_RAPID_DISCHARGE,
)
from app.config import get_settings

VEHICLE_STATUSES = ("offline", "charging", "active", "idle")
ALERT_RULES = (RULE_OVERHEAT, RULE_RAPID_DISCHARGE, RULE_GEOFENCE, RULE_OFFLINE)

RAW_HISTORY_LIMIT = 10_000
MAX_HISTORY_WINDOW = timedelta(days=31)

# One derivation shared by the listing filter and the summary counts, so
# the two endpoints can never disagree. Precedence: offline > charging >
# active > idle. A vehicle that has never sent a reading is offline.
_STATUS_CASE = """
    CASE
        WHEN last_seen_at IS NULL
             OR last_seen_at < now() - make_interval(secs => :offline_after) THEN 'offline'
        WHEN charging THEN 'charging'
        WHEN speed_kmh > 0 THEN 'active'
        ELSE 'idle'
    END
"""

_VEHICLE_COLUMNS = """
    vehicle_id, latest_seq, latest_device_ts, soc_pct, battery_temp_c,
    speed_kmh, lat, lng, odometer_km, charging, last_seen_at,
    min_seq, max_seq, received_count
"""


class InvalidCursor(ValueError):
    pass


def encode_cursor(payload: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_cursor(cursor: str, required_keys: set[str]) -> dict:
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise InvalidCursor("cursor is not valid base64-encoded JSON")
    if not isinstance(payload, dict) or set(payload) != required_keys:
        raise InvalidCursor("cursor payload has unexpected fields")
    return payload


def list_vehicles(
    session: Session,
    *,
    limit: int,
    cursor: str | None,
    status: str | None,
    bbox: tuple[float, float, float, float] | None,
) -> dict:
    after = None
    if cursor is not None:
        payload = decode_cursor(cursor, {"vehicle_id"})
        after = payload["vehicle_id"]
        if not isinstance(after, str) or not after:
            raise InvalidCursor("cursor vehicle_id must be a non-empty string")

    where = []
    params: dict = {
        "offline_after": get_settings().offline_after_seconds,
        "limit": limit + 1,
    }
    if after is not None:
        where.append("vehicle_id > :after")
        params["after"] = after
    if status is not None:
        where.append(f"({_STATUS_CASE}) = :status")
        params["status"] = status
    if bbox is not None:
        min_lat, min_lng, max_lat, max_lng = bbox
        where.append(
            "lat BETWEEN :min_lat AND :max_lat AND lng BETWEEN :min_lng AND :max_lng"
        )
        params.update(
            min_lat=min_lat, min_lng=min_lng, max_lat=max_lat, max_lng=max_lng
        )

    stmt = f"""
        SELECT {_VEHICLE_COLUMNS}, ({_STATUS_CASE}) AS status
        FROM vehicles
        {"WHERE " + " AND ".join(where) if where else ""}
        ORDER BY vehicle_id
        LIMIT :limit
    """
    rows = session.execute(text(stmt), params).mappings().all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "vehicles": [dict(row) for row in rows],
        "limit": limit,
        "has_more": has_more,
        "next_cursor": encode_cursor({"vehicle_id": rows[-1]["vehicle_id"]})
        if has_more
        else None,
    }


_VEHICLE_DB_ID = text("SELECT id FROM vehicles WHERE vehicle_id = :vehicle_id")

_HISTORY_COUNT = text("""
    SELECT count(*) FROM telemetry_readings
    WHERE vehicle_id = :id AND ts >= :from_ts AND ts <= :to_ts
""")

_HISTORY_COLUMNS = """
    seq, ts, soc_pct, battery_temp_c, speed_kmh, lat, lng,
    odometer_km, charging, fault_codes
"""

_HISTORY_RAW = text(f"""
    SELECT {_HISTORY_COLUMNS} FROM telemetry_readings
    WHERE vehicle_id = :id AND ts >= :from_ts AND ts <= :to_ts
      AND (ts, seq) > (:after_ts, :after_seq)
    ORDER BY ts, seq
    LIMIT :limit
""")

_HISTORY_RAW_FIRST = text(f"""
    SELECT {_HISTORY_COLUMNS} FROM telemetry_readings
    WHERE vehicle_id = :id AND ts >= :from_ts AND ts <= :to_ts
    ORDER BY ts, seq
    LIMIT :limit
""")

# Min/max downsampling: each bucket contributes the actual rows holding the
# lowest and highest soc_pct (spikes and troughs survive verbatim), plus the
# exact first and last rows of the range. UNION deduplicates rows picked by
# more than one branch (e.g. a bucket whose min is also the range's first
# point). buckets = (points - 2) // 2 guarantees at most `points` rows.
_HISTORY_MINMAX = text(f"""
    WITH bucketed AS (
        SELECT {_HISTORY_COLUMNS},
               floor(extract(epoch FROM ts - CAST(:from_ts AS timestamptz))
                     / :bucket_seconds) AS bucket
        FROM telemetry_readings
        WHERE vehicle_id = :id AND ts >= :from_ts AND ts <= :to_ts
    )
    SELECT {_HISTORY_COLUMNS} FROM (
        (SELECT DISTINCT ON (bucket) {_HISTORY_COLUMNS} FROM bucketed
         ORDER BY bucket, soc_pct ASC, ts ASC, seq ASC)
        UNION
        (SELECT DISTINCT ON (bucket) {_HISTORY_COLUMNS} FROM bucketed
         ORDER BY bucket, soc_pct DESC, ts ASC, seq ASC)
        UNION
        (SELECT {_HISTORY_COLUMNS} FROM bucketed ORDER BY ts ASC, seq ASC LIMIT 1)
        UNION
        (SELECT {_HISTORY_COLUMNS} FROM bucketed ORDER BY ts DESC, seq DESC LIMIT 1)
    ) picked
    ORDER BY ts, seq
""")


def vehicle_history(
    session: Session,
    vehicle_id: str,
    *,
    from_ts: datetime,
    to_ts: datetime,
    points: int | None,
    cursor: str | None,
) -> dict | None:
    db_id = session.execute(_VEHICLE_DB_ID, {"vehicle_id": vehicle_id}).scalar_one_or_none()
    if db_id is None:
        return None

    params = {"id": db_id, "from_ts": from_ts, "to_ts": to_ts}
    raw_count = session.execute(_HISTORY_COUNT, params).scalar_one()

    result = {
        "vehicle_id": vehicle_id,
        "from": from_ts,
        "to": to_ts,
        "raw_count": raw_count,
        "downsampled": False,
        "bucket_seconds": None,
        "next_cursor": None,
    }

    if points is not None and raw_count > points:
        buckets = max(1, (points - 2) // 2)
        bucket_seconds = (to_ts - from_ts).total_seconds() / buckets
        rows = session.execute(
            _HISTORY_MINMAX, {**params, "bucket_seconds": bucket_seconds}
        ).mappings().all()
        result.update(
            readings=[dict(row) for row in rows],
            downsampled=True,
            bucket_seconds=bucket_seconds,
        )
        return result

    if cursor is not None:
        payload = decode_cursor(cursor, {"ts", "seq"})
        try:
            after_ts = datetime.fromisoformat(payload["ts"])
            after_seq = payload["seq"]
        except (TypeError, ValueError):
            raise InvalidCursor("cursor ts/seq are not valid")
        if after_ts.tzinfo is None or not isinstance(after_seq, int) or after_seq < 1:
            raise InvalidCursor("cursor ts must be timezone-aware and seq a positive integer")
        rows = session.execute(
            _HISTORY_RAW,
            {**params, "after_ts": after_ts, "after_seq": after_seq,
             "limit": RAW_HISTORY_LIMIT + 1},
        ).mappings().all()
    else:
        rows = session.execute(
            _HISTORY_RAW_FIRST, {**params, "limit": RAW_HISTORY_LIMIT + 1}
        ).mappings().all()

    has_more = len(rows) > RAW_HISTORY_LIMIT
    rows = rows[:RAW_HISTORY_LIMIT]
    result["readings"] = [dict(row) for row in rows]
    if has_more:
        last = rows[-1]
        result["next_cursor"] = encode_cursor(
            {"ts": last["ts"].isoformat(), "seq": last["seq"]}
        )
    return result


# Single scan of the small vehicles table: status counts, average SOC and
# completeness together. Completeness uses the transactionally maintained
# seq counters, so cost is O(vehicles) regardless of readings table size.
_SUMMARY_VEHICLES = text(f"""
    SELECT
        count(*) AS vehicles_total,
        count(*) FILTER (WHERE status = 'offline') AS offline,
        count(*) FILTER (WHERE status = 'charging') AS charging,
        count(*) FILTER (WHERE status = 'active') AS active,
        count(*) FILTER (WHERE status = 'idle') AS idle,
        avg(soc_pct) AS avg_soc_pct,
        COALESCE(sum(received_count), 0) AS readings_total,
        COALESCE(sum(received_count) FILTER (WHERE received_count > 0), 0) AS received,
        COALESCE(sum(max_seq - min_seq + 1) FILTER (WHERE received_count > 0), 0) AS expected
    FROM (SELECT *, ({_STATUS_CASE}) AS status FROM vehicles) v
""")

_SUMMARY_ALERTS = text("""
    SELECT rule_type, count(*) AS n FROM alerts
    WHERE status = 'open'
    GROUP BY rule_type
""")


def fleet_summary(session: Session) -> dict:
    params = {"offline_after": get_settings().offline_after_seconds}
    row = session.execute(_SUMMARY_VEHICLES, params).mappings().one()
    alert_counts = {rule: 0 for rule in ALERT_RULES}
    for alert_row in session.execute(_SUMMARY_ALERTS).mappings():
        alert_counts[alert_row["rule_type"]] = alert_row["n"]

    received, expected = int(row["received"]), int(row["expected"])
    completeness = round(100.0 * received / expected, 2) if expected else None
    avg_soc = round(float(row["avg_soc_pct"]), 2) if row["avg_soc_pct"] is not None else None
    return {
        "vehicles_total": row["vehicles_total"],
        "vehicles_by_status": {
            status: row[status] for status in VEHICLE_STATUSES
        },
        "avg_soc_pct": avg_soc,
        "open_alerts_by_rule": alert_counts,
        "readings_total": int(row["readings_total"]),
        "data_completeness_pct": completeness,
    }