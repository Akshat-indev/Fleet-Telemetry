"""Fleet event publishing and persistence (A5).

Every event is INSERTed inside the caller's transaction — the same one
that changes vehicle state, opens/resolves an alert, or acknowledges it.
If that transaction rolls back, the event never existed; if it commits,
the event is durable and visible to every API instance. The identity id
is assigned by PostgreSQL, so ids are unique and totally ordered across
instances without any coordination.
"""

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings

EVENT_TELEMETRY_UPDATE = "telemetry_update"
EVENT_STATUS_CHANGE = "vehicle_status_change"
EVENT_ALERT_OPENED = "alert_opened"
EVENT_ALERT_ACKNOWLEDGED = "alert_acknowledged"
EVENT_ALERT_RESOLVED = "alert_resolved"
EVENT_VEHICLE_OFFLINE = "vehicle_offline"
EVENT_VEHICLE_RECOVERED = "vehicle_recovered"

# High-rate events that describe the same evolving state: a client only
# needs the newest one per vehicle. Everything else is critical lifecycle
# information and is always delivered individually, in id order.
REPLACEABLE_EVENTS = frozenset({EVENT_TELEMETRY_UPDATE, EVENT_STATUS_CHANGE})

_INSERT_EVENT = text("""
    INSERT INTO fleet_events (event_type, vehicle_id, payload)
    VALUES (:event_type, :vehicle_id, CAST(:payload AS jsonb))
""")

_FETCH_EVENTS = text("""
    SELECT id, event_type, vehicle_id, ts, payload
    FROM fleet_events
    WHERE id > :cursor
    ORDER BY id
    LIMIT :limit
""")

_EVENT_BOUNDS = text("SELECT min(id) AS min_id, max(id) AS max_id FROM fleet_events")

_EVENT_EXISTS = text("SELECT 1 FROM fleet_events WHERE id = :id")

_CLEANUP_BY_AGE = text("""
    DELETE FROM fleet_events
    WHERE ts < now() - make_interval(secs => :seconds)
""")

_CLEANUP_BY_COUNT = text("""
    DELETE FROM fleet_events
    WHERE id <= (SELECT max(id) - :max_rows FROM fleet_events)
""")


def publish(session: Session, event_type: str, vehicle_id: str, payload: dict) -> None:
    session.execute(
        _INSERT_EVENT,
        {
            "event_type": event_type,
            "vehicle_id": vehicle_id,
            # default=str renders Decimals/datetimes the same way alert
            # details already do; the wire format stays lossless for floats.
            "payload": json.dumps(payload, default=str),
        },
    )


def derive_status(
    last_seen_at: datetime | None,
    charging: bool | None,
    speed_kmh,
    now: datetime,
    offline_after_seconds: int,
) -> str:
    """Python mirror of queries._STATUS_CASE (offline > charging > active > idle)."""
    if last_seen_at is None or (now - last_seen_at).total_seconds() > offline_after_seconds:
        return "offline"
    if charging:
        return "charging"
    if speed_kmh is not None and float(speed_kmh) > 0:
        return "active"
    return "idle"


def event_bounds(session: Session) -> tuple[int | None, int | None]:
    row = session.execute(_EVENT_BOUNDS).mappings().one()
    return row["min_id"], row["max_id"]


def event_exists(session: Session, event_id: int) -> bool:
    return session.execute(_EVENT_EXISTS, {"id": event_id}).first() is not None


def fetch_events(session: Session, cursor: int, limit: int) -> list[dict]:
    return [
        {
            "id": row["id"],
            "type": row["event_type"],
            "ts": row["ts"].isoformat(),
            "vehicle_id": row["vehicle_id"],
            "payload": row["payload"],
        }
        for row in session.execute(
            _FETCH_EVENTS, {"cursor": cursor, "limit": limit}
        ).mappings()
    ]


def cleanup_expired_events(session: Session) -> int:
    settings = get_settings()
    removed = session.execute(
        _CLEANUP_BY_AGE, {"seconds": settings.event_retention_seconds}
    ).rowcount
    removed += session.execute(
        _CLEANUP_BY_COUNT, {"max_rows": settings.event_retention_max_rows}
    ).rowcount
    return removed