"""Alert evaluation, integrated into ingestion's per-reading transaction.

Order-independence: rules are recomputed from the device-time-ordered
history and evaluated at the vehicle's newest reading (the frontier).
Reconcile: trigger at frontier + no active alert -> create; resolve at
frontier + active alert -> resolve; otherwise keep the current state
(hysteresis). Late readings outside the window change nothing, and
duplicates are never evaluated because evaluation only runs on insert.

Concurrency: the caller's transaction already holds the vehicle row lock
(from the ingestion upsert / last_seen update), which serializes all alert
work for one vehicle. The partial unique index additionally guarantees one
active alert per (vehicle, rule) across instances.
"""

import json
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.alerts.rules import discharge_drop, overheat_state, point_in_polygon
from app.config import get_settings

RULE_OVERHEAT = "overheat"
RULE_RAPID_DISCHARGE = "rapid_discharge"
RULE_GEOFENCE = "geofence_breach"
RULE_OFFLINE = "offline"

_FRONTIER = text("""
    SELECT seq, ts, soc_pct, battery_temp_c, charging, lat, lng
    FROM telemetry_readings
    WHERE vehicle_id = :id
    ORDER BY ts DESC, seq DESC
    LIMIT 1
""")

_OVERHEAT_WINDOW = text("""
    SELECT battery_temp_c
    FROM telemetry_readings
    WHERE vehicle_id = :id
    ORDER BY ts DESC, seq DESC
    LIMIT 10
""")

_DISCHARGE_WINDOW = text("""
    SELECT ts, soc_pct, charging
    FROM telemetry_readings
    WHERE vehicle_id = :id AND ts >= :cutoff
    ORDER BY ts DESC, seq DESC
""")

_ACTIVE_ALERTS = text("""
    SELECT id, rule_type FROM alerts
    WHERE vehicle_id = :id AND status <> 'resolved'
""")

_CREATE_ALERT = text("""
    INSERT INTO alerts (vehicle_id, rule_type, details, opened_at)
    VALUES (:id, :rule, CAST(:details AS jsonb), now())
    ON CONFLICT (vehicle_id, rule_type) WHERE status <> 'resolved'
    DO NOTHING
""")

_RESOLVE_ALERT = text("""
    UPDATE alerts
    SET status = 'resolved', resolved_at = now(),
        version = version + 1, updated_at = now()
    WHERE id = :alert_id AND status <> 'resolved'
""")

_RESOLVE_OFFLINE = text("""
    UPDATE alerts
    SET status = 'resolved', resolved_at = now(),
        version = version + 1, updated_at = now()
    WHERE vehicle_id = :id AND rule_type = 'offline' AND status <> 'resolved'
""")


def _reconcile(
    session: Session,
    vehicle_db_id: int,
    active: dict,
    rule: str,
    trigger: bool,
    resolve: bool,
    details: dict,
) -> None:
    alert_id = active.get(rule)
    if alert_id is not None and resolve:
        session.execute(_RESOLVE_ALERT, {"alert_id": alert_id})
    elif alert_id is None and trigger:
        session.execute(
            _CREATE_ALERT,
            {"id": vehicle_db_id, "rule": rule, "details": json.dumps(details)},
        )


def resolve_offline(session: Session, vehicle_db_id: int) -> None:
    """Any valid reading (including duplicates) proves the device is alive."""
    session.execute(_RESOLVE_OFFLINE, {"id": vehicle_db_id})


def evaluate_reading(session: Session, vehicle_db_id: int) -> None:
    settings = get_settings()
    frontier = session.execute(_FRONTIER, {"id": vehicle_db_id}).mappings().first()
    if frontier is None:
        return

    active = {
        row["rule_type"]: row["id"]
        for row in session.execute(_ACTIVE_ALERTS, {"id": vehicle_db_id}).mappings()
    }
    marker = {"seq": frontier["seq"], "ts": frontier["ts"].isoformat()}

    temps = [
        row[0]
        for row in session.execute(_OVERHEAT_WINDOW, {"id": vehicle_db_id})
    ]
    trigger, resolve = overheat_state(temps)
    _reconcile(
        session, vehicle_db_id, active, RULE_OVERHEAT, trigger, resolve,
        {**marker, "battery_temp_c": str(frontier["battery_temp_c"])},
    )

    cutoff = frontier["ts"] - timedelta(seconds=settings.discharge_window_seconds)
    window = session.execute(
        _DISCHARGE_WINDOW, {"id": vehicle_db_id, "cutoff": cutoff}
    ).all()
    drop = discharge_drop(window, settings.discharge_window_seconds)
    _reconcile(
        session, vehicle_db_id, active, RULE_RAPID_DISCHARGE,
        drop > settings.discharge_drop_pct, drop <= settings.discharge_drop_pct,
        {**marker, "drop_pct": str(drop),
         "window_seconds": settings.discharge_window_seconds},
    )

    inside = point_in_polygon(
        float(frontier["lat"]), float(frontier["lng"]), settings.geofence_polygon
    )
    _reconcile(
        session, vehicle_db_id, active, RULE_GEOFENCE, not inside, inside,
        {**marker, "lat": str(frontier["lat"]), "lng": str(frontier["lng"])},
    )