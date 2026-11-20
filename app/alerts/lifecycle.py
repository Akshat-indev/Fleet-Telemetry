"""Alert lifecycle: acknowledgment with optimistic concurrency."""

from sqlalchemy import text
from sqlalchemy.orm import Session

_ACKNOWLEDGE = text("""
    UPDATE alerts
    SET status = 'acknowledged', acknowledged_at = now(),
        version = version + 1, updated_at = now()
    WHERE id = :id AND version = :version AND status = 'open'
    RETURNING id
""")

_CURRENT = text("SELECT status, version FROM alerts WHERE id = :id")


class AlertNotFound(Exception):
    pass


class InvalidTransition(Exception):
    pass


class StaleVersion(Exception):
    pass


def acknowledge(session: Session, alert_id: int, expected_version: int) -> None:
    """Atomic compare-and-set: the UPDATE only lands if both the version
    and the open status still match, so concurrent callers cannot silently
    overwrite each other."""
    updated = session.execute(
        _ACKNOWLEDGE, {"id": alert_id, "version": expected_version}
    ).first()
    if updated is not None:
        return

    current = session.execute(_CURRENT, {"id": alert_id}).mappings().first()
    if current is None:
        raise AlertNotFound(f"alert {alert_id} does not exist")
    if current["status"] != "open":
        raise InvalidTransition(
            f"alert is {current['status']}; only open alerts can be acknowledged"
        )
    raise StaleVersion(
        f"version mismatch: current version is {current['version']}"
    )