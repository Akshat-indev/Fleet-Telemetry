"""Offline detection worker.

A periodic set-based sweep finds vehicles whose server-side last_seen_at
is older than the threshold and opens one offline alert per vehicle.
- pg_try_advisory_xact_lock: with multiple API instances, only one runs the
  sweep per cycle; the lock releases automatically at transaction end.
- FOR UPDATE SKIP LOCKED on vehicles serializes against ingestion's vehicle
  row updates without blocking them: if a reading commits first, last_seen_at
  is fresh and the vehicle is skipped; if the sweep commits first, ingestion
  resolves the just-created alert when it processes the reading.
- The partial unique index makes alert creation idempotent regardless.
"""

import asyncio
import logging

from sqlalchemy import text
from starlette.concurrency import run_in_threadpool

from app.config import get_settings
from app.database import SessionLocal

logger = logging.getLogger("app.alerts.worker")

_ADVISORY_LOCK_KEY = 730_001

_SWEEP = text("""
    INSERT INTO alerts (vehicle_id, rule_type, details, opened_at)
    SELECT v.id, 'offline', jsonb_build_object('last_seen_at', v.last_seen_at), now()
    FROM vehicles v
    WHERE v.last_seen_at < now() - make_interval(secs => :after_seconds)
    FOR UPDATE SKIP LOCKED
    ON CONFLICT (vehicle_id, rule_type) WHERE status <> 'resolved'
    DO NOTHING
""")


def sweep_offline() -> int:
    settings = get_settings()
    with SessionLocal() as session:
        with session.begin():
            locked = session.execute(
                text("SELECT pg_try_advisory_xact_lock(:key)"),
                {"key": _ADVISORY_LOCK_KEY},
            ).scalar_one()
            if not locked:
                return 0
            result = session.execute(
                _SWEEP, {"after_seconds": settings.offline_after_seconds}
            )
            if result.rowcount:
                logger.info("offline sweep opened %s alert(s)", result.rowcount)
            return result.rowcount


async def offline_watchdog() -> None:
    settings = get_settings()
    while True:
        await asyncio.sleep(settings.offline_check_interval_seconds)
        try:
            await run_in_threadpool(sweep_offline)
        except Exception:
            logger.exception("offline sweep failed")