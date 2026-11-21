"""Resumable SSE streaming over the persisted fleet event log (A5).

Each connection keeps a cursor (the last event id delivered) and polls
fleet_events for newer rows. Because the log lives in PostgreSQL:
- any instance can serve any client, and a client may reconnect to a
  different instance with Last-Event-ID and continue exactly where it
  stopped;
- an API restart loses nothing — the cursor is meaningless without the
  log, and the log survives.

Backpressure: the only per-connection memory is the coalescer's pending
map, bounded by 2 entries per vehicle (one telemetry_update and one
vehicle_status_change). A slow client simply stops the loop at a socket
write; it can never block ingestion (readers only) or grow unbounded.
If retention deletes events a lagging client has not seen, the loss is
reported explicitly as a stream_gap control event — never silent.

Concurrent ingestion transactions reserve event ids in order but may
commit out of order, so a poll can briefly see id N+1 while N is still
in flight. The cursor therefore advances only through contiguous ids;
a hole near the top of the log is held for a grace period before being
declared unavailable, while ids below the retained range are reported
as retention gaps immediately.
"""

import asyncio
import json
import time

from fastapi import HTTPException, Request
from starlette.concurrency import run_in_threadpool

from app.config import get_settings
from app.database import SessionLocal
from app.events import (
    REPLACEABLE_EVENTS,
    event_bounds,
    event_exists,
    fetch_events,
)
from app.observability import metrics

class Coalescer:
    """Bounded per-connection buffer.

    Replaceable events (telemetry_update, vehicle_status_change) collapse
    to the newest one per (vehicle, event type) within the flush window.
    Critical events (alert lifecycle, offline/recovery) are never merged
    or dropped; they flush any pending replaceable events first so the
    wire order always matches id order.
    """

    def __init__(self, window_seconds: float):
        self._window = window_seconds
        self._pending: dict[tuple[str, str], dict] = {}
        self._first_add: float | None = None

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def add(self, event: dict) -> list[dict]:
        if event["type"] in REPLACEABLE_EVENTS:
            if self._window <= 0:
                return [event]
            if self._first_add is None:
                self._first_add = time.monotonic()
            self._pending[(event["vehicle_id"], event["type"])] = event
            return []
        out = self.flush()
        out.append(event)
        return out

    def due(self) -> bool:
        return (
            self._first_add is not None
            and time.monotonic() - self._first_add >= self._window
        )

    def flush(self) -> list[dict]:
        out = sorted(self._pending.values(), key=lambda e: e["id"])
        self._pending.clear()
        self._first_add = None
        return out


def _format_event(event: dict) -> str:
    return (
        f"id: {event['id']}\n"
        f"event: {event['type']}\n"
        f"data: {json.dumps(event, separators=(',', ':'))}\n\n"
    )


def _format_control(name: str, payload: dict) -> str:
    return f"event: {name}\ndata: {json.dumps(payload)}\n\n"


def _resolve_start(last_event_id: str | None) -> tuple[int, dict | None]:
    """Turn Last-Event-ID into a cursor plus an optional stream_reset notice.

    No header -> live tail. Valid retained id -> resume right after it.
    Id 0 -> replay everything still retained. Unknown (ahead of the log)
    or expired (deleted by retention) ids are handled explicitly, never
    by silently guessing a position.
    """
    with SessionLocal() as session:
        min_id, max_id = event_bounds(session)
        top = max_id or 0
        if last_event_id is None:
            return top, None
        try:
            lid = int(last_event_id)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Last-Event-ID must be an integer event id"
            )
        if lid < 0:
            raise HTTPException(
                status_code=400, detail="Last-Event-ID must not be negative"
            )
        if lid == 0:
            # Replay starts at the oldest retained id, not at literal 0:
            # retention trims the head of the log, and a cursor below min_id
            # would look like a hole to the gap-grace logic.
            return (min_id - 1 if min_id is not None else 0), None
        if lid > top:
            return top, {
                "reason": "unknown_last_event_id",
                "requested_id": lid,
                "latest_available_id": top,
            }
        if event_exists(session, lid):
            return lid, None
        return (min_id - 1 if min_id is not None else top), {
            "reason": "expired_last_event_id",
            "requested_id": lid,
            "oldest_available_id": min_id,
        }


def _poll(cursor: int, limit: int) -> tuple[list[dict], int | None, int | None]:
    with SessionLocal() as session:
        rows = fetch_events(session, cursor, limit)
        min_id, max_id = event_bounds(session)
        return rows, min_id, max_id


async def stream_events(
    request: Request,
    last_event_id: str | None,
    *,
    coalesce: bool = True,
):
    settings = get_settings()
    cursor, notice = await run_in_threadpool(_resolve_start, last_event_id)

    async def generate():
        nonlocal cursor
        metrics.sse_connected()
        try:
            coalescer = Coalescer(
                settings.sse_coalesce_window_seconds if coalesce else 0.0
            )
            if notice is not None:
                yield _format_control("stream_reset", notice)
            last_heartbeat = time.monotonic()
            hole_cursor: int | None = None
            hole_since = 0.0
            while True:
                if await request.is_disconnected():
                    return
                rows, min_id, _ = await run_in_threadpool(
                    _poll, cursor, settings.sse_batch_limit
                )
                if rows:
                    if rows[0]["id"] > cursor + 1:
                        # The missing id may belong to a transaction that has
                        # not committed yet (ids are reserved in order but can
                        # commit out of order). Hold the cursor at the hole for
                        # a grace period before declaring the ids unavailable.
                        if hole_cursor != cursor:
                            hole_cursor, hole_since = cursor, time.monotonic()
                            rows = []
                        elif (
                            time.monotonic() - hole_since
                            >= settings.sse_gap_grace_seconds
                        ):
                            trimmed = min_id is not None and cursor + 1 < min_id
                            for event in coalescer.flush():
                                yield _format_event(event)
                            yield _format_control("stream_gap", {
                                "reason": (
                                    "retention_expired" if trimmed
                                    else "events_unavailable"
                                ),
                                "last_delivered_id": cursor,
                                "next_available_id": rows[0]["id"],
                            })
                            cursor = rows[0]["id"] - 1
                            hole_cursor = None
                        else:
                            rows = []
                    if rows:
                        expected = cursor + 1
                        for event in rows:
                            if event["id"] != expected:
                                break
                            for out in coalescer.add(event):
                                yield _format_event(out)
                            expected += 1
                        if expected - 1 > cursor:
                            cursor = expected - 1
                            hole_cursor = None
                elif min_id is not None and min_id > cursor + 1:
                    # Everything newer than the cursor was trimmed while the
                    # client lagged; say so instead of skipping silently.
                    yield _format_control("stream_gap", {
                        "reason": "retention_expired",
                        "last_delivered_id": cursor,
                        "next_available_id": min_id,
                    })
                    cursor = min_id - 1
                if coalescer.due():
                    for event in coalescer.flush():
                        yield _format_event(event)
                now = time.monotonic()
                if now - last_heartbeat >= settings.sse_heartbeat_seconds:
                    yield ": keepalive\n\n"
                    last_heartbeat = now
                if not rows:
                    await asyncio.sleep(settings.sse_poll_interval_seconds)
        finally:
            metrics.sse_disconnected()

    return generate()