import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import AwareDatetime, ValidationError
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from app.alerts.lifecycle import (
    AlertNotFound,
    InvalidTransition,
    StaleVersion,
    acknowledge,
)
from app.alerts.worker import offline_watchdog
from app.database import SessionLocal, check_database_ready
from app.ingestion import ingest_batch
from app.models import Alert, Vehicle
from app.models.alert import ALERT_STATUSES
from app.queries import (
    MAX_HISTORY_WINDOW,
    VEHICLE_STATUSES,
    InvalidCursor,
    fleet_summary,
    list_vehicles,
    vehicle_history,
)
from app.schemas import (
    AckRequest,
    AlertOut,
    BatchResponse,
    FleetSummaryResponse,
    HistoryResponse,
    TelemetryBatchIn,
    VehicleListResponse,
)
from app.security import GENERIC_AUTH_ERROR, AuthenticationError, RateLimited, authenticate
from app.stream import stream_events


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(offline_watchdog())
    yield
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


app = FastAPI(title="VoltGrid Fleet Telemetry", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> JSONResponse:
    try:
        check_database_ready()
    except SQLAlchemyError:
        return JSONResponse(status_code=503, content={"status": "database unavailable"})
    return JSONResponse(status_code=200, content={"status": "ready"})


@app.post("/v1/telemetry/batch", response_model=BatchResponse)
async def telemetry_batch(request: Request):
    # Authenticate against the RAW body bytes before any JSON parsing, so
    # the signature covers exactly what the client sent.
    raw = await request.body()

    def _authenticate() -> None:
        with SessionLocal() as session:
            with session.begin():
                authenticate(
                    session,
                    api_key=request.headers.get("X-API-Key"),
                    timestamp=request.headers.get("X-Timestamp"),
                    signature=request.headers.get("X-Signature"),
                    raw_body=raw,
                )

    try:
        # Blocking DB work stays off the event loop so concurrent batches
        # run in parallel and SSE streams keep flowing during ingestion.
        await run_in_threadpool(_authenticate)
    except AuthenticationError:
        return JSONResponse(status_code=401, content={"detail": GENERIC_AUTH_ERROR})
    except RateLimited as exc:
        return JSONResponse(
            status_code=429,
            content={"detail": "rate limit exceeded"},
            headers={"Retry-After": str(exc.retry_after)},
        )
    except OperationalError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})

    try:
        batch = TelemetryBatchIn.model_validate_json(raw)
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid batch envelope")

    try:
        return await run_in_threadpool(ingest_batch, batch.readings)
    except OperationalError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})


def _alert_out(alert: Alert, external_vehicle_id: str) -> AlertOut:
    return AlertOut(
        id=alert.id,
        vehicle_id=external_vehicle_id,
        rule_type=alert.rule_type,
        status=alert.status,
        version=alert.version,
        details=alert.details,
        opened_at=alert.opened_at,
        acknowledged_at=alert.acknowledged_at,
        resolved_at=alert.resolved_at,
    )


@app.get("/v1/alerts", response_model=list[AlertOut])
def list_alerts(
    vehicle_id: str | None = None,
    status: str | None = None,
    rule_type: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
):
    if status is not None and status not in ALERT_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of {', '.join(ALERT_STATUSES)}",
        )
    stmt = (
        select(Alert, Vehicle.vehicle_id)
        .join(Vehicle, Alert.vehicle_id == Vehicle.id)
        .order_by(Alert.opened_at.desc(), Alert.id.desc())
        .limit(limit)
    )
    if vehicle_id is not None:
        stmt = stmt.where(Vehicle.vehicle_id == vehicle_id)
    if status is not None:
        stmt = stmt.where(Alert.status == status)
    if rule_type is not None:
        stmt = stmt.where(Alert.rule_type == rule_type)

    with SessionLocal() as session:
        rows = session.execute(stmt).all()
    return [_alert_out(alert, external_id) for alert, external_id in rows]


@app.post("/v1/alerts/{alert_id}/ack", response_model=AlertOut)
def acknowledge_alert(alert_id: int, body: AckRequest):
    try:
        with SessionLocal() as session:
            with session.begin():
                acknowledge(session, alert_id, body.version)
            row = session.execute(
                select(Alert, Vehicle.vehicle_id)
                .join(Vehicle, Alert.vehicle_id == Vehicle.id)
                .where(Alert.id == alert_id)
            ).one()
    except AlertNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except (InvalidTransition, StaleVersion) as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return _alert_out(*row)


@app.get("/v1/vehicles", response_model=VehicleListResponse)
def get_vehicles(
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    status: str | None = None,
    min_lat: float | None = Query(default=None, ge=-90, le=90),
    min_lng: float | None = Query(default=None, ge=-180, le=180),
    max_lat: float | None = Query(default=None, ge=-90, le=90),
    max_lng: float | None = Query(default=None, ge=-180, le=180),
):
    if status is not None and status not in VEHICLE_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"status must be one of {', '.join(VEHICLE_STATUSES)}",
        )
    bbox_params = (min_lat, min_lng, max_lat, max_lng)
    bbox = None
    if any(p is not None for p in bbox_params):
        if not all(p is not None for p in bbox_params):
            raise HTTPException(
                status_code=422,
                detail="bounding box requires all of min_lat, min_lng, max_lat, max_lng",
            )
        if min_lat > max_lat or min_lng > max_lng:
            raise HTTPException(
                status_code=422, detail="bounding box min must not exceed max"
            )
        bbox = bbox_params

    try:
        with SessionLocal() as session:
            return list_vehicles(
                session, limit=limit, cursor=cursor, status=status, bbox=bbox
            )
    except InvalidCursor as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except OperationalError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})


@app.get(
    "/v1/vehicles/{vehicle_id}/telemetry",
    response_model=HistoryResponse,
    response_model_by_alias=True,
)
def get_vehicle_telemetry(
    vehicle_id: str,
    from_ts: AwareDatetime = Query(alias="from"),
    to_ts: AwareDatetime = Query(alias="to"),
    points: int | None = Query(default=None, ge=4, le=5000),
    cursor: str | None = None,
):
    if from_ts >= to_ts:
        raise HTTPException(status_code=422, detail="'from' must be before 'to'")
    if to_ts - from_ts > MAX_HISTORY_WINDOW:
        raise HTTPException(
            status_code=422,
            detail=f"time window must not exceed {MAX_HISTORY_WINDOW.days} days",
        )
    if points is not None and cursor is not None:
        raise HTTPException(
            status_code=422, detail="cursor applies to raw mode only; omit points"
        )

    try:
        with SessionLocal() as session:
            result = vehicle_history(
                session, vehicle_id, from_ts=from_ts, to_ts=to_ts,
                points=points, cursor=cursor,
            )
    except InvalidCursor as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except OperationalError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})
    if result is None:
        raise HTTPException(status_code=404, detail=f"vehicle {vehicle_id} not found")
    readings = result.pop("readings")
    return HistoryResponse(
        **{**result, "from_ts": result["from"], "to_ts": result["to"]},
        returned_count=len(readings),
        readings=readings,
    )


@app.get("/v1/stream")
async def stream(
    request: Request,
    coalesce: bool = Query(default=True),
):
    generator = await stream_events(
        request, request.headers.get("Last-Event-ID"), coalesce=coalesce
    )
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/v1/fleet/summary", response_model=FleetSummaryResponse)
def get_fleet_summary():
    try:
        with SessionLocal() as session:
            return fleet_summary(session)
    except OperationalError:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})