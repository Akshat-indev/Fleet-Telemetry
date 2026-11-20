import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError

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
from app.schemas import AckRequest, AlertOut, BatchResponse, TelemetryBatchIn


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
def telemetry_batch(batch: TelemetryBatchIn):
    try:
        return ingest_batch(batch.readings)
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