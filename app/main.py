from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from app.database import check_database_ready
from app.ingestion import ingest_batch
from app.schemas import BatchResponse, TelemetryBatchIn

app = FastAPI(title="VoltGrid Fleet Telemetry")


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