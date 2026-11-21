"""Fleet simulator for the VoltGrid telemetry service (A5).

Drives virtual vehicles through the REAL ingestion API
(POST /v1/telemetry/batch, HMAC-signed) — never straight into PostgreSQL.
"""