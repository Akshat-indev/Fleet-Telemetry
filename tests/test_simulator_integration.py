"""Simulator integration tests: the real `python -m simulator` process
against real uvicorn instances over a scratch PostgreSQL database.
No mocks — traffic goes through signed HTTP ingestion, and outcomes are
verified in the database, the SSE log, and the measured terminal report.
"""

import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import text

from tests.sse_harness import (
    collect,
    free_port,
    open_stream,
    start_instances,
    stop_instances,
)

ROOT = Path(__file__).resolve().parents[1]


def parse_report(stdout: str) -> dict:
    report = {}
    for line in stdout.splitlines():
        match = re.match(r"^\s*(.+?)\s*:\s*(.+?)\s*$", line)
        if match:
            report[match.group(1)] = match.group(2)
    return report


def num(report: dict, key: str) -> float:
    return float(report[key].split()[0])


def sim_command(base, *extra):
    return [sys.executable, "-m", "simulator", "--api-base", base, *extra]


def sim_env(api_key, secret):
    env = os.environ.copy()
    env["SIM_API_KEY"] = api_key
    env["SIM_API_SECRET"] = secret
    return env


def run_simulator(base, api_key, secret, *extra, timeout=120):
    proc = subprocess.run(
        sim_command(base, *extra),
        cwd=ROOT,
        env=sim_env(api_key, secret),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return parse_report(proc.stdout)


def count_rows(engine, sql, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def test_end_to_end_pipeline_with_faults(stream_server):
    """Simulator -> signed HTTP -> ingestion -> alerts -> SSE replay.
    Every generated reading is accounted for server-side (no simulated
    network drops here), and abnormal profiles open real alerts."""
    env = stream_server
    prefix = "SIME2E"
    report = run_simulator(
        env.base, env.api_key, env.secret,
        "--vehicles", "12", "--duration", "16", "--interval", "1",
        "--batch-size", "20", "--concurrency", "4", "--seed", "7",
        "--vehicle-prefix", prefix,
        "--duplicate-prob", "0.2", "--out-of-order-prob", "0.1",
        "--delayed-prob", "0.1", "--max-delay", "3",
        "--malformed-prob", "0.15", "--network-drop-prob", "0",
        "--abnormal-fraction", "1.0", "--offline-prob", "0",
        "--clock-drift", "10",
    )

    generated = num(report, "Readings generated")
    duplicates = num(report, "Duplicates injected")
    assert generated == 12 * 16
    assert num(report, "Out-of-order injected") > 0
    assert num(report, "Delayed injected") > 0
    assert num(report, "Malformed injected") > 0
    assert num(report, "Successful (HTTP 200)") > 0
    assert num(report, "p95") >= 0  # measured latency is present

    accepted = num(report, "Accepted readings")
    seen_dupes = num(report, "Duplicate readings")
    rejected = num(report, "Rejected readings")
    assert accepted > 0 and seen_dupes > 0 and rejected > 0
    # No drops, no failed requests: every emission got a server verdict.
    assert accepted + seen_dupes + rejected == generated + duplicates
    assert rejected >= num(report, "Malformed injected")

    pattern = prefix + "-%"
    assert count_rows(
        env.engine,
        "SELECT count(*) FROM telemetry_readings t JOIN vehicles v "
        "ON v.id = t.vehicle_id WHERE v.vehicle_id LIKE :pattern",
        pattern=pattern,
    ) == int(accepted)
    assert count_rows(
        env.engine,
        "SELECT count(*) FROM vehicles WHERE vehicle_id LIKE :pattern",
        pattern=pattern,
    ) == 12
    assert count_rows(
        env.engine,
        "SELECT count(*) FROM alerts a JOIN vehicles v ON v.id = a.vehicle_id "
        "WHERE v.vehicle_id LIKE :pattern",
        pattern=pattern,
    ) > 0

    with open_stream(env.base, last_event_id=0) as response:
        events = collect(response, 2.0)
    data_events = [e for e in events if "event" in e]
    assert any(
        e["event"] == "telemetry_update"
        and e["data"]["vehicle_id"].startswith(prefix)
        for e in data_events
    )
    assert any(e["event"] == "alert_opened" for e in data_events)
    ids = [e["id"] for e in data_events if "id" in e]
    assert ids == sorted(ids)


def test_reconnection_after_server_outage(isolated_client):
    """The simulator starts against a dead port, keeps retrying with
    fresh signatures, and succeeds once a real instance comes up on that
    same port — no readings are lost to the outage."""
    client, engine = isolated_client
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    prefix = "SIMRC"
    proc = subprocess.Popen(
        sim_command(
            base,
            "--vehicles", "3", "--duration", "12", "--interval", "1",
            "--batch-size", "10", "--concurrency", "2", "--seed", "3",
            "--vehicle-prefix", prefix,
            "--duplicate-prob", "0", "--out-of-order-prob", "0",
            "--delayed-prob", "0", "--malformed-prob", "0",
            "--network-drop-prob", "0", "--offline-prob", "0",
            "--abnormal-fraction", "0",
            "--retry-max", "12", "--retry-backoff", "0.3",
            "--request-timeout", "2",
        ),
        cwd=ROOT,
        env=sim_env(client.api_key, client.api_secret),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    procs = []
    try:
        time.sleep(3)  # let connection errors accumulate against the dead port
        procs, _ = start_instances(
            engine.url.render_as_string(hide_password=False), 1, ports=[port]
        )
        stdout, stderr = proc.communicate(timeout=90)
    finally:
        if proc.poll() is None:
            proc.kill()
        stop_instances(procs)
    assert proc.returncode == 0, stdout + stderr
    report = parse_report(stdout)

    assert num(report, "Connection errors") > 0
    assert num(report, "Retries") > 0
    assert num(report, "Successful (HTTP 200)") > 0
    assert num(report, "Accepted readings") > 0
    assert count_rows(
        engine,
        "SELECT count(*) FROM telemetry_readings t JOIN vehicles v "
        "ON v.id = t.vehicle_id WHERE v.vehicle_id LIKE :pattern",
        pattern=prefix + "-%",
    ) == int(num(report, "Accepted readings"))


def test_offline_detection_and_recovery(isolated_client):
    """Offline-prone vehicles go silent past the server's offline
    threshold; the watchdog opens offline alerts and the next reading
    resolves them — observed in the database and the event log."""
    client, engine = isolated_client
    procs, bases = start_instances(
        engine.url.render_as_string(hide_password=False),
        1,
        extra_env={
            "OFFLINE_AFTER_SECONDS": "3",
            "OFFLINE_CHECK_INTERVAL_SECONDS": "1",
        },
    )
    try:
        prefix = "SIMOFF"
        report = run_simulator(
            bases[0], client.api_key, client.api_secret,
            "--vehicles", "4", "--duration", "18", "--interval", "1",
            "--batch-size", "10", "--concurrency", "2", "--seed", "11",
            "--vehicle-prefix", prefix,
            "--offline-prob", "1.0", "--offline-trigger-prob", "0.6",
            "--offline-silence", "6", "--abnormal-fraction", "0",
            "--duplicate-prob", "0", "--out-of-order-prob", "0",
            "--delayed-prob", "0", "--malformed-prob", "0",
            "--network-drop-prob", "0",
        )
        assert num(report, "Offline silence windows") > 0

        pattern = prefix + "-%"
        offline_alerts = count_rows(
            engine,
            "SELECT count(*) FROM alerts a JOIN vehicles v ON v.id = a.vehicle_id "
            "WHERE a.rule_type = 'offline' AND v.vehicle_id LIKE :pattern",
            pattern=pattern,
        )
        assert offline_alerts > 0
        assert count_rows(
            engine,
            "SELECT count(*) FROM alerts a JOIN vehicles v ON v.id = a.vehicle_id "
            "WHERE a.rule_type = 'offline' AND a.status = 'resolved' "
            "AND v.vehicle_id LIKE :pattern",
            pattern=pattern,
        ) > 0
        for event_type in ("vehicle_offline", "vehicle_recovered"):
            assert count_rows(
                engine,
                "SELECT count(*) FROM fleet_events WHERE event_type = :t "
                "AND vehicle_id LIKE :pattern",
                t=event_type,
                pattern=pattern,
            ) > 0
    finally:
        stop_instances(procs)


def test_default_60_40_valid_invalid_distribution_end_to_end(stream_server):
    """Default simulator configuration (60% valid / 40% invalid) runs against
    the real API; every invalid reading is rejected and zero invalid readings
    enter PostgreSQL."""
    env = stream_server
    prefix = "SIM6040"
    report = run_simulator(
        env.base, env.api_key, env.secret,
        "--vehicles", "25", "--duration", "12", "--interval", "1",
        "--batch-size", "50", "--concurrency", "4", "--seed", "42",
        "--vehicle-prefix", prefix,
        "--duplicate-prob", "0.05", "--out-of-order-prob", "0.05",
        "--delayed-prob", "0.05", "--max-delay", "2",
        "--network-drop-prob", "0", "--offline-prob", "0",
        "--abnormal-fraction", "0.4",
    )

    total = num(report, "Readings generated")
    valid_gen = num(report, "Valid readings generated")
    invalid_gen = num(report, "Invalid readings generated")
    assert total == 25 * 12
    assert valid_gen + invalid_gen == total

    valid_pct = 100.0 * valid_gen / total
    invalid_pct = 100.0 * invalid_gen / total
    assert 50.0 <= valid_pct <= 70.0
    assert 30.0 <= invalid_pct <= 50.0

    assert num(report, "Unexpectedly accepted invalid readings") == 0
    assert num(report, "Unexpectedly rejected valid readings") == 0
    assert report["Validation status"] == "PASS"

    valid_accepted = num(report, "Valid readings accepted")
    invalid_rejected = num(report, "Invalid readings rejected")
    assert valid_accepted == valid_gen
    assert invalid_rejected >= invalid_gen

    pattern = prefix + "-%"
    assert count_rows(
        env.engine,
        "SELECT count(*) FROM telemetry_readings t JOIN vehicles v "
        "ON v.id = t.vehicle_id WHERE v.vehicle_id LIKE :pattern",
        pattern=pattern,
    ) == int(valid_accepted)
    # Confirm no out-of-range SOC ever entered telemetry_readings or vehicles.
    assert count_rows(
        env.engine,
        "SELECT count(*) FROM telemetry_readings t JOIN vehicles v "
        "ON v.id = t.vehicle_id WHERE v.vehicle_id LIKE :pattern "
        "AND (t.soc_pct < 0 OR t.soc_pct > 100)",
        pattern=pattern,
    ) == 0