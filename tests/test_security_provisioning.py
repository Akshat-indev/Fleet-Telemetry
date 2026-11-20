"""A2 provisioning: create-group prints usable credentials exactly once;
list-groups never reveals secrets. Runs the real CLI in a subprocess."""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from app.security import compute_signature

ROOT = Path(__file__).resolve().parents[1]
URL = "/v1/telemetry/batch"


def run_manage(env, *args):
    return subprocess.run(
        [sys.executable, "-m", "app.manage", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_create_group_credentials_work_and_secret_not_listed(
    isolated_client, payload
):
    client, engine = isolated_client
    env = os.environ.copy()
    env["DATABASE_URL"] = engine.url.render_as_string(hide_password=False)

    created = run_manage(env, "create-group", "--name", "prov-test")
    assert created.returncode == 0, created.stderr
    api_key = re.search(r"api_key: (\S+)", created.stdout).group(1)
    secret = re.search(r"secret:\s+(\S+)", created.stdout).group(1)
    assert api_key.startswith("vg-")
    assert len(secret) == 64

    body = json.dumps({"readings": [payload("V-PROV", 1)]}).encode()
    ts = str(int(time.time()))
    headers = {
        "X-API-Key": api_key,
        "X-Timestamp": ts,
        "X-Signature": compute_signature(secret, api_key, ts, body),
    }
    r = client.post(URL, content=body, headers=headers)
    assert r.status_code == 200
    assert r.json()["accepted"] == 1

    listed = run_manage(env, "list-groups")
    assert listed.returncode == 0, listed.stderr
    assert api_key in listed.stdout
    assert "prov-test" in listed.stdout
    assert secret not in listed.stdout
    assert secret not in created.stderr