/*
 * k6 mixed-workload load test for VoltGrid Fleet Telemetry API.
 *
 * Exercises concurrent HMAC-signed batch ingestion (including invalid and
 * duplicate readings) alongside operator query endpoints:
 *   - POST /v1/telemetry/batch
 *   - GET  /v1/fleet/summary
 *   - GET  /v1/vehicles
 *   - GET  /v1/vehicles/{id}/history?from=&to=&points=200
 *   - GET  /v1/alerts
 *
 * Usage:
 *   SIM_API_KEY=vg-... SIM_API_SECRET=... k6 run loadtest/k6_load_test.js
 *   # Optional overrides: BASE_URL, VEHICLE_ID, VUS, DURATION
 */

import crypto from "k6/crypto";
import http from "k6/http";
import { check, sleep } from "k6";
import { Trend } from "k6/metrics";

const BASE_URL = (__ENV.BASE_URL || "http://localhost:8000").replace(/\/$/, "");
const API_KEY = __ENV.SIM_API_KEY || "";
const API_SECRET = __ENV.SIM_API_SECRET || "";
const HISTORY_VEHICLE = __ENV.VEHICLE_ID || "LOAD-0001";

const ingestLatency = new Trend("ingest_batch_ms", true);
const summaryLatency = new Trend("fleet_summary_ms", true);
const vehiclesLatency = new Trend("list_vehicles_ms", true);
const historyLatency = new Trend("vehicle_history_ms", true);
const alertsLatency = new Trend("list_alerts_ms", true);

export const options = {
  scenarios: {
    ingest: {
      executor: "constant-vus",
      exec: "ingestTraffic",
      vus: Number(__ENV.INGEST_VUS || 4),
      duration: __ENV.DURATION || "30s",
    },
    queries: {
      executor: "constant-vus",
      exec: "queryTraffic",
      vus: Number(__ENV.QUERY_VUS || 6),
      duration: __ENV.DURATION || "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    fleet_summary_ms: ["p(95)<150"],
    vehicle_history_ms: ["p(95)<250"],
  },
};

function sign(body) {
  const ts = String(Math.floor(Date.now() / 1000));
  const msg = `${API_KEY}\n${ts}\n${body}`;
  const sig = crypto.hmac("sha256", API_SECRET, msg, "hex");
  return {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
    "X-Timestamp": ts,
    "X-Signature": sig,
  };
}

export function ingestTraffic() {
  if (!API_KEY || !API_SECRET) {
    throw new Error("Set SIM_API_KEY and SIM_API_SECRET environment variables");
  }
  const nowMs = Date.now();
  const baseSeq = __VU * 100000 + __ITER * 20 + 1;
  const readings = [];
  for (let i = 0; i < 20; i++) {
    const vid = `K6-${String((__VU * 10 + (i % 10))).padStart(4, "0")}`;
    const valid = i % 5 < 3; // 60% valid / 40% invalid
    readings.push({
      vehicle_id: vid,
      seq: baseSeq + i,
      ts: new Date(nowMs - (20 - i) * 5000).toISOString(),
      soc_pct: valid ? 72.5 - i * 0.2 : 101,
      battery_temp_c: 34.0 + (i % 5),
      speed_kmh: 28.5,
      lat: 13.05,
      lng: 80.25,
      odometer_km: 1500 + i * 0.4,
      charging: false,
      fault_codes: [],
    });
  }
  const body = JSON.stringify({ readings });
  const res = http.post(`${BASE_URL}/v1/telemetry/batch`, body, { headers: sign(body) });
  ingestLatency.add(res.timings.duration);
  check(res, {
    "batch 200 or 429": (r) => r.status === 200 || r.status === 429,
    "batch partial success": (r) => {
      if (r.status !== 200) return true;
      const j = r.json();
      return j.accepted >= 0 && j.rejected === 8;
    },
  });
  sleep(0.15);
}

export function queryTraffic() {
  const rSum = http.get(`${BASE_URL}/v1/fleet/summary`);
  summaryLatency.add(rSum.timings.duration);
  check(rSum, { "summary 200": (r) => r.status === 200 });

  const rVeh = http.get(`${BASE_URL}/v1/vehicles?limit=50&status=active`);
  vehiclesLatency.add(rVeh.timings.duration);
  check(rVeh, { "vehicles 200": (r) => r.status === 200 });

  const toTs = new Date().toISOString();
  const fromTs = new Date(Date.now() - 24 * 3600 * 1000).toISOString();
  const rHist = http.get(
    `${BASE_URL}/v1/vehicles/${HISTORY_VEHICLE}/history?from=${encodeURIComponent(fromTs)}&to=${encodeURIComponent(toTs)}&points=200`
  );
  historyLatency.add(rHist.timings.duration);
  check(rHist, { "history 200 or 404": (r) => r.status === 200 || r.status === 404 });

  const rAl = http.get(`${BASE_URL}/v1/alerts?status=open&limit=50`);
  alertsLatency.add(rAl.timings.duration);
  check(rAl, { "alerts 200": (r) => r.status === 200 });

  sleep(0.05);
}
