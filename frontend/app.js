/* VoltGrid Ops Console — talks to the Fleet Telemetry API through the
   same-origin nginx proxy (/v1/*). No build step, no dependencies. */
"use strict";

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmtNum = (n) => (n == null ? "—" : Number(n).toLocaleString("en-US"));
const fmt1 = (n) => (n == null ? "—" : Number(n).toFixed(1));

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d) ? "—" : d.toISOString().slice(11, 19);
}
function fmtAgo(iso) {
  if (!iso) return "—";
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

/* ---------------- toasts ---------------- */
function toast(kind, title, msg) {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.innerHTML = `<div><b>${esc(title)}</b><span>${esc(msg)}</span></div>`;
  $("toasts").appendChild(el);
  setTimeout(() => {
    el.classList.add("out");
    setTimeout(() => el.remove(), 260);
  }, 6000);
}

/* ---------------- state ---------------- */
const state = {
  vehicles: new Map(),
  alerts: [],
  summary: null,
  events: [],            // ring buffer, newest last
  seenIds: new Set(),
  totalEvents: 0,
  rateStamps: [],
  feedPaused: false,
  feedFilter: "all",
  vehSearch: "",
  vehStatus: "",
  vehSort: "id",
  vehCap: 120,
  alertStatus: "open",
  alertRule: "",
  drawerId: null,
  creds: null,
  vehiclesDirty: true,
  alertsDirty: true,
  mapDirty: true,
};

const EVENT_META = {
  telemetry_update:      { color: "var(--cyan)",    label: "telemetry" },
  vehicle_status_change: { color: "var(--blue)",    label: "status" },
  alert_opened:          { color: "var(--red)",     label: "alert opened" },
  alert_acknowledged:    { color: "var(--amber)",   label: "alert ack" },
  alert_resolved:        { color: "var(--green)",   label: "alert resolved" },
  vehicle_offline:       { color: "var(--red)",     label: "offline" },
  vehicle_recovered:     { color: "var(--green)",   label: "recovered" },
};
const STATUS_COLORS = { active: "#a8ff3e", idle: "#ffb224", charging: "#3fd2ff", offline: "#ff4d6d" };

/* ---------------- credentials ---------------- */
function loadCreds() {
  try {
    const raw = localStorage.getItem("vg-creds");
    if (raw) {
      const c = JSON.parse(raw);
      if (c.key && c.secret) state.creds = c;
    }
  } catch { /* corrupted storage — ignore */ }
  renderCredState();
}
function renderCredState() {
  const has = !!state.creds;
  $("creds-state").textContent = has ? "Credentials ✓" : "Credentials";
  $("btn-creds").style.color = has ? "var(--green)" : "";
  const note = $("lab-cred-note");
  if (has) {
    note.classList.add("ok");
    note.querySelector("span").textContent = `Signing as ${state.creds.key.slice(0, 12)}…`;
    note.querySelector("svg").innerHTML = '<path d="M9 16.2 4.8 12l-1.4 1.4L9 19 21 7l-1.4-1.4z"/>';
    $("lab-open-creds").textContent = "Change";
  } else {
    note.classList.remove("ok");
    note.querySelector("span").textContent = "Set device-group credentials to sign batches.";
    $("lab-open-creds").textContent = "Add";
  }
}
function openCreds() {
  $("cred-key").value = state.creds?.key ?? "";
  $("cred-secret").value = state.creds?.secret ?? "";
  $("creds-scrim").hidden = false;
  $("cred-key").focus();
}
function closeCreds() { $("creds-scrim").hidden = true; }

/* ---------------- API helper ---------------- */
async function api(path, opts = {}) {
  const res = await fetch(path, opts);
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try { const j = await res.json(); if (j.detail) detail = typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail); } catch { /* non-JSON */ }
    const err = new Error(detail);
    err.status = res.status;
    err.retryAfter = res.headers.get("Retry-After");
    throw err;
  }
  return res.json();
}

/* ---------------- SSE client ---------------- */
const sse = { ctrl: null, lastId: null, resumeId: null, backoff: 1000, timer: null };

function setConn(stateName, label) {
  $("conn").dataset.state = stateName;
  $("conn-label").textContent = label ?? stateName.toUpperCase();
  $("live-pip").classList.toggle("off", stateName !== "live");
  $("stream-status").textContent =
    stateName === "live" ? (state.feedPaused ? "Display paused — still buffering" : "Streaming live") :
    stateName === "reconnecting" ? `Reconnecting in ${(sse.backoff / 1000).toFixed(0)}s…` :
    stateName === "connecting" ? "Connecting…" : stateName;
}

async function connectSSE() {
  clearTimeout(sse.timer);
  if (sse.ctrl) sse.ctrl.abort();
  const ctrl = new AbortController();
  sse.ctrl = ctrl;
  setConn("connecting");

  const headers = {};
  const startId = sse.resumeId !== null ? sse.resumeId : sse.lastId;
  if (startId !== null && startId !== undefined) headers["Last-Event-ID"] = String(startId);
  sse.resumeId = null;

  try {
    const res = await fetch("/v1/stream?coalesce=false", { headers, signal: ctrl.signal });
    if (!res.ok) throw new Error(`stream HTTP ${res.status}`);
    setConn("live");
    sse.backoff = 1000;
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf("\n\n")) >= 0) {
        const chunk = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        handleChunk(chunk);
      }
    }
    throw new Error("stream closed by server");
  } catch (err) {
    if (ctrl.signal.aborted) return;
    setConn("reconnecting");
    sse.timer = setTimeout(connectSSE, sse.backoff);
    sse.backoff = Math.min(sse.backoff * 2, 10000);
  }
}

function handleChunk(chunk) {
  let id = null, type = "message", data = "";
  for (const line of chunk.split("\n")) {
    if (line.startsWith("id:")) id = line.slice(3).trim();
    else if (line.startsWith("event:")) type = line.slice(6).trim();
    else if (line.startsWith("data:")) data += (data ? "\n" : "") + line.slice(5).trim();
  }
  if (!data) return;
  let payload;
  try { payload = JSON.parse(data); } catch { return; }

  if (type === "keepalive") return;
  if (type === "stream_reset" || type === "stream_gap") {
    onControl(type, payload);
    return;
  }
  if (id !== null) sse.lastId = Number(id);
  onEvent(payload);
}

function onControl(type, payload) {
  const reason = payload.reason ?? "";
  if (type === "stream_reset") {
    toast("control", "Stream reset", `${reason}${payload.oldest_available_id != null ? ` · oldest available id ${payload.oldest_available_id}` : ""}`);
  } else {
    toast("control", "Stream gap", `${reason} · last delivered ${payload.last_delivered_id}, next ${payload.next_available_id}`);
  }
  pushEvent({ id: null, type, ts: new Date().toISOString(), vehicle_id: null, payload, control: true });
}

function onEvent(evt) {
  // Manual resume/replay can re-deliver ids already on screen; never
  // double-count them or double-insert their alert cards.
  if (evt.id != null) {
    if (state.seenIds.has(evt.id)) return;
    state.seenIds.add(evt.id);
    if (state.seenIds.size > 5000) state.seenIds.delete(state.seenIds.values().next().value);
  }
  state.totalEvents++;
  state.rateStamps.push(Date.now());
  pushEvent(evt);
  applyEventToState(evt);
  $("last-id").textContent = evt.id ?? "—";
}

function pushEvent(evt) {
  state.events.push(evt);
  if (state.events.length > 600) state.events.splice(0, state.events.length - 600);
  $("stream-buffer").textContent = `buffer ${state.events.length} / 600`;
  $("stream-count").textContent = fmtNum(state.totalEvents);
  if (!state.feedPaused && matchesFilter(evt)) prependFeedRow(evt);
}

/* ---------------- feed ---------------- */
function matchesFilter(evt) {
  const f = state.feedFilter;
  if (f === "all") return true;
  if (f === "control") return !!evt.control;
  if (evt.control) return false;
  if (f === "alert") return evt.type.startsWith("alert_");
  if (f === "presence") return evt.type === "vehicle_offline" || evt.type === "vehicle_recovered";
  return evt.type === f;
}

function eventBody(evt) {
  const p = evt.payload ?? {};
  const vid = evt.vehicle_id ? `<b data-veh="${esc(evt.vehicle_id)}">${esc(evt.vehicle_id)}</b> ` : "";
  switch (evt.type) {
    case "telemetry_update":
      return `${vid}seq ${p.seq} · SoC ${fmt1(p.soc_pct)}% · ${fmt1(p.battery_temp_c)}°C · ${fmt1(p.speed_kmh)} km/h`;
    case "vehicle_status_change":
      return `${vid}${esc(p.from)} → ${esc(p.to)}`;
    case "alert_opened": {
      const det = p.details ? ` · ${alertDetailsText(p.details)}` : "";
      return `${vid}${esc(p.rule_type)} · alert #${p.alert_id}${esc(det)}`;
    }
    case "alert_acknowledged":
      return `${vid}alert #${p.alert_id} · v${p.version}`;
    case "alert_resolved":
      return `${vid}${esc(p.rule_type)} · alert #${p.alert_id} resolved`;
    case "vehicle_offline":
      return `${vid}device went silent`;
    case "vehicle_recovered":
      return `${vid}back online · seq ${p.seq ?? "—"}`;
    case "stream_reset":
    case "stream_gap":
      return `${evt.type} · ${esc(JSON.stringify(p))}`;
    default:
      return `${vid}${esc(JSON.stringify(p))}`;
  }
}

function feedRow(evt) {
  const li = document.createElement("li");
  const meta = EVENT_META[evt.type];
  li.className = "evt" + (evt.control ? " control" : "");
  li.style.setProperty("--evt-color", evt.control ? "var(--magenta)" : meta?.color ?? "var(--dim)");
  li.innerHTML =
    `<span class="e-id">${evt.id ?? "ctl"}</span>` +
    `<span class="e-type">${esc(meta?.label ?? evt.type)}</span>` +
    `<span class="e-body">${eventBody(evt)}</span>` +
    `<span class="e-time">${fmtTime(evt.ts)}</span>`;
  return li;
}

function prependFeedRow(evt) {
  const list = $("stream");
  list.prepend(feedRow(evt));
  while (list.children.length > 250) list.lastChild.remove();
  $("stream-empty").style.display = "none";
}

function rebuildFeed() {
  const list = $("stream");
  list.innerHTML = "";
  const visible = state.events.filter(matchesFilter).slice(-250).reverse();
  for (const evt of visible) list.appendChild(feedRow(evt));
  $("stream-empty").style.display = visible.length ? "none" : "";
}

/* ---------------- live state updates ---------------- */
let summaryRefreshTimer = null;
function scheduleSummaryRefresh() {
  if (summaryRefreshTimer) return;
  summaryRefreshTimer = setTimeout(() => {
    summaryRefreshTimer = null;
    loadSummary();
  }, 300);
}

let lastAlertToastAt = 0;
function toastAlertEvent(kind, title, msg) {
  const now = Date.now();
  if (now - lastAlertToastAt < 1200) return;
  lastAlertToastAt = now;
  toast(kind, title, msg);
}

function applyEventToState(evt) {
  const p = evt.payload ?? {};
  let v = evt.vehicle_id ? state.vehicles.get(evt.vehicle_id) : null;
  switch (evt.type) {
    case "telemetry_update":
      if (v) {
        Object.assign(v, {
          latest_seq: p.seq, latest_device_ts: p.ts, soc_pct: p.soc_pct,
          battery_temp_c: p.battery_temp_c, speed_kmh: p.speed_kmh,
          lat: p.lat, lng: p.lng, odometer_km: p.odometer_km,
          charging: p.charging, last_seen_at: evt.ts,
          max_seq: v.max_seq == null ? p.seq : Math.max(v.max_seq, p.seq),
          received_count: (v.received_count ?? 0) + 1,
        });
        if (v.status === "offline") v.status = p.charging ? "charging" : (p.speed_kmh > 0 ? "active" : "idle");
      } else if (evt.vehicle_id) {
        v = {
          vehicle_id: evt.vehicle_id,
          status: p.charging ? "charging" : (p.speed_kmh > 0 ? "active" : "idle"),
          latest_seq: p.seq, latest_device_ts: p.ts, soc_pct: p.soc_pct,
          battery_temp_c: p.battery_temp_c, speed_kmh: p.speed_kmh,
          lat: p.lat, lng: p.lng, odometer_km: p.odometer_km,
          charging: p.charging, last_seen_at: evt.ts,
          min_seq: p.seq, max_seq: p.seq, received_count: 1,
        };
        state.vehicles.set(evt.vehicle_id, v);
      }
      break;
    case "vehicle_status_change":
      if (v) v.status = p.to;
      break;
    case "vehicle_offline":
      if (v) v.status = "offline";
      scheduleSummaryRefresh();
      break;
    case "vehicle_recovered":
      if (v && v.status === "offline") v.status = v.charging ? "charging" : (v.speed_kmh > 0 ? "active" : "idle");
      scheduleSummaryRefresh();
      break;
    case "alert_opened": {
      if (!state.alerts.some((x) => x.id === p.alert_id)) {
        state.alerts.unshift({
          id: p.alert_id, vehicle_id: evt.vehicle_id, rule_type: p.rule_type,
          status: "open", version: 1, details: p.details ?? null,
          opened_at: evt.ts, acknowledged_at: null, resolved_at: null,
        });
      }
      state.alertsDirty = true;
      if ($("tab-alerts").classList.contains("active")) renderAlerts();
      if (state.drawerId && evt.vehicle_id === state.drawerId) renderDrawerAlerts();
      scheduleSummaryRefresh();
      const det = p.details ? ` · ${alertDetailsText(p.details)}` : "";
      toastAlertEvent("error", `Alert opened: ${p.rule_type}`, `${evt.vehicle_id ?? ""} · alert #${p.alert_id}${det}`);
      break;
    }
    case "alert_acknowledged": {
      const a = state.alerts.find((x) => x.id === p.alert_id);
      if (a) { a.status = "acknowledged"; a.version = p.version; a.acknowledged_at = evt.ts; state.alertsDirty = true; }
      if ($("tab-alerts").classList.contains("active")) renderAlerts();
      if (state.drawerId && evt.vehicle_id === state.drawerId) renderDrawerAlerts();
      scheduleSummaryRefresh();
      break;
    }
    case "alert_resolved": {
      const a = state.alerts.find((x) => x.id === p.alert_id);
      if (a) { a.status = "resolved"; a.resolved_at = evt.ts; state.alertsDirty = true; }
      if ($("tab-alerts").classList.contains("active")) renderAlerts();
      if (state.drawerId && evt.vehicle_id === state.drawerId) renderDrawerAlerts();
      scheduleSummaryRefresh();
      toastAlertEvent("success", `Alert resolved: ${p.rule_type}`, `${evt.vehicle_id ?? ""} · alert #${p.alert_id}`);
      break;
    }
  }
  state.vehiclesDirty = true;
  state.mapDirty = true;
  if (state.drawerId && evt.vehicle_id === state.drawerId) renderDrawerStats();
}

/* ---------------- fleet summary ---------------- */
let summaryErrToastAt = 0;
async function loadSummary() {
  try {
    const s = await api("/v1/fleet/summary");
    state.summary = s;
    const by = s.vehicles_by_status ?? {};
    $("kpi-total").textContent = fmtNum(s.vehicles_total);
    $("kpi-total-foot").textContent = `${fmtNum(s.readings_total)} readings ingested`;
    $("kpi-active").textContent = fmtNum(by.active ?? 0);
    $("kpi-active-foot").textContent = `${fmtNum(by.idle ?? 0)} idle · ${fmtNum(by.charging ?? 0)} charging`;
    $("kpi-offline").textContent = fmtNum(by.offline ?? 0);
    const openBy = s.open_alerts_by_rule ?? {};
    const openTotal = Object.values(openBy).reduce((a, b) => a + b, 0);
    $("kpi-alerts").textContent = fmtNum(openTotal);
    $("kpi-alerts-foot").textContent = openTotal
      ? Object.entries(openBy).map(([r, n]) => `${n} ${r}`).join(" · ")
      : "fleet healthy";
    $("kpi-soc").textContent = s.avg_soc_pct == null ? "—" : `${fmt1(s.avg_soc_pct)}%`;
    $("kpi-soc-bar").style.width = `${Math.min(100, s.avg_soc_pct ?? 0)}%`;
    $("kpi-complete").textContent = s.data_completeness_pct == null ? "—" : `${fmt1(s.data_completeness_pct)}%`;
    $("kpi-complete-bar").style.width = `${Math.min(100, s.data_completeness_pct ?? 0)}%`;
    $("kpi-readings").textContent = `${fmtNum(s.readings_total)} readings`;
    const badge = $("alerts-badge");
    badge.textContent = openTotal;
    badge.classList.toggle("zero", openTotal === 0);
  } catch (err) {
    if (Date.now() - summaryErrToastAt > 30000) {
      summaryErrToastAt = Date.now();
      toast("error", "API unreachable", `Fleet summary failed: ${err.message}`);
    }
  }
}

/* ---------------- vehicles ---------------- */
async function loadVehicles() {
  try {
    let cursor = null;
    let pages = 0;
    do {
      const qs = new URLSearchParams({ limit: "200" });
      if (cursor) qs.set("cursor", cursor);
      const page = await api(`/v1/vehicles?${qs}`);
      for (const v of page.vehicles) state.vehicles.set(v.vehicle_id, v);
      cursor = page.next_cursor;
      pages++;
      if (!page.has_more) break;
    } while (cursor && pages < 25);
    state.vehiclesDirty = true;
    state.mapDirty = true;
  } catch (err) {
    toast("error", "Vehicle load failed", err.message);
  }
}

function filteredVehicles() {
  let arr = [...state.vehicles.values()];
  if (state.vehSearch) {
    const q = state.vehSearch.toLowerCase();
    arr = arr.filter((v) => v.vehicle_id.toLowerCase().includes(q));
  }
  if (state.vehStatus) arr = arr.filter((v) => v.status === state.vehStatus);
  const cmp = {
    id: (a, b) => a.vehicle_id.localeCompare(b.vehicle_id),
    soc: (a, b) => (b.soc_pct ?? -1) - (a.soc_pct ?? -1),
    temp: (a, b) => (b.battery_temp_c ?? -999) - (a.battery_temp_c ?? -999),
    speed: (a, b) => (b.speed_kmh ?? -1) - (a.speed_kmh ?? -1),
    seen: (a, b) => new Date(b.last_seen_at ?? 0) - new Date(a.last_seen_at ?? 0),
  }[state.vehSort];
  return arr.sort(cmp);
}

function socColor(soc) {
  if (soc == null) return "var(--dim)";
  if (soc < 20) return "var(--red)";
  if (soc < 45) return "var(--amber)";
  return "var(--green)";
}

function renderVehicles() {
  const arr = filteredVehicles();
  const shown = arr.slice(0, state.vehCap);
  const rows = shown.map((v) => {
    const tempCls = v.battery_temp_c > 55 ? " hot" : "";
    const socCls = v.soc_pct != null && v.soc_pct < 20 ? " low" : "";
    return `<tr data-veh="${esc(v.vehicle_id)}">
      <td class="v-id">${esc(v.vehicle_id)}</td>
      <td><span class="status-pill ${esc(v.status)}">${esc(v.status)}</span></td>
      <td><span class="soc-cell"><span class="v-num${socCls}">${v.soc_pct == null ? "—" : fmt1(v.soc_pct) + "%"}</span>
        <span class="soc-mini"><i style="width:${Math.min(100, v.soc_pct ?? 0)}%;background:${socColor(v.soc_pct)}"></i></span></span></td>
      <td class="v-num${tempCls}">${v.battery_temp_c == null ? "—" : fmt1(v.battery_temp_c) + "°"}</td>
      <td class="v-num">${v.speed_kmh == null ? "—" : fmt1(v.speed_kmh)}</td>
      <td class="v-num">${fmtNum(v.latest_seq)}</td>
      <td class="v-num">${fmtAgo(v.last_seen_at)}</td>
    </tr>`;
  }).join("");
  $("veh-body").innerHTML = rows;
  $("veh-empty").hidden = arr.length > 0;
  $("veh-count").textContent = `${fmtNum(arr.length)} of ${fmtNum(state.vehicles.size)} vehicles`;
  $("btn-loadmore").hidden = arr.length <= state.vehCap;
}

/* ---------------- alerts ---------------- */
async function loadAlerts() {
  try {
    const qs = new URLSearchParams({ limit: "300" });
    if (state.alertStatus) qs.set("status", state.alertStatus);
    if (state.alertRule) qs.set("rule_type", state.alertRule);
    state.alerts = await api(`/v1/alerts?${qs}`);
    state.alertsDirty = true;
  } catch (err) {
    toast("error", "Alert load failed", err.message);
  }
}

function alertDetailsText(details) {
  if (!details) return "";
  const entries = Object.entries(details).slice(0, 5)
    .map(([k, v]) => `${k}: ${typeof v === "object" ? JSON.stringify(v) : v}`);
  const s = entries.join(" · ");
  return s.length > 180 ? s.slice(0, 180) + "…" : s;
}

function alertCard(a, compact = false) {
  const ackBtn = a.status === "open"
    ? `<button class="btn tiny ack" data-ack="${a.id}" data-version="${a.version}">Acknowledge</button>`
    : "";
  return `<div class="alert-card ${esc(a.status)}" data-alert="${a.id}">
    <div class="alert-top">
      <span class="rule-tag ${esc(a.rule_type)}">${esc(a.rule_type)}</span>
      <span class="alert-veh" data-veh="${esc(a.vehicle_id)}">${esc(a.vehicle_id)}</span>
      <span class="alert-status ${esc(a.status)}">${esc(a.status)}</span>
    </div>
    <div class="alert-meta">
      <span>#${a.id} · v${a.version}</span>
      <span>opened ${fmtTime(a.opened_at)} (${fmtAgo(a.opened_at)})</span>
      ${a.acknowledged_at ? `<span>ack ${fmtTime(a.acknowledged_at)}</span>` : ""}
      ${a.resolved_at ? `<span>resolved ${fmtTime(a.resolved_at)}</span>` : ""}
    </div>
    ${!compact && a.details ? `<div class="alert-details">${esc(alertDetailsText(a.details))}</div>` : ""}
    ${ackBtn ? `<div class="alert-actions">${ackBtn}</div>` : ""}
  </div>`;
}

function renderAlerts() {
  const list = $("alert-list");
  list.innerHTML = state.alerts.map((a) => alertCard(a)).join("");
  $("alert-empty").hidden = state.alerts.length > 0;
}

async function ackAlert(alertId, version) {
  const a = state.alerts.find((x) => x.id === alertId);
  const prev = a ? { ...a } : null;
  if (a) {
    a.status = "acknowledged";
    a.version = version + 1;
    a.acknowledged_at = new Date().toISOString();
    renderAlerts();
    if (state.drawerId === a.vehicle_id) renderDrawerAlerts();
  }
  try {
    const updated = await api(`/v1/alerts/${alertId}/ack`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ version }),
    });
    const i = state.alerts.findIndex((x) => x.id === alertId);
    if (i >= 0) state.alerts[i] = updated;
    toast("success", `Alert #${alertId} acknowledged`, `${updated.rule_type} · ${updated.vehicle_id} · now v${updated.version}`);
    loadSummary();
  } catch (err) {
    if (a && prev) Object.assign(a, prev);
    state.alertsDirty = true;
    toast("error", `Ack failed (${err.status ?? "network"})`, err.message);
    loadAlerts();
  }
  state.alertsDirty = true;
}

/* ---------------- geofence map ---------------- */
const GEOFENCE = [[12.8, 79.9], [13.3, 79.9], [13.3, 80.5], [12.8, 80.5]];

function inPolygon(lat, lng, poly) {
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [yi, xi] = poly[i], [yj, xj] = poly[j];
    if ((yi > lat) !== (yj > lat) && lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}

function renderMap() {
  const svg = $("geo-map");
  const W = 600, H = 460, PAD = 34;
  const lats = GEOFENCE.map((p) => p[0]), lngs = GEOFENCE.map((p) => p[1]);
  const minLat = Math.min(...lats) - 0.06, maxLat = Math.max(...lats) + 0.06;
  const minLng = Math.min(...lngs) - 0.07, maxLng = Math.max(...lngs) + 0.07;
  const X = (lng) => PAD + ((Math.min(Math.max(lng, minLng), maxLng) - minLng) / (maxLng - minLng)) * (W - 2 * PAD);
  const Y = (lat) => H - PAD - ((Math.min(Math.max(lat, minLat), maxLat) - minLat) / (maxLat - minLat)) * (H - 2 * PAD);

  let out = `<defs>
    <radialGradient id="mapbg" cx="50%" cy="42%" r="75%">
      <stop offset="0%" stop-color="#101d36"/><stop offset="100%" stop-color="#070c17"/>
    </radialGradient>
  </defs>
  <rect x="0" y="0" width="${W}" height="${H}" rx="12" fill="url(#mapbg)" stroke="#1e2d4a"/>`;

  for (let lat = 12.8; lat <= 13.31; lat += 0.1) {
    out += `<line x1="${PAD}" y1="${Y(lat)}" x2="${W - PAD}" y2="${Y(lat)}" stroke="#16223a" stroke-width="1"/>
      <text x="${PAD - 6}" y="${Y(lat) + 3}" fill="#5c6d8f" font-size="9" text-anchor="end" font-family="monospace">${lat.toFixed(1)}</text>`;
  }
  for (let lng = 79.9; lng <= 80.51; lng += 0.1) {
    out += `<line x1="${X(lng)}" y1="${PAD}" x2="${X(lng)}" y2="${H - PAD}" stroke="#16223a" stroke-width="1"/>
      <text x="${X(lng)}" y="${H - PAD + 15}" fill="#5c6d8f" font-size="9" text-anchor="middle" font-family="monospace">${lng.toFixed(1)}</text>`;
  }

  const polyPts = GEOFENCE.map(([la, ln]) => `${X(ln)},${Y(la)}`).join(" ");
  out += `<polygon points="${polyPts}" fill="rgba(232,121,249,0.05)" stroke="#e879f9" stroke-width="1.6" stroke-dasharray="7 5"/>
    <text x="${X(79.9) + 8}" y="${Y(13.3) - 8}" fill="#e879f9" font-size="10" letter-spacing="2" font-family="monospace">GEOFENCE</text>`;

  let plotted = 0, breaches = 0;
  for (const v of state.vehicles.values()) {
    if (v.lat == null || v.lng == null) continue;
    plotted++;
    const breach = !inPolygon(v.lat, v.lng, GEOFENCE);
    if (breach) breaches++;
    const color = breach ? "#e879f9" : STATUS_COLORS[v.status] ?? "#5c6d8f";
    const r = breach ? 4 : 2.7;
    if (breach) out += `<circle cx="${X(v.lng)}" cy="${Y(v.lat)}" r="7.5" fill="none" stroke="#e879f9" stroke-opacity="0.4"/>`;
    out += `<circle class="geo-vehicle" data-veh="${esc(v.vehicle_id)}" cx="${X(v.lng)}" cy="${Y(v.lat)}" r="${r}" fill="${color}" fill-opacity="0.88"><title>${esc(v.vehicle_id)} · ${esc(v.status)}${breach ? " · OUTSIDE GEOFENCE" : ""}</title></circle>`;
  }
  svg.innerHTML = out;
  $("map-info").textContent = `${fmtNum(plotted)} vehicles plotted · ${fmtNum(breaches)} outside the geofence · positions update live from the stream`;
}

/* ---------------- drawer + history chart ---------------- */
function openDrawer(vehicleId) {
  state.drawerId = vehicleId;
  $("drawer-id").textContent = vehicleId;
  $("drawer").classList.add("open");
  $("drawer").setAttribute("aria-hidden", "false");
  $("drawer-scrim").hidden = false;
  renderDrawerStats();
  renderDrawerAlerts();
  loadHistory(vehicleId);
}
function closeDrawer() {
  state.drawerId = null;
  $("drawer").classList.remove("open");
  $("drawer").setAttribute("aria-hidden", "true");
  $("drawer-scrim").hidden = true;
}

function renderDrawerStats() {
  const v = state.vehicles.get(state.drawerId);
  if (!v) {
    $("drawer-stats").innerHTML = `<div class="dstat" style="grid-column:1/-1"><span>status</span><b>not in the current vehicle list</b></div>`;
    return;
  }
  const cells = [
    ["Status", `<span class="status-pill ${esc(v.status)}">${esc(v.status)}</span>`],
    ["SoC", `${v.soc_pct == null ? "—" : fmt1(v.soc_pct) + "%"}`],
    ["Battery", `${v.battery_temp_c == null ? "—" : fmt1(v.battery_temp_c) + "°C"}`],
    ["Speed", `${v.speed_kmh == null ? "—" : fmt1(v.speed_kmh) + " km/h"}`],
    ["Odometer", `${v.odometer_km == null ? "—" : fmtNum(Math.round(v.odometer_km)) + " km"}`],
    ["Latest seq", fmtNum(v.latest_seq)],
    ["Received", fmtNum(v.received_count)],
    ["Last seen", fmtAgo(v.last_seen_at)],
  ];
  $("drawer-stats").innerHTML = cells.map(([k, val]) => `<div class="dstat"><span>${k}</span><b>${val}</b></div>`).join("");
}

async function renderDrawerAlerts() {
  const vid = state.drawerId;
  if (!vid) return;
  try {
    const alerts = await api(`/v1/alerts?vehicle_id=${encodeURIComponent(vid)}&limit=50`);
    if (state.drawerId !== vid) return;
    $("drawer-alert-list").innerHTML = alerts.length
      ? alerts.map((a) => alertCard(a, true)).join("")
      : `<li class="drawer-none">No alerts recorded for this vehicle.</li>`;
  } catch (err) {
    $("drawer-alert-list").innerHTML = `<li class="drawer-none">Could not load alerts: ${esc(err.message)}</li>`;
  }
}

async function loadHistory(vehicleId) {
  const svg = $("hist-chart");
  svg.innerHTML = `<text x="280" y="120" fill="#5c6d8f" font-size="12" text-anchor="middle" font-family="monospace">loading history…</text>`;
  const to = new Date();
  const from = new Date(to.getTime() - 3600 * 1000);
  try {
    const h = await api(`/v1/vehicles/${encodeURIComponent(vehicleId)}/telemetry?from=${encodeURIComponent(from.toISOString())}&to=${encodeURIComponent(to.toISOString())}&points=240`);
    if (state.drawerId !== vehicleId) return;
    drawHistory(h);
  } catch (err) {
    if (state.drawerId !== vehicleId) return;
    svg.innerHTML = `<text x="280" y="120" fill="#ff4d6d" font-size="12" text-anchor="middle" font-family="monospace">${esc(err.message)}</text>`;
    $("chart-foot").textContent = "history unavailable";
  }
}

function drawHistory(h) {
  const svg = $("hist-chart");
  const pts = h.readings ?? [];
  if (!pts.length) {
    svg.innerHTML = `<text x="280" y="120" fill="#5c6d8f" font-size="12" text-anchor="middle" font-family="monospace">no readings in the last hour</text>`;
    $("chart-foot").textContent = "window: last 60 min";
    return;
  }
  const W = 560, H = 240, L = 40, R = 10, laneH = 62, gap = 14, top = 8;
  const t0 = new Date(pts[0].ts).getTime();
  const t1 = new Date(pts[pts.length - 1].ts).getTime();
  const span = Math.max(1, t1 - t0);
  const X = (ts) => L + ((new Date(ts).getTime() - t0) / span) * (W - L - R);
  const lanes = [
    { key: "soc_pct", color: "#ffb224", min: 0, max: 100, label: "SoC %" },
    { key: "battery_temp_c", color: "#ff4d6d", min: 0, max: 80, label: "Temp °C" },
    { key: "speed_kmh", color: "#3fd2ff", min: 0, max: 80, label: "km/h" },
  ];
  let out = `<defs><linearGradient id="socfill" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0%" stop-color="#ffb224" stop-opacity="0.28"/><stop offset="100%" stop-color="#ffb224" stop-opacity="0"/>
  </linearGradient></defs>`;
  lanes.forEach((lane, li) => {
    const yTop = top + li * (laneH + gap);
    const Y = (val) => yTop + laneH - ((Math.min(Math.max(val, lane.min), lane.max) - lane.min) / (lane.max - lane.min)) * laneH;
    for (const frac of [0, 0.5, 1]) {
      const y = yTop + laneH * (1 - frac);
      const val = lane.min + (lane.max - lane.min) * frac;
      out += `<line x1="${L}" y1="${y}" x2="${W - R}" y2="${y}" stroke="#182540" stroke-width="1"/>
        <text x="${L - 6}" y="${y + 3}" fill="#5c6d8f" font-size="8.5" text-anchor="end" font-family="monospace">${val}</text>`;
    }
    out += `<text x="${W - R}" y="${yTop + 9}" fill="${lane.color}" font-size="9" text-anchor="end" font-family="monospace" opacity="0.9">${lane.label}</text>`;
    const line = pts.map((p) => `${X(p.ts).toFixed(1)},${Y(p[lane.key]).toFixed(1)}`).join(" ");
    if (li === 0) {
      out += `<polygon points="${L},${yTop + laneH} ${line} ${W - R},${yTop + laneH}" fill="url(#socfill)"/>`;
    }
    out += `<polyline points="${line}" fill="none" stroke="${lane.color}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>`;
    const last = pts[pts.length - 1];
    out += `<circle cx="${X(last.ts)}" cy="${Y(last[lane.key])}" r="3" fill="${lane.color}"/>`;
  });
  out += `<text x="${L}" y="${H - 3}" fill="#5c6d8f" font-size="9" font-family="monospace">${fmtTime(pts[0].ts)}</text>
    <text x="${W - R}" y="${H - 3}" fill="#5c6d8f" font-size="9" text-anchor="end" font-family="monospace">${fmtTime(pts[pts.length - 1].ts)}</text>`;
  svg.innerHTML = out;
  $("chart-foot").textContent =
    `${h.returned_count} points · ${h.downsampled ? `downsampled (${Number(h.bucket_seconds).toFixed(1)}s buckets)` : "raw"} · ${fmtNum(h.raw_count)} readings in window`;
}

/* ---------------- ingest lab ---------------- */
function parseLabNum(id, fallback) {
  const raw = $(id).value.trim();
  if (raw === "") return fallback;
  const v = Number(raw);
  return Number.isNaN(v) ? raw : v;
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pick(rng, arr) {
  return arr[Math.floor(rng() * arr.length)];
}

const LAB_INVALID_SCENARIOS = [
  ["soc_above_100", (b, rng) => { b.soc_pct = pick(rng, [101, 101.5, 125, 150]); }],
  ["soc_negative", (b, rng) => { b.soc_pct = pick(rng, [-1, -0.5, -15]); }],
  ["soc_null", (b) => { b.soc_pct = null; }],
  ["soc_string", (b, rng) => { b.soc_pct = pick(rng, ["75", "101", "NaN", "Infinity"]); }],
  ["soc_boolean", (b, rng) => { b.soc_pct = pick(rng, [true, false]); }],
  ["lat_out_of_range", (b, rng) => { b.lat = pick(rng, [91, 95, -90.5, -120]); }],
  ["lng_out_of_range", (b, rng) => { b.lng = pick(rng, [181, 200, -180.5, -210]); }],
  ["missing_coordinates", (b, rng) => { delete b[pick(rng, ["lat", "lng"])]; }],
  ["speed_negative", (b, rng) => { b.speed_kmh = pick(rng, [-0.5, -10, -25]); }],
  ["speed_invalid_type", (b, rng) => { b.speed_kmh = pick(rng, ["40", true, null]); }],
  ["odometer_negative", (b, rng) => { b.odometer_km = pick(rng, [-1, -50]); }],
  ["odometer_invalid_type", (b, rng) => { b.odometer_km = pick(rng, ["1200", false, null]); }],
  ["seq_nonpositive", (b, rng) => { b.seq = pick(rng, [0, -1, -42]); }],
  ["seq_invalid_type", (b, rng) => { b.seq = pick(rng, ["1", true, 1.5, null]); }],
  ["malformed_timestamp", (b, rng) => { b.ts = pick(rng, ["not-a-timestamp", "2026-11-20T10:00:00", 1700000000, "", null]); }],
  ["battery_temp_out_of_range", (b, rng) => { b.battery_temp_c = pick(rng, [151, 200, -51, -80]); }],
  ["charging_invalid_type", (b, rng) => { b.charging = pick(rng, ["true", "yes", 1, 0, null]); }],
  ["fault_codes_malformed", (b, rng) => { b.fault_codes = pick(rng, ["P0A80", [123], [""], null]); }],
  ["missing_required_field", (b, rng) => { delete b[pick(rng, ["soc_pct", "seq", "ts", "speed_kmh", "odometer_km", "charging"])]; }],
];

function corruptLabReading(reading, rng) {
  const bad = { ...reading };
  const [reason, mutate] = pick(rng, LAB_INVALID_SCENARIOS);
  mutate(bad, rng);
  return { payload: bad, expectedValid: false, reason };
}

function isLabBaseValid(base) {
  return (
    typeof base.vehicleId === "string" && base.vehicleId.trim().length > 0 &&
    typeof base.seq === "number" && Number.isInteger(base.seq) && base.seq >= 1 &&
    typeof base.soc === "number" && base.soc >= 0 && base.soc <= 100 &&
    typeof base.temp === "number" && base.temp >= -50 && base.temp <= 150 &&
    typeof base.speed === "number" && base.speed >= 0 && base.speed <= 9999.99 &&
    typeof base.lat === "number" && base.lat >= -90 && base.lat <= 90 &&
    typeof base.lng === "number" && base.lng >= -180 && base.lng <= 180
  );
}

function labReading(i, n, base, rng) {
  // Keep large runs inside a sane window: ~1 h of device time and a few
  // percent of SoC drift across the whole batch, however many readings.
  const stepMs = Math.min(5000, Math.floor(3600000 / n));
  const socDrift = Math.min(0.15, 6 / n);
  const ts = new Date(Date.now() - (n - 1 - i) * stepMs);
  const jitter = (amp) => (rng() - 0.5) * amp;
  const single = n === 1;

  // Never clamp out-of-range user inputs into validity — send them verbatim
  // so backend validation can reject them. Only apply synthetic drift/jitter
  // for multi-reading batches whose base input is already within valid bounds.
  const socInRange = typeof base.soc === "number" && base.soc >= 0 && base.soc <= 100;
  const soc_pct = (!socInRange || single)
    ? base.soc
    : Math.min(100, Math.max(0, +(base.soc - i * socDrift + jitter(0.4)).toFixed(2)));

  const tempInRange = typeof base.temp === "number" && base.temp >= -50 && base.temp <= 150;
  const battery_temp_c = base.hot
    ? +(58 + (i % 4)).toFixed(1)
    : ((!tempInRange || single) ? base.temp : +(base.temp + jitter(1.5)).toFixed(1));

  const speedInRange = typeof base.speed === "number" && base.speed >= 0;
  const speed_kmh = (!speedInRange || single)
    ? base.speed
    : Math.max(0, +(base.speed + jitter(6)).toFixed(1));

  const latInRange = typeof base.lat === "number" && base.lat >= -90 && base.lat <= 90;
  const lat = (!latInRange || single) ? base.lat : +(base.lat + jitter(0.004)).toFixed(5);

  const lngInRange = typeof base.lng === "number" && base.lng >= -180 && base.lng <= 180;
  const lng = (!lngInRange || single) ? base.lng : +(base.lng + jitter(0.004)).toFixed(5);

  const seq = typeof base.seq === "number" && Number.isInteger(base.seq) ? base.seq + i : base.seq;

  return {
    vehicle_id: base.vehicleId,
    seq,
    ts: ts.toISOString(),
    soc_pct,
    battery_temp_c,
    speed_kmh,
    lat,
    lng,
    odometer_km: +(1200 + i * 0.35).toFixed(2),
    charging: base.charging,
    fault_codes: [],
  };
}

async function signBatch(secret, apiKey, timestamp, bodyBytes) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  const prefix = enc.encode(`${apiKey}\n${timestamp}\n`);
  const msg = new Uint8Array(prefix.length + bodyBytes.length);
  msg.set(prefix, 0);
  msg.set(bodyBytes, prefix.length);
  const sig = await crypto.subtle.sign("HMAC", key, msg);
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function sendLabBatch() {
  if (!state.creds) { openCreds(); toast("warn", "Credentials required", "Add a device-group API key and secret first."); return; }
  if (!crypto.subtle) { toast("error", "Web Crypto unavailable", "Open the console via http://localhost — signing needs a secure context."); return; }

  const vehicleId = $("lab-vehicle").value.trim() || "LAB-001";
  const requested = parseInt($("lab-count").value, 10) || 1;
  const n = Math.min(5000, Math.max(1, requested));
  if (requested > 5000) toast("warn", "Capped at 5,000 readings", "Split into batches of 500 for the API contract.");
  const seq = parseLabNum("lab-seq", 1);
  const seedVal = parseInt($("lab-seed").value, 10);
  const seed = Number.isFinite(seedVal) ? seedVal : 42;
  const invalidPct = Math.min(100, Math.max(0, Number($("lab-invalid-pct").value) || 0));
  const dupPct = Math.min(100, Math.max(0, Number($("lab-dup-pct").value) || 0));
  const rng = mulberry32(seed);

  const base = {
    vehicleId, seq,
    soc: parseLabNum("lab-soc", 0),
    temp: parseLabNum("lab-temp", 0),
    speed: parseLabNum("lab-speed", 0),
    lat: parseLabNum("lab-lat", 0),
    lng: parseLabNum("lab-lng", 0),
    charging: $("lab-charging").checked,
    hot: $("lab-hot").checked,
  };

  const baseValid = isLabBaseValid(base);
  const audit = {
    generated: n,
    validGenerated: 0,
    invalidGenerated: 0,
    dupesInjected: 0,
    validAccepted: 0,
    invalidRejected: 0,
    unexpectedInvalidAccepted: 0,
    unexpectedValidRejected: 0,
  };

  const items = [];
  for (let i = 0; i < n; i++) {
    const raw = labReading(i, n, base, rng);
    let item;
    if (!baseValid) {
      // User manually entered an out-of-range base value (e.g. SoC 101):
      // transmit it verbatim so they see the exact backend rejection.
      item = { payload: raw, expectedValid: false, reason: "manual_invalid_input" };
      audit.invalidGenerated++;
    } else if (n > 1 && rng() < invalidPct / 100) {
      item = corruptLabReading(raw, rng);
      audit.invalidGenerated++;
    } else if (n === 1 && invalidPct >= 100) {
      item = corruptLabReading(raw, rng);
      audit.invalidGenerated++;
    } else {
      item = { payload: raw, expectedValid: true, reason: null };
      audit.validGenerated++;
    }
    items.push(item);

    // Inject realistic duplicate copies across the batch.
    if (n > 1 && dupPct > 0 && rng() < dupPct / 100) {
      items.push({ payload: { ...item.payload }, expectedValid: item.expectedValid, reason: item.reason, isDup: true });
      audit.dupesInjected++;
    }
  }

  // Out-of-order: realistic adjacent swaps (~15%) rather than full reversal.
  if ($("lab-ooo").checked && items.length > 1) {
    for (let i = 0; i < items.length - 1; i++) {
      if (rng() < 0.15) {
        [items[i], items[i + 1]] = [items[i + 1], items[i]];
        i++;
      }
    }
  }

  // The API envelope allows at most 500 readings per request; larger runs
  // are split into sequential signed batches and aggregated here.
  const chunks = [];
  for (let i = 0; i < items.length; i += 500) chunks.push(items.slice(i, i + 500));

  const btn = $("lab-send");
  const btnHtml = btn.innerHTML;
  btn.disabled = true;
  const totals = { accepted: 0, duplicates: 0, rejected: 0 };
  const results = [];
  let lastStatus = 0;
  let failed = null;

  for (let ci = 0; ci < chunks.length; ci++) {
    btn.textContent = chunks.length > 1 ? `Sending batch ${ci + 1}/${chunks.length}…` : "Signing…";
    $("lab-http").textContent = `batch ${ci + 1}/${chunks.length}`;
    const bodyBytes = new TextEncoder().encode(JSON.stringify({ readings: chunks[ci].map((x) => x.payload) }));
    try {
      const timestamp = String(Math.floor(Date.now() / 1000));
      const signature = await signBatch(state.creds.secret, state.creds.key, timestamp, bodyBytes);
      const res = await fetch("/v1/telemetry/batch", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": state.creds.key,
          "X-Timestamp": timestamp,
          "X-Signature": signature,
        },
        body: bodyBytes,
      });
      lastStatus = res.status;
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        failed = { status: res.status, detail: typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail ?? data), retryAfter: res.headers.get("Retry-After") };
        break;
      }
      totals.accepted += data.accepted ?? 0;
      totals.duplicates += data.duplicates ?? 0;
      totals.rejected += data.rejected ?? 0;
      for (const r of data.results ?? []) {
        const meta = chunks[ci][r.index];
        if (meta) {
          if (meta.expectedValid) {
            if (r.status === "accepted") audit.validAccepted++;
            else if (r.status === "rejected") audit.unexpectedValidRejected++;
          } else {
            if (r.status === "rejected") audit.invalidRejected++;
            else audit.unexpectedInvalidAccepted++;
          }
        }
        if (results.length < 400) {
          results.push({ ...r, index: r.index + ci * 500, scenario: meta?.reason ?? null });
        }
      }
    } catch (err) {
      failed = { status: 0, detail: err.message, retryAfter: null };
      break;
    }
  }

  btn.disabled = false;
  btn.innerHTML = btnHtml;

  if (failed) {
    $("lab-http").textContent = `HTTP ${failed.status || "ERR"} · stopped at batch ${Math.min(chunks.length, results.length ? Math.ceil(results.length / 500) + 1 : 1)}/${chunks.length}`;
    renderLabError(failed.status, failed.detail, failed.retryAfter);
    if (totals.accepted + totals.duplicates + totals.rejected > 0) {
      $("lab-summary").innerHTML = labStats(totals, audit) + `<div class="lab-stat rejected"><b>${esc(failed.status || "ERR")}</b><span>${esc(failed.detail)}</span></div>`;
    }
    return;
  }

  $("lab-http").textContent = `HTTP ${lastStatus} · ${chunks.length} batch${chunks.length > 1 ? "es" : ""}`;
  renderLabResult(totals, results, audit);
  if (typeof seq === "number" && Number.isInteger(seq) && seq >= 1 && totals.accepted > 0) {
    $("lab-seq").value = seq + n;
  }
  const summaryMsg = `${totals.accepted} accepted · ${totals.duplicates} duplicate · ${totals.rejected} rejected (${audit.validGenerated} valid / ${audit.invalidGenerated} invalid generated)`;
  if (audit.unexpectedInvalidAccepted > 0) {
    toast("error", `VALIDATION FAILURE: ${audit.unexpectedInvalidAccepted} invalid accepted!`, summaryMsg);
  } else if (totals.rejected > 0 && totals.accepted === 0 && totals.duplicates === 0) {
    toast("error", `${fmtNum(totals.rejected)} readings rejected`, summaryMsg);
  } else if (totals.rejected > 0) {
    toast("warn", `Partial batch: ${fmtNum(totals.accepted)} accepted, ${fmtNum(totals.rejected)} rejected`, summaryMsg);
  } else {
    toast("success", `${fmtNum(items.length)} readings ingested`, summaryMsg);
  }
  setTimeout(() => { loadSummary(); loadVehicles(); }, 800);
}

function labStats(t, audit) {
  const vPct = audit && audit.generated ? ((100 * audit.validGenerated) / audit.generated).toFixed(1) : "0.0";
  const iPct = audit && audit.generated ? ((100 * audit.invalidGenerated) / audit.generated).toFixed(1) : "0.0";
  const auditBar = audit ? `
    <div class="lab-audit ${audit.unexpectedInvalidAccepted > 0 ? "fail" : "pass"}">
      <span>Generated: ${fmtNum(audit.validGenerated)} valid (${vPct}%) · ${fmtNum(audit.invalidGenerated)} invalid (${iPct}%) · ${fmtNum(audit.dupesInjected)} dupes</span>
      <b>${audit.unexpectedInvalidAccepted > 0
        ? `FAIL: ${audit.unexpectedInvalidAccepted} invalid accepted!`
        : `AUDIT PASS · 0 invalid accepted`}</b>
    </div>` : "";
  return `
    <div class="lab-stat accepted"><b>${fmtNum(t.accepted)}</b><span>accepted</span></div>
    <div class="lab-stat duplicate"><b>${fmtNum(t.duplicates)}</b><span>duplicate</span></div>
    <div class="lab-stat rejected"><b>${fmtNum(t.rejected)}</b><span>rejected</span></div>
    ${auditBar}`;
}

function renderLabError(status, detail, retryAfter) {
  $("lab-summary").innerHTML = `<div class="lab-stat rejected" style="flex:1"><b>${status || "ERR"}</b><span>${esc(detail)}${retryAfter ? ` · retry after ${esc(retryAfter)}s` : ""}</span></div>`;
  $("lab-results").innerHTML = "";
  toast("error", `Batch failed (${status || "network"})`, detail);
}

function renderLabResult(totals, results, audit) {
  $("lab-summary").innerHTML = labStats(totals, audit);
  const shown = results.slice(0, 300);
  const rows = shown.map((r) => {
    const tag = r.scenario ? `[${r.scenario}] ` : "";
    const msg = r.status === "rejected"
      ? tag + (r.errors ?? []).map((e) => `${e.field ?? e.loc ?? "?"}: ${e.msg ?? e.message ?? JSON.stringify(e)}`).join("; ")
      : `${r.vehicle_id ?? ""} seq ${r.seq ?? ""}`;
    return `<li class="lab-row ${esc(r.status)}"><span class="idx">[${r.index}]</span><span class="st">${esc(r.status)}</span><span class="msg">${esc(msg)}</span></li>`;
  }).join("");
  const omitted = results.length - shown.length;
  $("lab-results").innerHTML = rows + (omitted > 0
    ? `<li class="lab-row"><span class="idx">…</span><span class="st">+${fmtNum(omitted)}</span><span class="msg">further per-reading outcomes omitted from view</span></li>`
    : "");
}

/* ---------------- tabs ---------------- */
function switchTab(name) {
  document.querySelectorAll(".tab").forEach((t) => {
    const on = t.dataset.tab === name;
    t.classList.toggle("active", on);
    t.setAttribute("aria-selected", String(on));
  });
  document.querySelectorAll(".tab-body").forEach((b) => b.classList.toggle("active", b.id === `tab-${name}`));
  if (name === "map") { state.mapDirty = true; renderMap(); }
  if (name === "alerts") renderAlerts();
  if (name === "vehicles") renderVehicles();
}

function syncLabNextSeq() {
  const vid = $("lab-vehicle").value.trim() || "LAB-001";
  const v = state.vehicles.get(vid);
  const next = v && v.max_seq != null ? Number(v.max_seq) + 1 : 1;
  $("lab-seq").value = String(next);
}

/* ---------------- wiring ---------------- */
function wire() {
  $("btn-creds").addEventListener("click", openCreds);
  $("lab-open-creds").addEventListener("click", openCreds);
  $("creds-close").addEventListener("click", closeCreds);
  $("creds-scrim").addEventListener("click", (e) => { if (e.target === $("creds-scrim")) closeCreds(); });
  $("cred-save").addEventListener("click", () => {
    const key = $("cred-key").value.trim();
    const secret = $("cred-secret").value.trim();
    if (!key || !secret) { toast("warn", "Both fields required", "API key and secret must be set."); return; }
    state.creds = { key, secret };
    localStorage.setItem("vg-creds", JSON.stringify(state.creds));
    renderCredState();
    closeCreds();
    toast("success", "Credentials saved", "Stored in this browser only. The Ingest Lab is ready.");
  });
  $("cred-clear").addEventListener("click", () => {
    state.creds = null;
    localStorage.removeItem("vg-creds");
    $("cred-key").value = "";
    $("cred-secret").value = "";
    renderCredState();
    toast("warn", "Credentials cleared", "Ingest signing is disabled until you add them again.");
  });

  document.querySelectorAll(".tab").forEach((t) => t.addEventListener("click", () => switchTab(t.dataset.tab)));

  // stream controls
  $("btn-pause").addEventListener("click", () => {
    state.feedPaused = !state.feedPaused;
    $("btn-pause").textContent = state.feedPaused ? "Resume feed" : "Pause";
    $("btn-pause").setAttribute("aria-pressed", String(state.feedPaused));
    if (!state.feedPaused) rebuildFeed();
    setConn($("conn").dataset.state);
  });
  $("btn-clear").addEventListener("click", () => {
    state.events = [];
    state.seenIds.clear();
    state.totalEvents = 0;
    $("stream").innerHTML = "";
    $("stream-empty").style.display = "";
    $("stream-count").textContent = "0";
    $("stream-buffer").textContent = "buffer 0 / 600";
  });
  $("btn-resume").addEventListener("click", () => {
    const raw = $("resume-id").value.trim();
    if (raw === "") {
      sse.lastId = null;
      sse.resumeId = null;
      toast("control", "Live tail", "Reconnecting without Last-Event-ID — only new events.");
    } else if (!/^\d+$/.test(raw)) {
      toast("warn", "Invalid id", "Last-Event-ID must be a non-negative integer.");
      return;
    } else {
      sse.resumeId = Number(raw);
      toast("control", "Resuming", `Reconnecting with Last-Event-ID: ${raw}`);
    }
    connectSSE();
  });
  $("btn-replay").addEventListener("click", () => {
    sse.resumeId = 0;
    toast("control", "Replay from 0", "Requesting every retained event from the outbox.");
    connectSSE();
  });
  $("stream-filters").addEventListener("click", (e) => {
    const chip = e.target.closest(".chip");
    if (!chip) return;
    state.feedFilter = chip.dataset.filter;
    document.querySelectorAll("#stream-filters .chip").forEach((c) => c.classList.toggle("active", c === chip));
    rebuildFeed();
  });

  // vehicle table
  $("veh-search").addEventListener("input", (e) => { state.vehSearch = e.target.value.trim(); state.vehCap = 120; renderVehicles(); });
  $("veh-status").addEventListener("change", (e) => { state.vehStatus = e.target.value; state.vehCap = 120; renderVehicles(); });
  $("veh-sort").addEventListener("change", (e) => { state.vehSort = e.target.value; renderVehicles(); });
  $("btn-loadmore").addEventListener("click", () => { state.vehCap += 120; renderVehicles(); });
  $("btn-refresh-veh").addEventListener("click", loadVehicles);

  // alerts
  $("alert-status").addEventListener("change", (e) => { state.alertStatus = e.target.value; loadAlerts(); });
  $("alert-rule").addEventListener("change", (e) => { state.alertRule = e.target.value; loadAlerts(); });
  $("btn-refresh-alerts").addEventListener("click", loadAlerts);

  // lab
  $("lab-send").addEventListener("click", sendLabBatch);
  $("lab-vehicle").addEventListener("change", syncLabNextSeq);
  $("lab-valid-pct").addEventListener("input", (e) => {
    const v = Math.min(100, Math.max(0, Number(e.target.value) || 0));
    $("lab-invalid-pct").value = String(Math.round((100 - v) * 100) / 100);
  });
  $("lab-invalid-pct").addEventListener("input", (e) => {
    const inv = Math.min(100, Math.max(0, Number(e.target.value) || 0));
    $("lab-valid-pct").value = String(Math.round((100 - inv) * 100) / 100);
  });

  // drawer
  $("drawer-close").addEventListener("click", closeDrawer);
  $("drawer-scrim").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") { closeDrawer(); closeCreds(); }
  });

  // delegated clicks: vehicle links + ack buttons anywhere
  document.addEventListener("click", (e) => {
    const ack = e.target.closest("[data-ack]");
    if (ack) {
      ackAlert(Number(ack.dataset.ack), Number(ack.dataset.version));
      return;
    }
    const veh = e.target.closest("[data-veh]");
    if (veh) openDrawer(veh.dataset.veh);
  });
}

/* ---------------- periodic loops ---------------- */
function startLoops() {
  setInterval(() => { $("clock").textContent = new Date().toISOString().slice(11, 19); }, 1000);

  setInterval(() => {
    const cutoff = Date.now() - 5000;
    state.rateStamps = state.rateStamps.filter((t) => t > cutoff);
    $("evt-rate").textContent = (state.rateStamps.length / 5).toFixed(1);
  }, 1000);

  setInterval(() => {
    if (state.vehiclesDirty && $("tab-vehicles").classList.contains("active")) {
      state.vehiclesDirty = false;
      renderVehicles();
    }
    if (state.alertsDirty && $("tab-alerts").classList.contains("active")) {
      state.alertsDirty = false;
      renderAlerts();
    }
    if (state.mapDirty && $("tab-map").classList.contains("active")) {
      state.mapDirty = false;
      renderMap();
    }
  }, 1200);

  setInterval(loadSummary, 10000);
  setInterval(loadVehicles, 60000);
  setInterval(loadAlerts, 30000);
}

/* ---------------- init ---------------- */
async function init() {
  loadCreds();
  wire();
  startLoops();
  $("clock").textContent = new Date().toISOString().slice(11, 19);
  connectSSE();
  await Promise.all([loadSummary(), loadVehicles(), loadAlerts()]);
  syncLabNextSeq();
  renderVehicles();
  renderAlerts();
}

init();