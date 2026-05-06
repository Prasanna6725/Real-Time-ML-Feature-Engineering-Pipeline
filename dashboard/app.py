from __future__ import annotations

import asyncio
import json
import os
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from kafka import KafkaConsumer
import uvicorn


def parse_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


app = FastAPI(title="Feature Dashboard")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FEATURE_STORE_TOPIC = os.getenv("FEATURE_STORE_TOPIC", "feature-store")
PIPELINE_METRICS_TOPIC = os.getenv("PIPELINE_METRICS_TOPIC", "pipeline-metrics")
HTTP_HOST = os.getenv("DASHBOARD_HTTP_HOST", "0.0.0.0")
HTTP_PORT = parse_int(os.getenv("DASHBOARD_PORT"), 8501)

feature_state: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
metric_state: dict[str, Any] = {
    "late_events_dropped": 0,
    "watermark_lag_ms": 0,
    "current_watermark": None,
    "last_metric_update": None,
}
event_queue: queue.Queue[dict[str, Any]] = queue.Queue()
state_ready = threading.Event()
consumer_started = threading.Event()


def consume_features() -> None:
    while True:
        try:
            consumer = KafkaConsumer(
                FEATURE_STORE_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="dashboard-feature-consumer",
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
                key_deserializer=lambda raw: raw.decode("utf-8") if raw else "",
            )
            break
        except Exception:
            time.sleep(2)

    for message in consumer:
        payload = message.value
        entity_id = payload.get("entity_id", "unknown")
        feature_name = payload.get("feature_name", "unknown")
        feature_state[entity_id][feature_name] = payload
        event_queue.put({"type": "feature", "payload": payload})
        state_ready.set()


def consume_metrics() -> None:
    while True:
        try:
            consumer = KafkaConsumer(
                PIPELINE_METRICS_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="dashboard-metrics-consumer",
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            break
        except Exception:
            time.sleep(2)

    consumer_started.set()
    for message in consumer:
        payload = message.value
        metric_state["late_events_dropped"] = int(payload.get("late_events_dropped", metric_state["late_events_dropped"]))
        metric_state["watermark_lag_ms"] = int(payload.get("watermark_lag_ms", metric_state["watermark_lag_ms"]))
        metric_state["current_watermark"] = payload.get("current_watermark")
        metric_state["last_metric_update"] = payload.get("computed_at")
        event_queue.put({"type": "metric", "payload": payload})
        state_ready.set()


def freshness_for(payload: dict[str, Any]) -> float:
    computed_at = payload.get("computed_at")
    if not computed_at:
        return 0.0
    return max(0.0, (utc_now() - parse_ts(computed_at)).total_seconds())


def latest_features(entity_id: str) -> list[dict[str, Any]]:
    records = list(feature_state.get(entity_id, {}).values())
    records.sort(key=lambda record: record.get("computed_at", ""), reverse=True)
    for record in records:
        record["freshness_seconds"] = freshness_for(record)
    return records


def render_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Real-Time Feature Dashboard</title>
  <style>
    :root {
      --bg: #0b1020;
      --panel: rgba(15, 23, 42, 0.82);
      --panel-strong: #111827;
      --text: #e5eefc;
      --muted: #9fb0cc;
      --accent: #7dd3fc;
      --accent-2: #34d399;
      --danger: #fb7185;
      --border: rgba(148, 163, 184, 0.2);
      --shadow: 0 20px 80px rgba(0, 0, 0, 0.4);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(125, 211, 252, 0.22), transparent 28%),
        radial-gradient(circle at top right, rgba(52, 211, 153, 0.18), transparent 24%),
        linear-gradient(180deg, #050816 0%, #0b1020 55%, #090e1a 100%);
      min-height: 100vh;
    }
    .shell {
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero {
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 20px;
      align-items: stretch;
      margin-bottom: 24px;
    }
    .title-card, .status-card, .panel {
      border: 1px solid var(--border);
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
      border-radius: 24px;
    }
    .title-card {
      padding: 28px;
      position: relative;
      overflow: hidden;
    }
    .title-card::after {
      content: "";
      position: absolute;
      inset: auto -20px -40px auto;
      width: 220px;
      height: 220px;
      background: radial-gradient(circle, rgba(125, 211, 252, 0.3), transparent 68%);
      pointer-events: none;
    }
    .eyebrow {
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 12px;
      color: var(--accent);
      margin-bottom: 12px;
    }
    h1 {
      margin: 0;
      font-size: clamp(32px, 5vw, 54px);
      line-height: 0.96;
      letter-spacing: -0.05em;
      max-width: 10ch;
    }
    .subtitle {
      margin-top: 16px;
      color: var(--muted);
      max-width: 64ch;
      line-height: 1.6;
    }
    .status-card {
      padding: 24px;
      display: grid;
      gap: 12px;
      align-content: space-between;
    }
    .metric-row {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
    }
    .metric {
      border: 1px solid var(--border);
      border-radius: 18px;
      background: rgba(8, 15, 32, 0.72);
      padding: 18px;
      min-height: 120px;
    }
    .metric label, .section-label {
      color: var(--muted);
      font-size: 13px;
      display: block;
      margin-bottom: 8px;
    }
    .metric strong {
      display: block;
      font-size: 28px;
      letter-spacing: -0.04em;
      margin-bottom: 6px;
    }
    .metric small { color: var(--muted); }
    .controls {
      display: flex;
      gap: 10px;
      align-items: center;
      margin: 16px 0 20px;
      flex-wrap: wrap;
    }
    input {
      width: min(360px, 100%);
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: rgba(3, 7, 18, 0.76);
      color: var(--text);
      font-size: 15px;
      outline: none;
    }
    button {
      padding: 14px 18px;
      border: 0;
      border-radius: 14px;
      color: #07111f;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      font-weight: 700;
      cursor: pointer;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 20px;
      margin-top: 20px;
    }
    .panel {
      padding: 22px;
    }
    .feature-list {
      display: grid;
      gap: 12px;
      margin-top: 14px;
    }
    .feature-item {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 16px;
      background: rgba(8, 15, 32, 0.6);
      border: 1px solid var(--border);
      border-radius: 16px;
    }
    .feature-name { font-weight: 600; }
    .feature-meta { color: var(--muted); font-size: 13px; text-align: right; }
    .status-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
      background: var(--accent-2);
      box-shadow: 0 0 0 6px rgba(52, 211, 153, 0.15);
      margin-right: 8px;
    }
    .mono { font-variant-numeric: tabular-nums; }
    .event-stream {
      max-height: 390px;
      overflow: auto;
    }
    .event-line {
      padding: 10px 0;
      border-bottom: 1px solid rgba(148, 163, 184, 0.12);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .event-line strong { color: var(--text); }
    .footer-note { margin-top: 12px; color: var(--muted); font-size: 13px; }
    @media (max-width: 920px) {
      .hero, .grid, .metric-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <div class="title-card">
        <div class="eyebrow">Kafka + Flink feature pipeline</div>
        <h1>Real-time features for live ranking and personalization.</h1>
        <p class="subtitle">
          The dashboard subscribes to the compacted feature store and pipeline metrics topic, giving you a live view of feature freshness, watermark lag, and late-event handling.
        </p>
      </div>
      <div class="status-card">
        <div><span class="status-dot"></span><span id="connection-status">Connecting to stream</span></div>
        <div class="section-label">Pipeline health</div>
        <div class="metric-row">
          <div class="metric">
            <label>Late events dropped</label>
            <strong id="late-events" class="mono">0</strong>
            <small>Events beyond the 30-second watermark tolerance.</small>
          </div>
          <div class="metric">
            <label>Watermark lag</label>
            <strong id="watermark-lag" class="mono">0 ms</strong>
            <small>Wall-clock time minus the latest watermark.</small>
          </div>
          <div class="metric">
            <label>Last metric update</label>
            <strong id="metric-updated" class="mono">-</strong>
            <small id="metric-watermark">Waiting for metrics stream.</small>
          </div>
        </div>
      </div>
    </div>

    <div class="controls">
      <input id="entity-input" value="user_001" placeholder="Enter user_id or content_id" />
      <button id="lookup-btn">Load entity</button>
      <div class="footer-note">Try <span class="mono">user_001</span> or <span class="mono">content_001</span>.</div>
    </div>

    <div class="grid">
      <div class="panel">
        <div class="section-label">Latest features for selected entity</div>
        <div id="entity-title" class="mono" style="font-size: 28px; font-weight: 700; letter-spacing: -0.04em; margin-top: 4px;">user_001</div>
        <div id="feature-list" class="feature-list"></div>
      </div>
      <div class="panel">
        <div class="section-label">Event stream</div>
        <div id="event-stream" class="event-stream"></div>
      </div>
    </div>
  </div>

  <script>
    const state = { entityId: 'user_001', features: [] };

    function formatFreshness(seconds) {
      if (seconds < 1) return 'just updated';
      if (seconds < 60) return `${seconds.toFixed(1)}s ago`;
      const minutes = Math.floor(seconds / 60);
      const remainder = Math.floor(seconds % 60);
      return `${minutes}m ${remainder}s ago`;
    }

    function renderFeatures(payload) {
      const container = document.getElementById('feature-list');
      const title = document.getElementById('entity-title');
      title.textContent = payload.entity_id || state.entityId;
      const records = payload.features || [];
      if (!records.length) {
        container.innerHTML = '<div class="feature-item"><div class="feature-name">No features yet</div><div class="feature-meta">Waiting for the pipeline to emit a value.</div></div>';
        return;
      }
      container.innerHTML = records.map((record) => {
        const freshness = formatFreshness(record.freshness_seconds || 0);
        return `
          <div class="feature-item">
            <div>
              <div class="feature-name">${record.feature_name}</div>
              <div class="feature-meta">Computed at ${record.computed_at}</div>
            </div>
            <div class="feature-meta">
              <div class="mono">${JSON.stringify(record.feature_value)}</div>
              <div>${freshness}</div>
            </div>
          </div>`;
      }).join('');
    }

    function renderMetrics(payload) {
      document.getElementById('late-events').textContent = payload.late_events_dropped ?? 0;
      document.getElementById('watermark-lag').textContent = `${payload.watermark_lag_ms ?? 0} ms`;
      document.getElementById('metric-updated').textContent = payload.computed_at || '-';
      document.getElementById('metric-watermark').textContent = payload.current_watermark ? `Watermark: ${payload.current_watermark}` : 'Waiting for metrics stream.';
    }

    function appendEvent(payload) {
      const stream = document.getElementById('event-stream');
      const line = document.createElement('div');
      line.className = 'event-line';
      line.innerHTML = `<strong>${payload.type}</strong><br/><span class="mono">${JSON.stringify(payload.payload)}</span>`;
      stream.prepend(line);
      while (stream.children.length > 40) stream.removeChild(stream.lastChild);
    }

    async function loadEntity(entityId) {
      state.entityId = entityId;
      const response = await fetch(`/api/entity/${encodeURIComponent(entityId)}`);
      const payload = await response.json();
      renderFeatures(payload);
    }

    document.getElementById('lookup-btn').addEventListener('click', () => {
      const entityId = document.getElementById('entity-input').value.trim() || 'user_001';
      loadEntity(entityId);
    });

    document.getElementById('entity-input').addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        document.getElementById('lookup-btn').click();
      }
    });

    loadEntity('user_001');

    const stream = new EventSource('/stream');
    stream.onopen = () => {
      document.getElementById('connection-status').textContent = 'Connected to feature stream';
    };
    stream.onerror = () => {
      document.getElementById('connection-status').textContent = 'Stream reconnecting';
    };
    stream.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.type === 'feature' && payload.payload.entity_id === state.entityId) {
        loadEntity(state.entityId);
      }
      if (payload.type === 'metric') {
        renderMetrics(payload.payload);
      }
      appendEvent(payload);
    };
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return render_page()


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ready": state_ready.is_set(),
        "consumer_started": consumer_started.is_set(),
        "entity_count": len(feature_state),
        "metric_state": metric_state,
    }


@app.get("/api/entity/{entity_id}")
def api_entity(entity_id: str) -> JSONResponse:
    records = latest_features(entity_id)
    return JSONResponse({"entity_id": entity_id, "features": records})


@app.get("/api/metrics")
def api_metrics() -> JSONResponse:
    return JSONResponse(metric_state)


@app.get("/stream")
async def stream(_: Request) -> StreamingResponse:
    async def event_generator() -> Any:
        while True:
            try:
                payload = event_queue.get(timeout=1.0)
                yield f"data: {json.dumps(payload)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"
                await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


def start_consumers() -> None:
    threading.Thread(target=consume_features, daemon=True).start()
    threading.Thread(target=consume_metrics, daemon=True).start()


def main() -> None:
    start_consumers()
    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT, log_level="info")


if __name__ == "__main__":
    main()
