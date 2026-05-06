from __future__ import annotations

import json
import os
import random
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import uvicorn


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def parse_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


def parse_float(value: str | None, default: float) -> float:
    try:
        return float(value) if value is not None else default
    except ValueError:
        return default


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def write_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
      handle.write(json.dumps(payload, separators=(",", ":")) + "\n")


class ProducerState:
    def __init__(self) -> None:
        self.ready = threading.Event()
        self.running = threading.Event()
        self.running.set()
        self.created_topics = False
        self.sent_events = 0
        self.sent_metadata = 0
        self.last_event_at: str | None = None
        self.last_metadata_at: str | None = None
        self.late_events = 0


app = FastAPI(title="Producer Service")
state = ProducerState()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
USER_EVENTS_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")
CONTENT_METADATA_TOPIC = os.getenv("CONTENT_METADATA_TOPIC", "content-metadata")
FEATURE_STORE_TOPIC = os.getenv("FEATURE_STORE_TOPIC", "feature-store")
PIPELINE_METRICS_TOPIC = os.getenv("PIPELINE_METRICS_TOPIC", "pipeline-metrics")
HTTP_PORT = parse_int(os.getenv("PRODUCER_HTTP_PORT"), 8000)
SPEEDUP_FACTOR = parse_int(os.getenv("PRODUCER_SPEEDUP_FACTOR"), 60)
EVENT_INTERVAL_SECONDS = parse_float(os.getenv("PRODUCER_EVENT_INTERVAL_SECONDS"), 0.2)
LATE_EVENT_RATE = parse_float(os.getenv("PRODUCER_LATE_EVENT_RATE"), 0.05)
SIMULATION_SEED = parse_int(os.getenv("PRODUCER_SIMULATION_SEED"), 42)
LATE_EVENT_MIN_SECONDS = parse_int(os.getenv("PRODUCER_LATE_EVENT_MIN_SECONDS"), 35)
LATE_EVENT_MAX_SECONDS = parse_int(os.getenv("PRODUCER_LATE_EVENT_MAX_SECONDS"), 90)
DATA_DIR = Path(os.getenv("PRODUCER_DATA_DIR", "/data"))
ENABLE_LATE_EVENTS = parse_bool(os.getenv("PRODUCER_ENABLE_LATE_EVENTS"), True)

RNG = random.Random(SIMULATION_SEED)

CONTENT_POOL = [
    {"content_id": "content_001", "category": "sci-fi", "creator_id": "creator_a"},
    {"content_id": "content_002", "category": "news", "creator_id": "creator_b"},
    {"content_id": "content_003", "category": "sports", "creator_id": "creator_c"},
    {"content_id": "content_004", "category": "finance", "creator_id": "creator_d"},
    {"content_id": "content_005", "category": "music", "creator_id": "creator_e"},
    {"content_id": "content_006", "category": "gaming", "creator_id": "creator_f"},
    {"content_id": "content_007", "category": "travel", "creator_id": "creator_g"},
    {"content_id": "content_008", "category": "education", "creator_id": "creator_h"},
]

USER_PROFILES = {
    "user_001": {
        "name": "binge_watcher",
        "event_weights": {"view": 0.55, "click": 0.22, "like": 0.13, "share": 0.10},
        "dwell_range": (12000, 55000),
    },
    "user_002": {
        "name": "news_scanner",
        "event_weights": {"view": 0.68, "click": 0.16, "like": 0.10, "share": 0.06},
        "dwell_range": (3000, 18000),
    },
    "user_003": {
        "name": "casual_browser",
        "event_weights": {"view": 0.58, "click": 0.20, "like": 0.12, "share": 0.10},
        "dwell_range": (5000, 25000),
    },
    "user_004": {
        "name": "collector",
        "event_weights": {"view": 0.45, "click": 0.25, "like": 0.20, "share": 0.10},
        "dwell_range": (8000, 30000),
    },
    "user_005": {
        "name": "power_user",
        "event_weights": {"view": 0.40, "click": 0.30, "like": 0.18, "share": 0.12},
        "dwell_range": (8000, 45000),
    },
}


def weighted_choice(weights: dict[str, float]) -> str:
    threshold = RNG.random()
    total = 0.0
    for key, weight in weights.items():
        total += weight
        if threshold <= total:
            return key
    return next(iter(weights))


def wait_for_kafka() -> KafkaAdminClient:
    last_error: Exception | None = None
    while state.running.is_set():
        try:
            return KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="producer-admin")
        except Exception as exc:  # pragma: no cover - startup retry
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka startup aborted: {last_error}")


def ensure_topics(admin: KafkaAdminClient) -> None:
    desired_topics = [
        NewTopic(name=USER_EVENTS_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=CONTENT_METADATA_TOPIC, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
        NewTopic(name=FEATURE_STORE_TOPIC, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
        NewTopic(name=PIPELINE_METRICS_TOPIC, num_partitions=1, replication_factor=1),
    ]
    existing_topics = set(admin.list_topics())
    topics_to_create = [topic for topic in desired_topics if topic.name not in existing_topics]
    if not topics_to_create:
        state.created_topics = True
        return
    try:
        admin.create_topics(topics_to_create, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    state.created_topics = True


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=10,
        linger_ms=50,
        value_serializer=json_bytes,
        key_serializer=lambda value: value.encode("utf-8"),
    )


def seed_metadata(producer: KafkaProducer) -> None:
    metadata_timestamp = datetime.now(timezone.utc)
    output_file = DATA_DIR / "generated_metadata.jsonl"
    for item in CONTENT_POOL:
        payload = {
            "content_id": item["content_id"],
            "category": item["category"],
            "creator_id": item["creator_id"],
            "publish_timestamp": isoformat(metadata_timestamp),
        }
        producer.send(CONTENT_METADATA_TOPIC, key=item["content_id"], value=payload)
        write_jsonl(output_file, payload)
        state.sent_metadata += 1
        state.last_metadata_at = payload["publish_timestamp"]
    producer.flush()


def choose_content(user_id: str) -> dict[str, str]:
    user_index = int(user_id.split("_")[-1]) if user_id.split("_")[-1].isdigit() else 0
    return CONTENT_POOL[(user_index + RNG.randint(0, len(CONTENT_POOL) - 1)) % len(CONTENT_POOL)]


def build_event(simulated_now: datetime, user_id: str) -> dict[str, Any]:
    profile = USER_PROFILES[user_id]
    content = choose_content(user_id)
    event_type = weighted_choice(profile["event_weights"])
    dwell_low, dwell_high = profile["dwell_range"]
    dwell_time_ms = RNG.randint(dwell_low, dwell_high)
    if event_type == "view":
        dwell_time_ms = max(1000, int(dwell_time_ms * 0.75))
    if event_type == "share":
        dwell_time_ms = max(dwell_time_ms, 2000)
    is_late = ENABLE_LATE_EVENTS and RNG.random() < LATE_EVENT_RATE
    if is_late:
        late_offset = RNG.randint(LATE_EVENT_MIN_SECONDS, LATE_EVENT_MAX_SECONDS)
        event_time = simulated_now - timedelta(seconds=late_offset)
        state.late_events += 1
    else:
        event_time = simulated_now - timedelta(seconds=RNG.uniform(0, 2))
    return {
        "user_id": user_id,
        "content_id": content["content_id"],
        "event_type": event_type,
        "dwell_time_ms": dwell_time_ms,
        "timestamp": isoformat(event_time),
    }


def emit_events() -> None:
    admin = wait_for_kafka()
    ensure_topics(admin)
    producer = build_producer()
    seed_metadata(producer)
    state.ready.set()

    simulation_clock = datetime.now(timezone.utc)
    output_file = DATA_DIR / "generated_events.jsonl"
    user_ids = list(USER_PROFILES)

    while state.running.is_set():
        simulation_clock += timedelta(seconds=EVENT_INTERVAL_SECONDS * SPEEDUP_FACTOR)
        for user_id in user_ids:
            event = build_event(simulation_clock, user_id)
            producer.send(USER_EVENTS_TOPIC, key=user_id, value=event)
            write_jsonl(output_file, event)
            state.sent_events += 1
            state.last_event_at = event["timestamp"]
        producer.flush()
        time.sleep(EVENT_INTERVAL_SECONDS)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ready": state.ready.is_set(),
        "topics_created": state.created_topics,
        "sent_events": state.sent_events,
        "sent_metadata": state.sent_metadata,
        "late_events": state.late_events,
        "last_event_at": state.last_event_at,
        "last_metadata_at": state.last_metadata_at,
    }


@app.get("/")
def root() -> dict[str, Any]:
    return health()


def main() -> None:
    worker = threading.Thread(target=emit_events, daemon=True)
    worker.start()
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")


if __name__ == "__main__":
    main()
