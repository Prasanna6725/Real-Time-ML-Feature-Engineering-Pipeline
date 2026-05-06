#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
broker_host="${bootstrap_servers%%,*}"
broker_host="${broker_host%%:*}"
broker_port="${bootstrap_servers%%,*}"
broker_port="${broker_port##*:}"
max_attempts="${PIPELINE_KAFKA_STARTUP_MAX_ATTEMPTS:-30}"
retry_seconds="${PIPELINE_KAFKA_STARTUP_RETRY_SECONDS:-2}"

echo "[pipeline] Waiting for Kafka broker ${broker_host}:${broker_port}"
attempt=1
until bash -lc "</dev/tcp/${broker_host}/${broker_port}" >/dev/null 2>&1; do
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "[pipeline] Kafka broker not reachable after ${max_attempts} attempts"
    exit 1
  fi
  echo "[pipeline] Kafka not ready (attempt ${attempt}/${max_attempts}); retrying in ${retry_seconds}s"
  attempt=$((attempt + 1))
  sleep "$retry_seconds"
done

echo "[pipeline] Kafka broker is reachable; starting pipeline"

exec java \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -Dkafka.bootstrap.servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" \
  -Duser.events.topic="${USER_EVENTS_TOPIC:-user-events}" \
  -Dcontent.metadata.topic="${CONTENT_METADATA_TOPIC:-content-metadata}" \
  -Dfeature.store.topic="${FEATURE_STORE_TOPIC:-feature-store}" \
  -Dpipeline.metrics.topic="${PIPELINE_METRICS_TOPIC:-pipeline-metrics}" \
  -Dpipeline.http.port="${PIPELINE_HTTP_PORT:-8090}" \
  -Dpipeline.parallelism="${PIPELINE_PARALLELISM:-1}" \
  -Dpipeline.kafka.startup.max.attempts="${PIPELINE_KAFKA_STARTUP_MAX_ATTEMPTS:-30}" \
  -Dpipeline.kafka.startup.retry.seconds="${PIPELINE_KAFKA_STARTUP_RETRY_SECONDS:-2}" \
  -Dpipeline.user.window.minutes="${PIPELINE_USER_WINDOW_MINUTES:-5}" \
  -Dpipeline.content.window.minutes="${PIPELINE_CONTENT_WINDOW_MINUTES:-5}" \
  -Dpipeline.content.slide.minutes="${PIPELINE_CONTENT_SLIDE_MINUTES:-1}" \
  -Dpipeline.category.window.minutes="${PIPELINE_CATEGORY_WINDOW_MINUTES:-5}" \
  -Dproducer.data.dir="${PRODUCER_DATA_DIR:-/data}" \
  -jar /opt/feature-pipeline/app.jar
