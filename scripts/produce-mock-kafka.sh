#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.kafka.yml}"
TOPIC="${TOPIC:-demo.logs}"
COUNT="${COUNT:-50}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -le 0 ]; then
  echo "COUNT must be a positive integer, got: $COUNT" >&2
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  DC=(docker compose -f "$COMPOSE_FILE")
elif command -v docker-compose >/dev/null 2>&1; then
  DC=(docker-compose -f "$COMPOSE_FILE")
else
  echo "Missing docker compose (docker compose or docker-compose)." >&2
  exit 1
fi

mapfile -t SERVICES < <("${DC[@]}" config --services 2>/dev/null || true)
if [ "${#SERVICES[@]}" -eq 0 ]; then
  echo "No services found in compose file: $COMPOSE_FILE" >&2
  exit 1
fi

service_exists() {
  local needle="$1"
  local s
  for s in "${SERVICES[@]}"; do
    if [ "$s" = "$needle" ]; then
      return 0
    fi
  done
  return 1
}

if [ -n "${KAFKA_SERVICE:-}" ]; then
  SERVICE="$KAFKA_SERVICE"
  if ! service_exists "$SERVICE"; then
    echo "KAFKA_SERVICE '$SERVICE' not found in compose file." >&2
    echo "Available services: ${SERVICES[*]}" >&2
    exit 1
  fi
elif service_exists kafka; then
  SERVICE="kafka"
elif service_exists broker; then
  SERVICE="broker"
else
  SERVICE="${SERVICES[0]}"
fi

"${DC[@]}" up -d "$SERVICE" >/dev/null

echo "Ensuring topic exists: $TOPIC"
"${DC[@]}" exec -T \
  -e TOPIC="$TOPIC" \
  -e KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
  "$SERVICE" bash -lc \
  '
set -euo pipefail
if command -v kafka-topics.sh >/dev/null 2>&1; then
  TOPICS_CMD="kafka-topics.sh"
elif command -v kafka-topics >/dev/null 2>&1; then
  TOPICS_CMD="kafka-topics"
elif [ -x /opt/kafka/bin/kafka-topics.sh ]; then
  TOPICS_CMD="/opt/kafka/bin/kafka-topics.sh"
elif [ -x /opt/kafka/bin/kafka-topics ]; then
  TOPICS_CMD="/opt/kafka/bin/kafka-topics"
else
  echo "Could not find kafka-topics command inside container." >&2
  exit 127
fi
"$TOPICS_CMD" --bootstrap-server "$KAFKA_BOOTSTRAP" --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
'

echo "Producing $COUNT mock messages to topic: $TOPIC"
{
  for i in $(seq 1 "$COUNT"); do
    case $((RANDOM % 4)) in
      0) level="DEBUG" ;;
      1) level="INFO" ;;
      2) level="WARN" ;;
      3) level="ERROR" ;;
    esac
    ts="$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")"
    req_id="$(printf "req-%06d" "$((RANDOM % 1000000))")"
    user_id="$((RANDOM % 10000))"

    printf '{"adate":"%s","stuff":"mock kafka event %d","demo":"kafka-local-demo","request_id":"%s","user_id":%d}\n' \
      "$ts" "$i" "$req_id" "$user_id"
  done
} | "${DC[@]}" exec -T \
  -e TOPIC="$TOPIC" \
  -e KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
  "$SERVICE" bash -lc \
  '
set -euo pipefail
if command -v kafka-console-producer.sh >/dev/null 2>&1; then
  PRODUCER_CMD="kafka-console-producer.sh"
elif command -v kafka-console-producer >/dev/null 2>&1; then
  PRODUCER_CMD="kafka-console-producer"
elif [ -x /opt/kafka/bin/kafka-console-producer.sh ]; then
  PRODUCER_CMD="/opt/kafka/bin/kafka-console-producer.sh"
elif [ -x /opt/kafka/bin/kafka-console-producer ]; then
  PRODUCER_CMD="/opt/kafka/bin/kafka-console-producer"
else
  echo "Could not find kafka-console-producer command inside container." >&2
  exit 127
fi
"$PRODUCER_CMD" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC"
'

echo "Done."
