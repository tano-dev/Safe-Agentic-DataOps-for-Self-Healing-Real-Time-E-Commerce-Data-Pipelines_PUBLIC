#!/usr/bin/env bash
set -euo pipefail

BROKER="127.0.0.1:9092"
TOPICS=("events-raw" "events-processed" "incidents")

for topic in "${TOPICS[@]}"; do
  echo "Đang tạo topic: ${topic}"
  docker exec redpanda rpk topic create "${topic}" \
    --brokers "${BROKER}" \
    --partitions 3 \
    --replicas 1 \
    || echo "  (đã tồn tại, bỏ qua)"
done

echo ""
echo "List topic hien co:"
docker exec redpanda rpk topic list --brokers "${BROKER}"
