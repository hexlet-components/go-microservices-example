#!/usr/bin/env bash
set -euo pipefail
docker compose up -d --wait
for topic in audit-created audit-checked; do
  docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done
