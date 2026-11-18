#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export AUDIT_TEST_REQUESTS_DSN="${AUDIT_TEST_REQUESTS_DSN:-postgres://requests:requests-local@localhost:55441/requests?sslmode=disable}"
export AUDIT_TEST_CHECKS_DSN="${AUDIT_TEST_CHECKS_DSN:-postgres://checks:checks-local@localhost:55442/checks?sslmode=disable}"
export AUDIT_TEST_REPORTS_DSN="${AUDIT_TEST_REPORTS_DSN:-postgres://reports:reports-local@localhost:55443/reports?sslmode=disable}"
export AUDIT_TEST_BROKERS="${AUDIT_TEST_BROKERS:-localhost:29092}"
export AUDIT_TEST_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export OUTBOX_TEST_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export KAFKA_BROKERS="$AUDIT_TEST_BROKERS"
export KAFKA_BROKER="$AUDIT_TEST_BROKERS"
for module in basic rpc events isolation consumer outbox failures compatibility migration; do
  echo "Checking $module"
  (cd "$module" && go test -count=1 -race -v ./...)
done
