#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export AUDIT_TEST_REQUESTS_DSN="${AUDIT_TEST_REQUESTS_DSN:-postgres://requests:requests-local@localhost:55441/requests?sslmode=disable}"
export AUDIT_TEST_CHECKS_DSN="${AUDIT_TEST_CHECKS_DSN:-postgres://checks:checks-local@localhost:55442/checks?sslmode=disable}"
export AUDIT_TEST_REPORTS_DSN="${AUDIT_TEST_REPORTS_DSN:-postgres://reports:reports-local@localhost:55443/reports?sslmode=disable}"
export AUDIT_TEST_BROKERS="${AUDIT_TEST_BROKERS:-localhost:29092}"
export AUDIT_TEST_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export TEST_REQUESTS_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export TEST_CHECKS_DATABASE_URL="$AUDIT_TEST_CHECKS_DSN"
export OUTBOX_TEST_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export TRACE_TEST_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export RELEASE_TEST_DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export DATABASE_URL="$AUDIT_TEST_REQUESTS_DSN"
export KAFKA_BROKERS="$AUDIT_TEST_BROKERS"
formatted=$(find examples -name '*.go' -not -path '*/.tools/*' -exec gofmt -l {} +)
if [ -n "$formatted" ]; then
  echo "$formatted"
  exit 1
fi
while IFS= read -r module; do
  directory=$(dirname "$module")
  echo "Checking $directory"
  if [ -f "$directory/cmd/setup/main.go" ]; then
    (cd "$directory" && go run ./cmd/setup)
  fi
  (cd "$directory" && go test -count=1 -race -v ./...)
done < <(find examples -name go.mod -not -path '*/message-processing/*' -not -path '*/.tools/*' | sort)
