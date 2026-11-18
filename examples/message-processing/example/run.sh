#!/usr/bin/env bash
set -euo pipefail
export JWT_SECRET="${JWT_SECRET:-local-audit-demo-secret-at-least-32-bytes}"
case "${1:-}" in
  api) export DATABASE_URL='postgres://requests:requests-local@localhost:55431/requests?sslmode=disable' ;;
  checks) export DATABASE_URL='postgres://checks:checks-local@localhost:55432/checks?sslmode=disable' ;;
  reports) export DATABASE_URL='postgres://reports:reports-local@localhost:55433/reports?sslmode=disable' ;;
  site|token) ;;
  *) echo 'usage: bash run.sh api|checks|reports|site|token' >&2; exit 1 ;;
esac
go run ./cmd/audit -role "$1"
