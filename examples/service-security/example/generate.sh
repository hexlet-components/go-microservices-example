#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
case "$(uname -s)/$(uname -m)" in
  Darwin/arm64) platform=osx-aarch_64 ;;
  Darwin/x86_64) platform=osx-x86_64 ;;
  Linux/aarch64|Linux/arm64) platform=linux-aarch_64 ;;
  Linux/x86_64) platform=linux-x86_64 ;;
  *) echo 'generation supports macOS and Linux on arm64/x86_64' >&2; exit 1 ;;
esac
mkdir -p .tools
if [ ! -x .tools/protoc/bin/protoc ]; then
  curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v36.2/protoc-36.2-${platform}.zip" -o .tools/protoc.zip
  unzip -q .tools/protoc.zip -d .tools/protoc
fi
GOBIN="$PWD/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$PWD/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
.tools/protoc/bin/protoc \
  --plugin=protoc-gen-go="$PWD/.tools/protoc-gen-go" \
  --plugin=protoc-gen-go-grpc="$PWD/.tools/protoc-gen-go-grpc" \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  reports/v1/reports.proto
