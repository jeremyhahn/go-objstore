#!/usr/bin/env bash
# Builds and starts a local objstore-server with the REST, MCP (HTTP), and
# Unix-socket transports enabled against a temp-dir local backend, for SDK
# e2e smoke tests. Prints the connection details as shell exports and writes
# the server PID to $PID_FILE (default /tmp/objstore-smoke.pid).
#
# Usage:
#   eval "$(./scripts/start-test-server.sh)"   # exports SMOKE_* vars
#   ... run smoke tests ...
#   kill "$(cat /tmp/objstore-smoke.pid)"
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${PID_FILE:-/tmp/objstore-smoke.pid}"
REST_PORT="${REST_PORT:-18080}"
MCP_PORT="${MCP_PORT:-18081}"
UNIX_SOCK="${UNIX_SOCK:-/tmp/objstore-smoke.sock}"
STORAGE_DIR="$(mktemp -d /tmp/objstore-smoke-XXXXXX)"
BIN="$(mktemp -d /tmp/objstore-smoke-bin-XXXXXX)/objstore-server"

(cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/objstore-server) >&2

rm -f "$UNIX_SOCK"
"$BIN" \
  --backend local --path "$STORAGE_DIR" \
  --grpc=false --quic=false \
  --rest --rest-port "$REST_PORT" \
  --mcp --mcp-mode http --mcp-addr "127.0.0.1:$MCP_PORT" \
  --unix --unix-socket "$UNIX_SOCK" >&2 &
echo $! > "$PID_FILE"

# Wait for readiness.
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$REST_PORT/health" >/dev/null 2>&1 && [ -S "$UNIX_SOCK" ]; then
    break
  fi
  sleep 0.1
done

echo "export SMOKE_REST_ADDR=127.0.0.1:$REST_PORT"
echo "export SMOKE_MCP_ADDR=127.0.0.1:$MCP_PORT"
echo "export SMOKE_UNIX_SOCK=$UNIX_SOCK"
