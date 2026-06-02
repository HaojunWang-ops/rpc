#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"

SERVER_BIN="$BUILD_DIR/examples/provider"
CLIENT_BIN="$BUILD_DIR/examples/consumer"

SERVER_LOG="$BUILD_DIR/server.log"
CLIENT_LOG="$BUILD_DIR/client.log"

SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "[run.sh] stop server, pid=${SERVER_PID}"
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

echo "[run.sh] configure project..."
cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Debug

echo "[run.sh] build project..."
cmake --build "$BUILD_DIR" -j"$(nproc)"

: > "$SERVER_LOG"
: > "$CLIENT_LOG"

echo "[run.sh] start server..."
"$SERVER_BIN" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
echo "[run.sh] server pid: $SERVER_PID"

sleep 1

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "[run.sh] server failed to start"
    echo "========== server.log =========="
    sed 's/^/[server] /' "$SERVER_LOG"
    exit 1
fi

echo "[run.sh] run client..."
"$CLIENT_BIN" > "$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!
echo "[run.sh] client pid: $CLIENT_PID"

wait "$CLIENT_PID" || {
    echo "[run.sh] client failed"
    echo "========== client.log =========="
    cat "$CLIENT_LOG"
    echo "========== server.log =========="
    cat "$SERVER_LOG"
    exit 1
}
##运行结束后
echo
echo "========== client.log =========="
sed 's/^/[client] /' "$CLIENT_LOG"

echo
echo "========== server.log =========="
sed 's/^/[server] /' "$SERVER_LOG"
echo 