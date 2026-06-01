#!/usr/bin/env bash
# Head-to-head benchmark: hocuspocu-rs (release) vs @hocuspocus/server 4.1.0.
# Runs identical workloads against fresh instances of each server and samples
# server-process RSS. Prints BENCH_RESULT json lines + RSS markers.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUST_BIN="$ROOT/target/release/examples/server"
INTEROP="$ROOT/interop"
RUST_PORT=8088
JS_PORT=8089

rss_kb() { ps -o rss= -p "$1" 2>/dev/null | tr -d ' '; }
free_port() { lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | xargs -r kill 2>/dev/null; sleep 0.3; }

run_suite() {
  local name="$1" port="$2" pid="$3"
  local url="ws://127.0.0.1:$port"
  echo "########## $name  $url  (pid=$pid) ##########"
  sleep 0.5
  echo "MARK idle_rss_kb=$(rss_kb "$pid")"

  for R in 10 50 100; do
    BENCH_MODE=latency HP_URL="$url" RECEIVERS="$R" MSGS=300 INTERVAL_MS=4 \
      node "$INTEROP/bench_client.mjs" 2>/dev/null | grep BENCH_RESULT
  done

  # throughput with peak-RSS sampling
  ( BENCH_MODE=throughput HP_URL="$url" SENDERS=20 DURATION_MS=5000 \
      node "$INTEROP/bench_client.mjs" 2>/dev/null | grep BENCH_RESULT ) &
  local bp=$! maxrss=0 r
  while kill -0 "$bp" 2>/dev/null; do
    r=$(rss_kb "$pid"); [ -n "$r" ] && [ "$r" -gt "$maxrss" ] && maxrss=$r
    sleep 0.2
  done
  wait "$bp"
  echo "MARK throughput_peak_rss_kb=$maxrss"

  # connection scaling with peak-RSS sampling
  ( BENCH_MODE=connect HP_URL="$url" CONN=200 \
      node "$INTEROP/bench_client.mjs" 2>/dev/null | grep BENCH_RESULT ) &
  bp=$!; maxrss=0
  while kill -0 "$bp" 2>/dev/null; do
    r=$(rss_kb "$pid"); [ -n "$r" ] && [ "$r" -gt "$maxrss" ] && maxrss=$r
    sleep 0.2
  done
  wait "$bp"
  echo "MARK connect_peak_rss_kb=$maxrss"
  echo "MARK final_rss_kb=$(rss_kb "$pid")"
}

echo "node $(node --version)"
echo "===== RUST ====="
free_port "$RUST_PORT"
HP_PORT="$RUST_PORT" "$RUST_BIN" > /tmp/bench_rust.log 2>&1 &
RUST_PID=$!
sleep 2
run_suite "hocuspocu-rs" "$RUST_PORT" "$RUST_PID"
kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null

echo ""
echo "===== JS (@hocuspocus/server 4.1.0) ====="
free_port "$JS_PORT"
HP_PORT="$JS_PORT" node "$INTEROP/js_server.mjs" > /tmp/bench_js.log 2>&1 &
JS_PID=$!
sleep 2
run_suite "@hocuspocus/server" "$JS_PORT" "$JS_PID"
kill "$JS_PID" 2>/dev/null; wait "$JS_PID" 2>/dev/null

echo ""
echo "DONE"
