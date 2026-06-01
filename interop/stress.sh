#!/usr/bin/env bash
# Head-to-head STRESS test: hocuspocu-rs (release) vs @hocuspocus/server 4.1.0.
# Pushes both servers harder than run_bench.sh and samples server-process RSS
# (min/peak/settled) throughout each stage. All simulated clients share one Node
# process, so latency is measured against a single monotonic clock; the server
# is the external process whose RSS we sample.
#
# Stages, per server:
#   A  heavy fan-out latency      200 receivers, 1 doc
#   B1 connection memory (real)   1000 conns sharded over 100 docs, held open
#   B2 awareness storm (worst)    400 conns on ONE doc  (O(N^2) broadcast)
#   C  sustained throughput soak  40 senders, 20s, RSS time-series
#   D  churn / unload hygiene     4x (connect 300 distinct docs -> drop), settle
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUST_BIN="$ROOT/target/release/examples/server"
INTEROP="$ROOT/interop"
RUST_PORT=8088
JS_PORT=8089
TMP="$(mktemp -d)"

rss_kb()   { ps -o rss= -p "$1" 2>/dev/null | tr -d ' '; }
mb()       { awk "BEGIN{printf \"%.1f\", $1/1024}"; }
free_port(){ lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | xargs -r kill 2>/dev/null; sleep 0.3; }

# Run a node client (args via env), sampling server RSS every 0.2s while it runs.
# Echoes "<min_kb> <peak_kb>" and writes the client's BENCH_RESULT to $TMP/out.
run_sampled() {
  local srv_pid="$1"; shift
  : > "$TMP/out"
  ( "$@" > "$TMP/out" 2>"$TMP/err" ) &
  local cp=$! min=99999999 max=0 r
  while kill -0 "$cp" 2>/dev/null; do
    r=$(rss_kb "$srv_pid")
    if [ -n "$r" ]; then
      [ "$r" -gt "$max" ] && max=$r
      [ "$r" -lt "$min" ] && min=$r
    fi
    sleep 0.2
  done
  wait "$cp"
  echo "$min $max"
}

result_line() { grep BENCH_RESULT "$TMP/out" | sed 's/^BENCH_RESULT //'; }

run_suite() {
  local name="$1" port="$2" pid="$3"
  local url="ws://127.0.0.1:$port"
  echo "########################################################"
  echo "# $name   $url   (pid=$pid)"
  echo "########################################################"
  sleep 0.5
  local base; base=$(rss_kb "$pid")
  echo "[baseline] idle RSS = $(mb "$base") MB"

  echo "--- A. heavy fan-out latency (200 receivers, 1 doc) ---"
  read amin amax < <(run_sampled "$pid" env BENCH_MODE=latency HP_URL="$url" \
      RECEIVERS=200 MSGS=300 INTERVAL_MS=2 node "$INTEROP/bench_client.mjs")
  echo "  $(result_line)"
  echo "  server RSS during: $(mb "$amin")–$(mb "$amax") MB"

  echo "--- B1. connection memory: 1000 conns / 100 docs, held 3s ---"
  read bmin bmax < <(run_sampled "$pid" env BENCH_MODE=connect HP_URL="$url" \
      CONN=1000 CONN_DOCS=100 HOLD_MS=3000 node "$INTEROP/bench_client.mjs")
  echo "  $(result_line)"
  echo "  server RSS w/ 1000 live: peak $(mb "$bmax") MB  (Δ over idle $(mb $((bmax-base))) MB)"

  echo "--- B2. awareness storm: 400 conns on ONE doc, held 3s ---"
  read smin smax < <(run_sampled "$pid" env BENCH_MODE=connect HP_URL="$url" \
      CONN=400 CONN_DOCS=1 HOLD_MS=3000 node "$INTEROP/bench_client.mjs")
  echo "  $(result_line)"
  echo "  server RSS w/ 400 on 1 doc: peak $(mb "$smax") MB"

  echo "--- C. sustained throughput soak (40 senders, 20s) ---"
  read cmin cmax < <(run_sampled "$pid" env BENCH_MODE=throughput HP_URL="$url" \
      SENDERS=40 DURATION_MS=20000 node "$INTEROP/bench_client.mjs")
  echo "  $(result_line)"
  echo "  server RSS during soak: $(mb "$cmin")–$(mb "$cmax") MB (peak grows with doc size)"

  echo "--- D. churn / unload hygiene (4x connect 300 distinct docs -> drop) ---"
  local dmax=0 rr
  for round in 1 2 3 4; do
    read _dmin rr < <(run_sampled "$pid" env BENCH_MODE=connect HP_URL="$url" \
        CONN=300 CONN_DOCS=300 node "$INTEROP/bench_client.mjs")
    [ "$rr" -gt "$dmax" ] && dmax=$rr
  done
  echo "  peak RSS across churn rounds: $(mb "$dmax") MB"
  echo "  settling (waiting 8s for store-debounce + unload)..."
  sleep 8
  local settled; settled=$(rss_kb "$pid")
  echo "  settled RSS after all clients gone = $(mb "$settled") MB  (idle baseline was $(mb "$base") MB)"
  echo ""
}

echo "node $(node --version)  |  $(uname -srm)"
echo ""

echo "=========================== RUST ==========================="
free_port "$RUST_PORT"
HP_PORT="$RUST_PORT" "$RUST_BIN" > /tmp/stress_rust.log 2>&1 &
RUST_PID=$!
sleep 2
run_suite "hocuspocu-rs" "$RUST_PORT" "$RUST_PID"
kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null

echo "================= JS (@hocuspocus/server 4.1.0) ================="
free_port "$JS_PORT"
HP_PORT="$JS_PORT" node "$INTEROP/js_server.mjs" > /tmp/stress_js.log 2>&1 &
JS_PID=$!
sleep 2
run_suite "@hocuspocus/server" "$JS_PORT" "$JS_PID"
kill "$JS_PID" 2>/dev/null; wait "$JS_PID" 2>/dev/null

rm -rf "$TMP"
echo "DONE"
