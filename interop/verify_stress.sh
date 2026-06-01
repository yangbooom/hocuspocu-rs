#!/usr/bin/env bash
# Focused re-measurement of the contradictory stress findings, repeated to
# separate signal from noise: fan-out latency, per-connection memory, throughput.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUST_BIN="$ROOT/target/release/examples/server"
INTEROP="$ROOT/interop"
TMP="$(mktemp -d)"
rss_kb(){ ps -o rss= -p "$1" 2>/dev/null | tr -d ' '; }
mb(){ awk "BEGIN{printf \"%.1f\", $1/1024}"; }
free_port(){ lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | xargs kill 2>/dev/null; sleep 0.3; }

run_sampled(){ local pid="$1"; shift; : >"$TMP/out"; ( "$@" >"$TMP/out" 2>/dev/null ) & local cp=$! mx=0 r
  while kill -0 "$cp" 2>/dev/null; do r=$(rss_kb "$pid"); [ -n "$r" ]&&[ "$r" -gt "$mx" ]&&mx=$r; sleep 0.2; done
  wait "$cp"; echo "$mx"; }
# Safe nested-field reader: splits "a.b" on dots and walks the object (no eval).
jget(){ grep BENCH_RESULT "$TMP/out" | sed 's/^BENCH_RESULT //' | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{let o=JSON.parse(s);for(const k of process.argv[1].split('.'))o=o[k];console.log(o)})" "$1"; }

suite(){ local name="$1" port="$2" pid="$3"; local url="ws://127.0.0.1:$port"
  sleep 0.5; local base; base=$(rss_kb "$pid")
  echo "### $name  (idle $(mb "$base") MB)"
  echo -n "  latency p50/p99 (200 rcv) x3: "
  for i in 1 2 3; do run_sampled "$pid" env BENCH_MODE=latency HP_URL="$url" RECEIVERS=200 MSGS=300 INTERVAL_MS=2 node "$INTEROP/bench_client.mjs" >/dev/null
    echo -n "[$(jget 'latency_ms.p50')/$(jget 'latency_ms.p99')] "; done; echo
  echo -n "  conn mem 800/80docs Δ-per-conn x2: "
  for i in 1 2; do mx=$(run_sampled "$pid" env BENCH_MODE=connect HP_URL="$url" CONN=800 CONN_DOCS=80 HOLD_MS=2500 node "$INTEROP/bench_client.mjs")
    echo -n "[peak $(mb "$mx")MB, $(( (mx - base) * 1024 / 800 ))KB/conn] "; sleep 1; done; echo
  echo -n "  throughput routed/s (40 snd,12s) x2: "
  for i in 1 2; do run_sampled "$pid" env BENCH_MODE=throughput HP_URL="$url" SENDERS=40 DURATION_MS=12000 node "$INTEROP/bench_client.mjs" >/dev/null
    echo -n "[$(jget 'routed_per_sec')] "; done; echo
}

echo "node $(node --version)"
free_port 8088; HP_PORT=8088 "$RUST_BIN" >/tmp/vs_rust.log 2>&1 & RP=$!; sleep 2
suite "hocuspocu-rs" 8088 "$RP"; kill "$RP" 2>/dev/null; wait "$RP" 2>/dev/null
free_port 8089; HP_PORT=8089 node "$INTEROP/js_server.mjs" >/tmp/vs_js.log 2>&1 & JP=$!; sleep 2
suite "@hocuspocus/server" 8089 "$JP"; kill "$JP" 2>/dev/null; wait "$JP" 2>/dev/null
rm -rf "$TMP"; echo DONE