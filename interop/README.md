# Interop & benchmark harness

Proves that `hocuspocu-rs` is wire-compatible with upstream Hocuspocus by driving
it with the **real `@hocuspocus/provider` 4.1.0** client, and benchmarks it
head-to-head against the reference `@hocuspocus/server` 4.1.0.

```sh
npm install
```

## Multi-user interop (the compatibility proof)

```sh
# 1. start a Rust server
cargo build --release -p hocuspocu-rs --example server
HP_PORT=8088 ../target/release/examples/server &

# 2. point N real provider clients at it
HP_URL=ws://127.0.0.1:8088 HP_USERS=8 node multi_user_interop.mjs
```

Checks: initial sync, concurrent Map/Array/Text edits converging (compared by
Yjs state vector), awareness propagation, late-joiner full-state delivery,
same-position text CRDT convergence, and awareness cleanup on disconnect.

The same suite passes unchanged against `node js_server.mjs` (the upstream JS
server), which validates the harness itself.

## Persistence round-trip

```sh
HP_PORT=8090 HP_PERSIST=1 HP_DEBOUNCE=200 ../target/release/examples/server &
HP_URL=ws://127.0.0.1:8090 node persistence_test.mjs
```

Writes a doc, disconnects every client (forcing `on_store_document` + unload),
then reconnects a fresh client and asserts the state was restored via
`on_load_document`.

## Head-to-head benchmark

```sh
bash run_bench.sh
```

Starts each server fresh and runs identical latency / throughput / connection
workloads (`bench_client.mjs`) while sampling server-process RSS. Because every
simulated client lives in one Node process, broadcast latency is measured
against a single monotonic clock.
