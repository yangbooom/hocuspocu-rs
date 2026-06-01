// Head-to-head load client. Drives ANY hocuspocus server (Rust or JS) through
// one of three benchmarks and prints a `BENCH_RESULT {json}` line. The server
// process is external; the *driver* script samples its RSS. Because every
// simulated client lives in this one Node process, broadcast latency is
// measured against a single monotonic clock (performance.now()).
//
//   BENCH_MODE=latency    HP_URL=ws://127.0.0.1:8088 RECEIVERS=50 MSGS=500 INTERVAL_MS=4 node bench_client.mjs
//   BENCH_MODE=throughput HP_URL=ws://127.0.0.1:8088 SENDERS=20 DURATION_MS=5000          node bench_client.mjs
//   BENCH_MODE=connect    HP_URL=ws://127.0.0.1:8088 CONN=200                              node bench_client.mjs

import * as Y from "yjs";
import { HocuspocusProvider } from "@hocuspocus/provider";
import { WebSocket } from "ws";
import { performance } from "node:perf_hooks";

const URL = process.env.HP_URL || "ws://127.0.0.1:8088";
const MODE = process.env.BENCH_MODE || "latency";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred, { timeout = 30000, every = 10, label = "" } = {}) {
  const start = performance.now();
  for (;;) {
    if (await pred()) return true;
    if (performance.now() - start > timeout)
      throw new Error(`timeout: ${label}`);
    await sleep(every);
  }
}

let uc = 0;
function connect(docName) {
  const id = uc++;
  const document = new Y.Doc();
  // Unique query per client => independent WebSocket (server ignores the query).
  const provider = new HocuspocusProvider({
    url: `${URL}?c=${id}`,
    name: docName,
    document,
    token: "bench",
    WebSocketPolyfill: WebSocket,
  });
  return { id, document, provider };
}
async function connectSynced(docName) {
  const c = connect(docName);
  await waitFor(() => c.provider.synced === true, {
    timeout: 30000,
    label: `sync user ${c.id}`,
  });
  return c;
}
function destroyAll(cs) {
  for (const c of cs) {
    try { c.provider.destroy(); } catch {}
  }
}
function pct(sorted, p) {
  if (!sorted.length) return null;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i];
}
function stats(arr) {
  if (!arr.length) return { n: 0 };
  const s = [...arr].sort((a, b) => a - b);
  const sum = s.reduce((a, b) => a + b, 0);
  return {
    n: s.length,
    mean: +(sum / s.length).toFixed(3),
    p50: +pct(s, 50).toFixed(3),
    p95: +pct(s, 95).toFixed(3),
    p99: +pct(s, 99).toFixed(3),
    max: +s[s.length - 1].toFixed(3),
  };
}
function emit(obj) {
  console.log("BENCH_RESULT " + JSON.stringify(obj));
}

// ── latency: 1 sender pushes MSGS seqs; R receivers measure emit->receive ──
async function benchLatency() {
  const RECEIVERS = parseInt(process.env.RECEIVERS || "50", 10);
  const MSGS = parseInt(process.env.MSGS || "500", 10);
  const INTERVAL_MS = parseFloat(process.env.INTERVAL_MS || "4");
  const docName = "bench-lat-" + Math.floor(performance.now());

  const sender = await connectSynced(docName);
  const receivers = [];
  for (let i = 0; i < RECEIVERS; i++) receivers.push(await connectSynced(docName));

  const sendTimes = new Map(); // seq -> emit time
  const latencies = [];
  for (const r of receivers) {
    r._seen = 0;
    const arr = r.document.getArray("lat");
    arr.observe(() => {
      const len = arr.length;
      for (let i = r._seen; i < len; i++) {
        const seq = arr.get(i);
        const t0 = sendTimes.get(seq);
        if (t0 !== undefined) latencies.push(performance.now() - t0);
      }
      r._seen = len;
    });
  }

  await sleep(250); // ensure observers + sync settled
  const senderArr = sender.document.getArray("lat");
  const t0 = performance.now();
  for (let s = 0; s < MSGS; s++) {
    sendTimes.set(s, performance.now());
    senderArr.push([s]);
    if (INTERVAL_MS > 0) await sleep(INTERVAL_MS);
  }
  await waitFor(
    () => receivers.every((r) => r.document.getArray("lat").length >= MSGS),
    { timeout: 60000, label: "all receivers drained" },
  );
  const wall = performance.now() - t0;

  emit({
    mode: "latency",
    url: URL,
    receivers: RECEIVERS,
    msgs: MSGS,
    interval_ms: INTERVAL_MS,
    fanout_samples: latencies.length,
    wall_ms: +wall.toFixed(1),
    latency_ms: stats(latencies),
  });
  destroyAll([sender, ...receivers]);
}

// ── throughput: SENDERS hammer updates for DURATION; count routed ops ──
async function benchThroughput() {
  const SENDERS = parseInt(process.env.SENDERS || "20", 10);
  const DURATION_MS = parseInt(process.env.DURATION_MS || "5000", 10);
  const docName = "bench-tput-" + Math.floor(performance.now());

  const senders = [];
  for (let i = 0; i < SENDERS; i++) senders.push(await connectSynced(docName));
  // one observer counts total elements routed back across all senders' arrays
  const observer = await connectSynced(docName);
  let received = 0;
  const oArr = observer.document.getArray("tput");
  let oSeen = 0;
  oArr.observe(() => {
    received += oArr.length - oSeen;
    oSeen = oArr.length;
  });

  await sleep(200);
  let sent = 0;
  let running = true;
  const t0 = performance.now();
  const loops = senders.map(async (s) => {
    const arr = s.document.getArray("tput");
    let i = 0;
    while (running) {
      arr.push([s.id * 1e7 + i]);
      i++;
      sent++;
      if ((i & 31) === 0) await sleep(0); // yield to event loop
    }
  });
  await sleep(DURATION_MS);
  running = false;
  await Promise.all(loops);
  // allow tail of broadcasts to arrive
  await waitFor(() => received >= sent * 0.999, { timeout: 15000, label: "tput drain" }).catch(() => {});
  const wall = (performance.now() - t0) / 1000;

  emit({
    mode: "throughput",
    url: URL,
    senders: SENDERS,
    duration_ms: DURATION_MS,
    ops_sent: sent,
    ops_received_by_observer: received,
    sent_per_sec: Math.round(sent / wall),
    routed_per_sec: Math.round(received / wall),
  });
  destroyAll([...senders, observer]);
}

// ── connect: time to connect + sync CONN clients ──
// CONN_DOCS shards the clients across that many documents (default 1). Sharding
// isolates pure connection/memory cost from the O(N^2) awareness storm you get
// when every client joins one document.
async function benchConnect() {
  const CONN = parseInt(process.env.CONN || "200", 10);
  const CONN_DOCS = Math.max(1, parseInt(process.env.CONN_DOCS || "1", 10));
  const HOLD_MS = parseInt(process.env.HOLD_MS || "0", 10);
  const base = "bench-conn-" + Math.floor(performance.now());
  const docFor = (i) => (CONN_DOCS === 1 ? base : `${base}-${i % CONN_DOCS}`);
  const t0 = performance.now();
  const clients = [];
  // connect in waves to avoid SYN floods skewing the result
  const WAVE = 25;
  for (let i = 0; i < CONN; i += WAVE) {
    const batch = [];
    for (let j = i; j < Math.min(CONN, i + WAVE); j++) batch.push(connect(docFor(j)));
    await Promise.all(
      batch.map((c) =>
        waitFor(() => c.provider.synced === true, { timeout: 30000, label: `sync ${c.id}` }),
      ),
    );
    clients.push(...batch);
  }
  const wall = performance.now() - t0;
  emit({
    mode: "connect",
    url: URL,
    connections: CONN,
    docs: CONN_DOCS,
    total_ms: +wall.toFixed(1),
    per_conn_ms: +(wall / CONN).toFixed(3),
    conn_per_sec: Math.round(CONN / (wall / 1000)),
  });
  // Optionally hold the connections open (so the driver can sample steady-state
  // RSS with all clients live) before tearing them down.
  if (HOLD_MS > 0) await sleep(HOLD_MS);
  destroyAll(clients);
}

const run =
  MODE === "throughput"
    ? benchThroughput
    : MODE === "connect"
      ? benchConnect
      : benchLatency;

run()
  .then(async () => {
    await sleep(150);
    process.exit(0);
  })
  .catch((e) => {
    console.error("BENCH ERROR:", e);
    process.exit(1);
  });
