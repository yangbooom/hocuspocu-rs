// Multi-user interop test: real @hocuspocus/provider 4.1.0 clients against a
// running hocuspocu-rs server. Each simulated user gets its own WebSocket +
// Y.Doc, exactly like N independent browser tabs. Proves wire-protocol
// compatibility (sync handshake, update broadcast, awareness, late-join).
//
//   HP_URL=ws://127.0.0.1:8088 HP_USERS=5 node multi_user_interop.mjs
//
// Exit code 0 = all checks passed, 1 = a check failed.

import * as Y from "yjs";
import { HocuspocusProvider } from "@hocuspocus/provider";
import { WebSocket } from "ws";

const URL = process.env.HP_URL || "ws://127.0.0.1:8088";
const N = parseInt(process.env.HP_USERS || "5", 10);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred, { timeout = 8000, every = 25, label = "" } = {}) {
  const start = Date.now();
  for (;;) {
    let ok = false;
    try {
      ok = await pred();
    } catch {
      ok = false;
    }
    if (ok) return true;
    if (Date.now() - start > timeout) {
      throw new Error(`timeout waiting for: ${label} (${timeout}ms)`);
    }
    await sleep(every);
  }
}

function sv(doc) {
  return Buffer.from(Y.encodeStateVector(doc)).toString("base64");
}
function allEqual(arr) {
  return arr.every((x) => x === arr[0]);
}

let userCounter = 0;
async function connectUser(docName) {
  const id = userCounter++;
  const document = new Y.Doc();
  // Unique query per user => a fresh, independent WebSocket (the server ignores
  // the query string). This faithfully simulates N separate browser tabs.
  const provider = new HocuspocusProvider({
    url: `${URL}?c=${id}`,
    name: docName,
    document,
    token: "interop-token",
    WebSocketPolyfill: WebSocket,
  });
  await waitFor(() => provider.synced === true, {
    timeout: 10000,
    label: `user ${id} synced`,
  });
  return { id, document, provider };
}

function teardown(users) {
  for (const u of users) {
    try {
      u.provider.destroy();
    } catch {}
  }
}

const results = [];
function record(name, pass, detail) {
  results.push({ name, pass, detail });
  console.log(`${pass ? "✅ PASS" : "❌ FAIL"}  ${name}${detail ? "  — " + detail : ""}`);
}

async function main() {
  console.log(`Interop: ${N} users -> ${URL}`);
  const docName = "interop-doc-" + Math.floor(Date.now() / 1000);

  // ── connect N users ──
  const users = [];
  for (let i = 0; i < N; i++) users.push(await connectUser(docName));
  record("connect + initial sync", users.length === N, `${users.length}/${N} users synced`);

  // ── Test 1: concurrent structured edits converge (Map + Array + Text) ──
  try {
    users.forEach((u, i) => {
      const map = u.document.getMap("map");
      const arr = u.document.getArray("arr");
      const txt = u.document.getText("text");
      u.document.transact(() => {
        map.set(`user${u.id}`, i);
        arr.push([`item-${u.id}`]);
        txt.insert(txt.length, `[${u.id}]`);
      });
    });

    await waitFor(
      () => {
        const svs = users.map((u) => sv(u.document));
        const mapSizes = users.map((u) => u.document.getMap("map").size);
        return allEqual(svs) && mapSizes.every((s) => s === N);
      },
      { timeout: 10000, label: "structured convergence" },
    );

    const svs = users.map((u) => sv(u.document));
    // Canonical (key-order-insensitive) compare; key insertion order legitimately
    // differs per client, so sort keys before comparing.
    const canon = (o) => JSON.stringify(Object.fromEntries(Object.entries(o).sort()));
    const mapJson = canon(users[0].document.getMap("map").toJSON());
    const allMapsSame = users.every(
      (u) => canon(u.document.getMap("map").toJSON()) === mapJson,
    );
    const arrLen = users[0].document.getArray("arr").length;
    record(
      "concurrent Map/Array/Text edits converge",
      allEqual(svs) && allMapsSame && arrLen === N,
      `state-vectors identical=${allEqual(svs)}, maps-equal=${allMapsSame}, map keys=${users[0].document.getMap("map").size}, arr len=${arrLen}`,
    );
  } catch (e) {
    record("concurrent Map/Array/Text edits converge", false, e.message);
  }

  // ── Test 2: awareness propagation ──
  try {
    users.forEach((u) =>
      u.provider.setAwarenessField("user", { id: u.id, name: `user-${u.id}` }),
    );
    await waitFor(
      () => users.every((u) => u.provider.awareness.getStates().size === N),
      { timeout: 8000, label: "awareness propagation" },
    );
    const counts = users.map((u) => u.provider.awareness.getStates().size);
    record(
      "awareness states propagate to all users",
      counts.every((c) => c === N),
      `each user sees ${counts.join("/")} states (expected ${N})`,
    );
  } catch (e) {
    record("awareness states propagate to all users", false, e.message);
  }

  // ── Test 3: late joiner receives full converged state ──
  try {
    const late = await connectUser(docName);
    await waitFor(
      () =>
        sv(late.document) === sv(users[0].document) &&
        late.document.getMap("map").size === N,
      { timeout: 10000, label: "late joiner full state" },
    );
    const ok =
      sv(late.document) === sv(users[0].document) &&
      late.document.getText("text").length === users[0].document.getText("text").length;
    record(
      "late joiner gets full document state",
      ok,
      `late map keys=${late.document.getMap("map").size}, text len=${late.document.getText("text").length}`,
    );
    users.push(late);
  } catch (e) {
    record("late joiner gets full document state", false, e.message);
  }

  // ── Test 4: concurrent Y.Text inserts at same position converge (CRDT) ──
  try {
    const baseLen = users[0].document.getText("text").length;
    users.forEach((u) =>
      u.document.getText("text").insert(0, `<${u.id}>`),
    );
    const expectedAdded = users.reduce((s, u) => s + `<${u.id}>`.length, 0);
    await waitFor(
      () => {
        const svs = users.map((u) => sv(u.document));
        const lens = users.map((u) => u.document.getText("text").length);
        return allEqual(svs) && allEqual(lens);
      },
      { timeout: 10000, label: "text CRDT convergence" },
    );
    const texts = users.map((u) => u.document.getText("text").toString());
    const finalLen = users[0].document.getText("text").length;
    record(
      "concurrent same-position text inserts converge",
      allEqual(texts) && finalLen === baseLen + expectedAdded,
      `all identical=${allEqual(texts)}, len=${finalLen} (expected ${baseLen + expectedAdded})`,
    );
  } catch (e) {
    record("concurrent same-position text inserts converge", false, e.message);
  }

  // ── Test 5: disconnect cleans up awareness on the server ──
  // The server must BROADCAST the departed peer's awareness removal to remaining
  // peers (matching upstream handleAwarenessUpdate). Otherwise ghost cursors
  // linger until each client's ~30s awareness timeout. We give every current
  // user an awareness state first, then remove one that definitely has one.
  try {
    users.forEach((u) =>
      u.provider.setAwarenessField("user", { id: u.id, here: true }),
    );
    const before = users.length;
    await waitFor(
      () => users.every((u) => u.provider.awareness.getStates().size === before),
      { timeout: 8000, label: "all users have awareness" },
    );
    const victim = users.shift(); // an original user that definitely has awareness
    const expected = users.length;
    victim.provider.destroy();
    await waitFor(
      () => users.every((u) => u.provider.awareness.getStates().size === expected),
      { timeout: 9000, label: "awareness cleanup on disconnect" },
    );
    const counts = users.map((u) => u.provider.awareness.getStates().size);
    record(
      "server broadcasts awareness removal on disconnect",
      counts.every((c) => c === expected),
      `remaining users see ${counts.join("/")} states (expected ${expected})`,
    );
  } catch (e) {
    record("server broadcasts awareness removal on disconnect", false, e.message);
  }

  // ── Test 6: UNCLEAN disconnect (network drop) also clears awareness ──
  // A clean provider.destroy() sends its own awareness-removal, so it can mask a
  // server that fails to announce departures. The real test is an abrupt socket
  // kill (no client-sent removal) — only the SERVER can clear the ghost, exactly
  // as upstream does. Requires >=2 remaining users.
  try {
    users.forEach((u) =>
      u.provider.setAwarenessField("user", { id: u.id, here: true }),
    );
    const before = users.length;
    await waitFor(
      () => users.every((u) => u.provider.awareness.getStates().size === before),
      { timeout: 8000, label: "all users have awareness (unclean)" },
    );
    const victim = users.shift();
    const expected = users.length;
    // Abruptly terminate the underlying socket without provider.destroy(), and
    // stop reconnection, so no client-side awareness-null is ever sent.
    const wsp = victim.provider.configuration.websocketProvider;
    wsp.shouldConnect = false;
    try {
      wsp.webSocket.terminate();
    } catch {
      try {
        wsp.webSocket.close();
      } catch {}
    }
    await waitFor(
      () => users.every((u) => u.provider.awareness.getStates().size === expected),
      { timeout: 9000, label: "awareness cleared after unclean disconnect" },
    );
    const counts = users.map((u) => u.provider.awareness.getStates().size);
    record(
      "server clears awareness after UNCLEAN disconnect (network drop)",
      counts.every((c) => c === expected),
      `remaining users see ${counts.join("/")} states (expected ${expected})`,
    );
  } catch (e) {
    record(
      "server clears awareness after UNCLEAN disconnect (network drop)",
      false,
      e.message,
    );
  }

  teardown(users);
  await sleep(200);

  const failed = results.filter((r) => !r.pass);
  console.log(
    `\n=== INTEROP SUMMARY: ${results.length - failed.length}/${results.length} passed ===`,
  );
  console.log("JSON_RESULT " + JSON.stringify({ passed: results.length - failed.length, total: results.length, results }));
  process.exit(failed.length === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error("HARNESS ERROR:", e);
  process.exit(2);
});
