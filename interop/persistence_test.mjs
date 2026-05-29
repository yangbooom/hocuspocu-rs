// Persistence round-trip test. Requires a server started with HP_PERSIST=1 and a
// short HP_DEBOUNCE (e.g. 200). Proves the on_store_document -> unload ->
// on_load_document cycle restores document state for a brand-new client.
//
//   HP_URL=ws://127.0.0.1:8090 node persistence_test.mjs

import * as Y from "yjs";
import { HocuspocusProvider } from "@hocuspocus/provider";
import { WebSocket } from "ws";

const URL = process.env.HP_URL || "ws://127.0.0.1:8090";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(pred, { timeout = 8000, every = 25, label = "" } = {}) {
  const start = Date.now();
  for (;;) {
    if (await pred()) return true;
    if (Date.now() - start > timeout) throw new Error("timeout: " + label);
    await sleep(every);
  }
}
let id = 0;
async function connect(name) {
  const document = new Y.Doc();
  const provider = new HocuspocusProvider({
    url: `${URL}?c=${id++}`,
    name,
    document,
    token: "t",
    WebSocketPolyfill: WebSocket,
  });
  await waitFor(() => provider.synced === true, { timeout: 10000, label: "sync" });
  return { document, provider };
}

const docName = "persist-doc-" + Math.floor(Date.now() / 1000);
let pass = true;

// 1) write, let it persist, then fully disconnect so the doc unloads
const a = await connect(docName);
a.document.transact(() => {
  a.document.getMap("data").set("persisted", "yes");
  a.document.getMap("data").set("count", 42);
  a.document.getText("body").insert(0, "hello persistence");
});
await sleep(700); // > debounce => on_store_document fires
a.provider.destroy();
await sleep(800); // doc has 0 connections => stores + unloads

// 2) brand-new client should be seeded from storage via on_load_document
const b = await connect(docName);
await sleep(400);
const data = b.document.getMap("data");
const body = b.document.getText("body").toString();
const ok =
  data.get("persisted") === "yes" &&
  data.get("count") === 42 &&
  body === "hello persistence";
console.log(
  `${ok ? "✅ PASS" : "❌ FAIL"}  persistence round-trip — persisted=${data.get("persisted")} count=${data.get("count")} body="${body}"`,
);
pass = pass && ok;
b.provider.destroy();
await sleep(200);
console.log("JSON_RESULT " + JSON.stringify({ passed: pass ? 1 : 0, total: 1 }));
process.exit(pass ? 0 : 1);
