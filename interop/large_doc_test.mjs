// Safety check for the 16 KiB read-buffer change: a document far larger than the
// buffer must still sync intact (the initial SyncStep2 is one big message read in
// many chunks). Writer fills a Y.Text with ~1 MB, a fresh reader must receive all.
import * as Y from "yjs";
import { HocuspocusProvider } from "@hocuspocus/provider";
import { WebSocket } from "ws";

const URL = process.env.HP_URL || "ws://127.0.0.1:8091";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const waitSync = (p) => new Promise((res, rej) => {
  if (p.synced) return res();
  p.on("synced", () => res());
  setTimeout(() => rej(new Error("sync timeout")), 30000);
});

const BIG = 1_000_000; // ~1 MB, >> 16 KiB read buffer
const docName = "large-doc-" + Date.now();

const wd = new Y.Doc();
const wp = new HocuspocusProvider({ url: `${URL}?w`, name: docName, document: wd, token: "t", WebSocketPolyfill: WebSocket });
await waitSync(wp);
wd.getText("t").insert(0, "x".repeat(BIG));
const wlen = wd.getText("t").length;
await sleep(800); // let the big update flush to the server

const rd = new Y.Doc();
const rp = new HocuspocusProvider({ url: `${URL}?r`, name: docName, document: rd, token: "t", WebSocketPolyfill: WebSocket });
await waitSync(rp);
await sleep(500);
const rlen = rd.getText("t").length;

const ok = rlen === wlen && wlen === BIG;
console.log(`writer text len = ${wlen}, reader text len = ${rlen}`);
console.log(ok ? `✅ PASS large-doc sync (${(BIG/1024).toFixed(0)} KiB >> 16 KiB buffer, intact)`
               : `❌ FAIL large-doc sync (mismatch)`);
wp.destroy(); rp.destroy();
process.exit(ok ? 0 : 1);
