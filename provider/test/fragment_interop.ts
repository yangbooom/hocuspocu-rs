// End-to-end fragmentation interop test.
//
// Proves the in-repo TypeScript provider and the Rust server fragment +
// reassemble in BOTH directions:
//   - INBOUND  (client -> server): client A writes a payload larger than the
//     chunk size, so the provider splits the update into FragmentStart/Data/End
//     frames that the Rust server must reassemble.
//   - OUTBOUND (server -> client): client B connects fresh; the server (started
//     with HP_CHUNK) chunks the full document state it sends back, which the
//     provider's FragmentBuffer must reassemble.
//
// Run from inside provider/ so yjs/ws/provider all resolve to the SAME
// node_modules (a second yjs copy silently breaks sync):
//   HP_URL=ws://127.0.0.1:8088 CHUNK=1024 npx tsx test/fragment_interop.ts
//
// Exits 0 with "OK: ..." on convergence, 1 with a clear FAIL otherwise.

import * as Y from 'yjs';
import { WebSocket } from 'ws';

import { HocuspocusProvider } from '../HocuspocusProvider';
import { HocuspocusProviderWebsocket } from '../HocuspocusProviderWebsocket';

const HP_URL = process.env.HP_URL ?? 'ws://127.0.0.1:8088';
const CHUNK = Number(process.env.CHUNK ?? '1024');
const DOC_NAME = 'fragment-doc';
const TIMEOUT_MS = 10_000;

// A payload comfortably larger than the chunk size forces inbound fragmentation.
const EXPECTED_LEN = CHUNK * 4;

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function makeClient(doc: Y.Doc) {
  const socket = new HocuspocusProviderWebsocket({
    url: HP_URL,
    // Inject the Node WebSocket implementation from `ws`.
    WebSocketPolyfill: WebSocket,
  });

  const provider = new HocuspocusProvider({
    websocketProvider: socket,
    name: DOC_NAME,
    document: doc,
    token: 'interop',
    messageChunkSize: CHUNK,
  });

  // When a websocketProvider is supplied, the provider does NOT manage the
  // socket itself, so it must be attached explicitly to start the handshake
  // (see canonical usage: wsProvider.attach()).
  provider.attach();

  return { socket, provider };
}

async function waitForSynced(provider: HocuspocusProvider, label: string): Promise<void> {
  if (provider.isSynced) return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${label} did not sync within ${TIMEOUT_MS}ms`)),
      TIMEOUT_MS
    );
    provider.on('synced', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function main() {
  console.log(
    `fragment interop: url=${HP_URL} chunk=${CHUNK} expectedLen=${EXPECTED_LEN}`
  );

  const docA = new Y.Doc();
  const { socket: socketA, provider: providerA } = makeClient(docA);

  // Wait for the first handshake so the server has loaded the (empty) doc.
  await waitForSynced(providerA, 'client A');

  // INBOUND fragmentation: a single large insert -> one big update message
  // that the provider chunks into FragmentStart/Data*/End frames.
  const big = 'x'.repeat(EXPECTED_LEN);
  docA.getText('t').insert(0, big);

  // Now connect client B with a fresh doc. On its handshake the server sends
  // the full state, which it chunks OUTBOUND because it was started with HP_CHUNK.
  const docB = new Y.Doc();
  const { socket: socketB, provider: providerB } = makeClient(docB);
  await waitForSynced(providerB, 'client B');

  // Poll for convergence rather than relying on a single fixed sleep.
  const deadline = Date.now() + TIMEOUT_MS;
  let lenA = 0;
  let lenB = 0;
  while (Date.now() < deadline) {
    lenA = docA.getText('t').length;
    lenB = docB.getText('t').length;
    if (lenA === EXPECTED_LEN && lenB === EXPECTED_LEN) {
      break;
    }
    await sleep(50);
  }

  // Tear down before deciding the result so the process can exit cleanly.
  providerA.destroy();
  providerB.destroy();
  socketA.destroy();
  socketB.destroy();

  if (lenA === EXPECTED_LEN && lenB === EXPECTED_LEN) {
    // Verify the actual content matches too, not just the length.
    if (docA.getText('t').toString() !== big || docB.getText('t').toString() !== big) {
      console.error('FAIL: lengths matched but content differed');
      process.exit(1);
    }
    console.log(
      `OK: both clients converged on ${EXPECTED_LEN} chars through fragmentation`
    );
    process.exit(0);
  }

  console.error(
    `FAIL: did not converge within ${TIMEOUT_MS}ms ` +
      `(expected ${EXPECTED_LEN}, got A=${lenA} B=${lenB})`
  );
  process.exit(1);
}

main().catch(err => {
  console.error(`FAIL: ${err instanceof Error ? err.stack ?? err.message : String(err)}`);
  process.exit(1);
});
