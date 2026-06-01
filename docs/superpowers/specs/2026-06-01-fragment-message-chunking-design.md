# Fragment message chunking — design

- **Date:** 2026-06-01
- **Status:** Approved + grilled — ready for implementation plan
- **Branch:** `worktree-fragment-chunking`

## Background & motivation

hocuspocu-rs is a wire-compatible Rust port of Hocuspocus. In a previous TypeScript
fork (walla-next), an application-level **message fragmentation** protocol was added to
both the server (`@hocuspocus/server` fork) and the client (`@hocuspocus/provider` fork):
large WebSocket messages are split into a series of small messages and reassembled on the
other side.

The reason is concrete and not theoretical: when the product was supplied to a customer,
**their corporate network (an intermediary proxy/firewall/load balancer) terminated the
WebSocket connection whenever a single WebSocket message was large.** That intermediary is
not under our control, so raising any receive-side `max_message_size` / `maxPayload` does
not help — those only *reject* oversized messages; they do not keep messages small across
an uncontrollable hop. WebSocket protocol-level frame fragmentation also does not help,
because the intermediary chokes on the *logical* message size. The only fix is to split one
logical Yjs message into **multiple small WebSocket messages** at the application layer and
reassemble them — which is exactly what the fork did, with a 60 KB chunk size that the
customer's network tolerated.

This design ports that capability to hocuspocu-rs (bidirectional) and brings a matching
TypeScript provider into this repository so the protocol is defined and tested in one place.

### Relationship to "max message size" (not redundant)

| Mechanism | Layer | Effect |
| --- | --- | --- |
| `max_message_size` / `maxPayload` | WS library receive guard | **Rejects** messages over a cap (anti-DoS). Does not split. |
| WebSocket protocol framing | Transport | Transparently splits one message into TCP frames, reassembled into **one** message. Cannot bypass an intermediary's per-message limit. |
| **Fragment protocol (this design)** | Application | Splits one logical message into **separate small WS messages**, reassembled by the app. The only way under an uncontrollable intermediary cap. |

## Goals

- Bidirectional application-level fragmentation in hocuspocu-rs (server splits large
  outbound messages; server reassembles fragmented inbound messages).
- **Opt-in:** off by default; identical to upstream behavior unless a chunk size is set.
- Outbound chunking configured **per-connection via a hook** (mirrors the fork's
  `connectionConfig.messageChunkSize`).
- A matching TypeScript provider in-repo, byte-compatible with the Rust server.

## Non-goals

- Implementing real application-level Ping/Pong behavior (`MessageType::Ping=9`/`Pong=10`
  stay *defined* for upstream compatibility but remain unused, as today).
- A compile-time cargo feature gate — fragmentation is always compiled in and gated at
  runtime (see "Feature gating").
- A global `Configuration`-level default chunk size — per-connection via hook only (a
  global default can be added trivially later if it is ever wanted).
- Publishing the TS provider to npm (in-repo source-of-truth + interop client, for now).
- Changing how the existing (already deployed) walla clients talk to the *old* JS server.

## Protocol

### Message types

Upstream Hocuspocus defines `MessageType` values `0..=10` (`Sync=0 … SyncStatus=8`,
`Ping=9`, `Pong=10`). The walla fork was based on a 3.x line (ending at `SyncStatus=8`) and
placed fragments at `10/11/12`, which collides with upstream's later `Pong=10`.

To stay upstream-compatible **and** support fragments, we move the fragment types into a
clearly separated block above the upstream range (all values ≤ 127, so each is still a
single varint byte — no wire-size cost):

```
FragmentStart = 100
FragmentData  = 101
FragmentEnd   = 102
```

`Ping=9` / `Pong=10` are kept as-is.

### Frame formats

Every frame keeps the existing envelope `[var_string address][var_uint type][payload…]`,
where `address` is the connection's message address (`document_name`, or
`document_name\0session_id` when multiplexing).

- **FragmentStart:** `[address][100][var_string unique_id]`
- **FragmentData:** `[address][101][var_string unique_id][var_uint index][var_uint8array chunk]`
- **FragmentEnd:** `[address][102][var_string unique_id]`

`unique_id` is a freshly generated UUID v4 per fragmented message (the `uuid` crate is
already a dependency; replaces the fork's `Math.random()` id — the value is opaque to the
receiver). The fragment frame's `address` is the **same first var_string as the original
message** (verified: the fork provider's `MessageSender` uses `peekVarString` of the
original message, which is the full multiplexed address). The server already splits
`address` on `\0` to recover `document_name`, so multiplexed addresses round-trip.

### Chunking semantics

`message_chunk_size` is the **payload slice size** (fork parity): the chunk bytes are
slices of the **entire original frame** (including its `address` prefix), cut at
`chunk_size`. Reassembling the chunks in `index` order reproduces the exact original frame;
the receiver then strips the `address` prefix and dispatches it through the normal message
path. Each `FragmentData` frame adds a small header (`address` + type + `unique_id` + index
+ length prefix ≈ 80 B + address) on top of each `chunk_size`-byte slice, so the **emitted
WS message is slightly larger than `chunk_size`**. The configured size must therefore be
chosen with margin below the intermediary's true limit (the fork used 60 KB for a network
that broke above ~64 KB). This will be documented for operators.

> **Fork bug we will *not* reproduce:** the JS server's `Connection.send` sends a small
> message (`≤ chunkSize`) **and then also** sends it as a one-chunk fragment set (a missing
> `return`). Yjs makes the duplicate harmless but it doubles traffic. The Rust port sends
> small messages once (raw) and only fragments when `> chunk_size`. The fork *provider*'s
> `MessageSender` already guards correctly (verified), so there is no compatibility impact.

## Configuration

Add to `ConnectionConfiguration` (`hocuspocu-rs/src/types.rs`):

```rust
pub struct ConnectionConfiguration {
    pub read_only: bool,
    pub is_authenticated: bool,
    pub message_chunk_size: usize, // 0 = disabled (default)
}
```

Set from a hook (e.g. `on_authenticate`) by mutating
`connection_config.message_chunk_size`, mirroring the fork. `setup_new_connection`
(`client_connection.rs`) reads it and uses it **only to gate outbound chunking** (the sink
wrapper, below). No global `Configuration` default — per-connection via hook only.

> **Inbound reassembly is unconditional.** `message_chunk_size` does *not* gate inbound.
> Whether the *client* fragments is decided by the client's own chunk size, so the server
> must always reassemble incoming `100/101/102` frames regardless of its own outbound
> setting. Inbound handling is therefore always on — but is only ever reached on an
> authenticated, established connection (pre-auth messages are queued, never reassembled).

## Outbound (server → client) chunking

**Key architectural fact (verified):** outbound bytes reach the socket from *two* groups of
call sites — `Connection::send` (awareness init, stateless, close, sync replies) **and** the
`Document`'s broadcast paths (`broadcast_awareness_to_connections`, the update broadcast,
`broadcast_stateless`, `send_to_connection`, `send_to_all_connections`), which write
directly to `ConnectionEntry.ws: Arc<dyn WebSocketSink>`. The document-update broadcast is
the most important path and bypasses `Connection::send` entirely. All app-level outbound
goes through `Arc<dyn WebSocketSink>::send`; the only raw socket writes live inside the
single writer task that drains the sink's command channel.

**Approach: wrap the sink once.** Introduce a `ChunkingSink` that implements
`WebSocketSink` by delegating `ready_state`/`close` to an inner sink and chunking inside
`send`:

```rust
struct ChunkingSink { inner: Arc<dyn WebSocketSink>, chunk_size: usize }

impl WebSocketSink for ChunkingSink {
    fn send(&self, bytes: Vec<u8>) -> Result<(), _> {
        if bytes.len() <= self.chunk_size {
            return self.inner.send(bytes);          // raw, no duplicate
        }
        let address = peek_first_var_string(&bytes);
        let id = uuid::Uuid::new_v4().to_string();
        self.inner.send(fragment_start(address, &id))?;
        for (i, chunk) in bytes.chunks(self.chunk_size).enumerate() {
            self.inner.send(fragment_data(address, &id, i, chunk))?;
        }
        self.inner.send(fragment_end(address, &id))
    }
    fn ready_state(&self) -> WsReadyState { self.inner.ready_state() }
    // close(...) -> inner
}
```

In `setup_new_connection`, when `message_chunk_size > 0`, wrap `self.websocket` once and
pass the **same wrapped sink** to both `document.add_connection(...)` and
`Connection::new(...)`. When `0`, pass the raw sink.

- **No broadcast/send call-site changes**, and **no changes to `Connection::send` or
  `Connection::new` signatures** — they just receive the (already-wrapped) sink. `Connection`
  stores no chunk size for outbound.
- **No recursion:** fragment frames go out via `inner`, never back through
  `ChunkingSink::send`.
- **Ordering (verified):** `TungsteniteWebSocketSink::send` pushes onto an mpsc channel
  drained by one writer task, so `Start → Data… → End` keep order. If two large messages to
  the same connection are sent concurrently their fragments may interleave on the channel,
  but each frame carries `unique_id` so the receiver reassembles correctly; small
  (non-fragmented) messages are single pushes and cannot be corrupted.
- The large initial sync (`SyncStep2`) and awareness/stateless broadcasts all flow through
  the wrapped sink, so the most common large message is covered.

Fragment-frame builders are added to `OutgoingMessage` (or as small helpers) using the
existing `encoding::write_var_*` functions.

## Inbound (client → server) reassembly

Mirror the fork's `FragmentBuffer` + `activeFragmentTransmissions`, located on the
`Connection` (where per-connection state lives):

- New field on `Connection`: `fragment_buffers: Mutex<HashMap<String, FragmentBuffer>>`.
- `FragmentBuffer`: `index -> Vec<u8>` map, `received_end` flag; `is_complete()` ==
  end received **and** chunks are contiguous `0..=max_index`; `combine()` sorts by index
  and concatenates. (Same logic as the fork.)
- In `Connection::process_messages`, after decoding the `address` prefix and running
  `before_handle_message`, peek the `var_uint` type:
  - `100` → create buffer for `unique_id` (warn if one already exists).
  - `101` → append `(index, chunk)` to the buffer (warn if `unique_id` unknown).
  - `102` → mark end; if complete, remove the buffer and feed the **combined bytes** through
    the same `MessageReceiver` + result-handling path used for normal messages (the combined
    bytes are a complete original frame). Warn + drop on unknown id / incomplete.
  - anything else → existing path (full `MessageReceiver::apply`).
- Fragment frames are consumed at the `Connection` layer and never reach `MessageReceiver`.
- **`before_handle_message` runs per fragment frame, not on the reassembled message**
  (matches the fork). Consequence: a hook can inspect each frame but the reassembled large
  message is not re-checked — which is consistent with intentionally allowing large
  (chunked) messages.
- **No resource caps (strict fork parity).** Buffers live on the `Connection` and are
  dropped when it closes; there is no per-connection size / count / idle-timeout limit.
  Fragments are only processed on an authenticated, established connection — the same trust
  model as the fork. (Documented as an explicit trust assumption.)

Reassembly relies on WebSocket in-order delivery within a connection (same assumption as the
fork — no total-count field, contiguity is the completeness check).

## Feature gating

No cargo feature. The fragment types and inbound handling are **always compiled**; behavior
is gated at runtime:

- Outbound: only chunks when `message_chunk_size > 0`.
- Inbound: always handles `100/101/102`, but they are only sent by a client that opted into
  chunking — a stock upstream client never sends them, so a default deployment is
  behaviorally identical to upstream.

## TypeScript provider (in-repo, source of truth)

Copy the walla provider fork
(`apps/dashboard/utils/yjs/hocuspocus-provider/`, ~19 files / ~1900 lines) into a new
top-level **`provider/`** directory as a standalone package.

- Own `package.json` with its deps (`lib0`, `yjs`, `y-protocols`, `ws`,
  `@hocuspocus/common`).
- **Only change:** the fragment numbers in `provider/.../types.ts`
  (`FragmentStart 10→100`, `FragmentData 11→101`, `FragmentEnd 12→102`). Verify no other
  file hardcodes `10/11/12` for fragments.
- Everything else stays identical. **This repo's `provider/` is now the source of truth**
  that walla copies from / depends on going forward.
- Not published to npm; used as the canonical client and the interop test client.

## Testing & verification

- **Unit (`hocuspocu-rs/tests/wire_protocol_test.rs`):** fragment frame
  encode/decode; small message → single raw frame (no fragments); large message →
  `Start` + N×`Data` + `End`; reassembly round-trip reproduces the original bytes;
  out-of-order chunk reassembly; unknown/incomplete id handling.
- **Interop (`interop/`):** node 22 / bun / `tsx` are all available; the harness already has
  a `large_doc_test.mjs`. Add a fragmentation test (TypeScript, run via `tsx`) that imports
  the new `../provider`, drives the Rust server, and syncs a document larger than the chunk
  size in both directions — asserting fragments are actually emitted/reassembled (a case the
  stock `@hocuspocus/provider` 4.1.0 cannot exercise).
- The existing interop suite (real `@hocuspocus/provider` 4.1.0, chunking off) must stay
  green — proves opt-in does not change default behavior.
- Full `cargo test` + `cargo clippy --all-targets` stay green; `cargo fmt` clean.

## Migration notes (must document)

Fragment numbers move to `100/101/102`, so **the currently deployed walla clients (which use
`10/11/12`) are NOT compatible with the new Rust server.** The server now reads type `10` as
`Pong` (unhandled → "no handler" log), so an old client's `FragmentStart=10` will not be
understood. **Decision: lockstep deploy** — the new TS provider and the Rust server ship
together; existing browser tabs pick up the new provider on reconnect/refresh (Yjs recovers
state). No dual-accept (`10/11/12`) transition code in the server — it would reintroduce the
`Pong=10` collision the renumbering exists to avoid.

## Files touched (summary)

| File | Change |
| --- | --- |
| `hocuspocu-rs/src/types.rs` | add `FragmentStart=100/FragmentData=101/FragmentEnd=102` to `MessageType` + `TryFrom`; add `message_chunk_size` to `ConnectionConfiguration`. |
| `hocuspocu-rs/src/outgoing_message.rs` | fragment-frame builders. |
| `hocuspocu-rs/src/connection.rs` | `ChunkingSink` (new sink wrapper); `fragment_buffers` field + inbound `100/101/102` handling in `process_messages`. `Connection::send` / `Connection::new` signatures unchanged. |
| `hocuspocu-rs/src/client_connection.rs` | in `setup_new_connection`, wrap sink when `message_chunk_size > 0`; pass wrapped sink to `add_connection` + `Connection::new`. |
| `hocuspocu-rs/src/lib.rs` | re-export any new public types. |
| `hocuspocu-rs/tests/wire_protocol_test.rs` | fragment unit tests. |
| `provider/**` | new — cloned TS provider as a standalone package, renumbered. |
| `interop/**` | fragmentation interop test (tsx) using the new provider. |
| `CLAUDE.md` | document the fragment types, runtime opt-in, inbound-unconditional, migration note. |
| `examples/server.rs` (optional) | demonstrate enabling `message_chunk_size` via a hook. |

## Resolved decisions

- Direction: **bidirectional**.
- Config: **per-connection via hook** (`message_chunk_size`, default `0` = off); **no global default**.
- **Inbound reassembly is unconditional**; `message_chunk_size` gates **outbound only**.
- Numbers: **100/101/102**; keep `Ping=9`/`Pong=10`.
- `message_chunk_size` = **payload slice size** (fork parity); set with margin below the network limit.
- Inbound resource safety: **strict fork parity, no caps**; cleared on close; authenticated-only.
- Outbound: **`ChunkingSink` wrapper**; `Connection` send path unchanged; ordering safe (FIFO writer) and interleaving safe (per-`unique_id`).
- `before_handle_message`: **per fragment frame**, not on the reassembled message.
- **No cargo feature** — always compiled, runtime opt-in.
- TS provider: **full clone + renumber only**, top-level `provider/` standalone package, **this repo = source of truth**, interop via `tsx`, not published to npm.
- Migration: **lockstep**, documented; no dual-accept code.
