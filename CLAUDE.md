# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust port of [Hocuspocus](https://hocuspocus.dev) (`@hocuspocus/server` + `@hocuspocus/common`), a real-time collaborative-editing backend built on Yjs. It uses [`yrs`](https://docs.rs/yrs) (the Rust Yjs implementation, with its native `Awareness`) and `tokio-tungstenite` for WebSockets.

**The overriding constraint is wire-protocol and behavioral compatibility with the upstream TypeScript implementation.** Existing Yjs clients (`y-websocket`, `@hocuspocus/provider`) must interoperate with this server unchanged, and it is meant to be a drop-in peer for other hocuspocus servers (incl. multi-server setups). The binary message format, the sync/awareness protocol, close-event codes, hook names, and debounce/unload semantics all mirror the JS reference — code comments like `// matches TS` mark spots where behavior was deliberately copied. Crate versions (`4.1.0`) track the upstream npm version. When changing protocol/encoding/awareness code, assume a JS client is on the other end and check the upstream behavior first; recent git history is almost entirely "compatibility gap" fixes.

## Workspace layout

Cargo workspace (`resolver = "2"`), two library crates plus one runnable example:

- **`hocuspocu-rs-common`** — shared, dependency-light types mirroring `@hocuspocus/common`: `auth` (`AuthMessageType`), `awareness_states`, `close_events` (`CloseEvent`, `reset_connection()` and other coded close reasons), `routing_key`, `skip_further_hooks_error` (`SkipFurtherHooksError`), `types` (`WsReadyState`). Published on crates.io as `hocuspocu-rs-common`; the server depends on it under the in-code alias `hocuspocus_common` (Cargo `package = ` rename) and re-exports it as `hocuspocu_rs::common`.
- **`hocuspocu-rs`** — the server; depends on `hocuspocu-rs-common`, `yrs` (feature `sync`), `tokio` (full), `tokio-tungstenite`, `futures-util`, `uuid`, `dashmap`, `async-trait`, `bytes`, `tracing`, `http`, `url`, `serde`. Ships `examples/server.rs` (a runnable server with optional in-memory persistence).

Crate package names are `hocuspocu-rs` / `hocuspocu-rs-common`, but the crate **directories** are `hocuspocu-rs/` and `hocuspocu-rs-common/`. Publish order on crates.io: `hocuspocu-rs-common` first, then `hocuspocu-rs`.

The `interop/` directory (workspace root, not part of either published crate) holds the JS interop + benchmark harness: `multi_user_interop.mjs` drives the server with the real `@hocuspocus/provider` 4.1.0; `run_bench.sh` benchmarks head-to-head against `@hocuspocus/server`. See `interop/README.md`. The `provider/` directory (also workspace root, not published) is the source-of-truth TypeScript provider implementing the fragment-chunking protocol (message numbers 100/101/102); the fragmentation interop test lives at `provider/test/fragment_interop.ts` and is run under `tsx`.

## Commands

```sh
cargo build                              # build the workspace
cargo test                               # all unit + integration tests
cargo test -p hocuspocu-rs          # one crate
cargo test --test wire_protocol_test     # one integration file (in hocuspocu-rs/tests/)
cargo test some_test_name                # tests matching a name substring
cargo clippy --all-targets               # lint
cargo fmt                                # format
```

Run the bundled example server (env vars are optional):

```sh
cargo run --example server               # listens on :80
HP_PORT=8088 HP_PERSIST=1 HP_LOG=1 cargo run --example server
```

`HP_PORT` (port), `HP_PERSIST` (in-memory persistence), `HP_LOG` (lifecycle logging), `HP_DEBOUNCE` (store debounce ms). The `interop/` harness has its own commands — see `interop/README.md`.

Integration tests live in `hocuspocu-rs/tests/` (`basic_test.rs`, `integration_test.rs`, `wire_protocol_test.rs`). `wire_protocol_test.rs` is the guard for binary compatibility — run it after touching anything under `encoding.rs`, `incoming_message.rs`, `outgoing_message.rs`, `message_receiver.rs`, or awareness handling.

## Architecture (`hocuspocu-rs/src`)

Embed the server as a library: build a `Hocuspocus` (or a `Server` that wraps one), then feed it connections. There is **no builder type** — construct via `Hocuspocus::new(Some(Configuration { .. }))` (returns `Arc<Self>`) or `.configure(Configuration)`. `Configuration { name, timeout (60s), debounce (2s), max_debounce (10s), quiet, unload_immediately, extensions: Vec<Arc<dyn Extension>> }`. Extensions are sorted by `priority()` (higher runs first) at configure time.

- **`hocuspocus.rs`** (the core, ~900 lines) — `Hocuspocus` owns `documents: RwLock<HashMap<String, Arc<Document>>>` plus `loading_documents`/`unloading_documents` maps that serialize concurrent load/unload of the same document name (a per-name `Mutex` holding the load result, so concurrent openers await one load). Also owns the `Debouncer`. **Hooks are not a separate manager** — each lifecycle event has a `hooks_*` method here that locks the config and iterates `extensions` in order. Key flows: `create_document`/`load_document` (runs `on_create_document` → `on_load_document` → installs the `yrs` update + awareness observers → `after_load_document`), `handle_document_update` (`on_change` then debounced store), `store_document_hooks` (debounced `on_store_document`/`after_store_document`, then maybe unload), `unload_document`, `handle_connection`, `open_direct_connection`.
- **`document.rs`** — `Document` wraps a `yrs::Doc` and a native `yrs` `Awareness`, tracks connections, and registers `observe_update_v1` (fires on *every* mutation: wire updates and `DirectConnection` transactions alike → drives `on_change`/store) and an awareness `on_update` (drives `on_awareness_update`). Holds `before_broadcast_stateless` / `before_handle_awareness` callbacks wired up by `load_document`, and a `save_mutex` serializing stores.
- **`connection.rs`** / **`client_connection.rs`** / **`direct_connection.rs`** — `Connection` is the transport-agnostic per-client state; `ClientConnection` drives a socket (auth handshake, message dispatch, timeout, close); `DirectConnection` is an in-process client (no socket) used for server-side document access and tests.
- **`server.rs`** — `Server` + `ServerConfiguration { port (80), address (0.0.0.0), stop_on_signals, config }`: a `tokio-tungstenite` listener (`TcpListener` + `accept_async`) that turns each socket into a `ClientConnection`. The socket is abstracted behind the **`WebSocketSink` trait** (in `types.rs`); `TungsteniteWebSocketSink` is the concrete impl. To embed without `Server`, call `Hocuspocus::handle_connection(Arc<dyn WebSocketSink>, request, default_context)` with your own sink.
- **`encoding.rs`** — lib0-compatible varint codec: a `Decoder<'a>` plus free functions `write_var_uint` / `write_var_string` / `write_var_uint8_array`. (No `Encoder` struct — outgoing buffers are plain `Vec<u8>`.)
- **`incoming_message.rs`** / **`outgoing_message.rs`** — `IncomingMessage<'a>` reads a frame; `OutgoingMessage` builds one. **Every frame is `[var_string document_name][var_uint message_type][payload…]`** — the document name prefix is part of the wire format, not just routing.
- **`message_receiver.rs`** — `MessageReceiver::apply` reads the document-name prefix + `MessageType` and dispatches. `BroadcastStateless` is server→client only and is rejected if received from a client.
- **`util/debounce.rs`** — `Debouncer` keyed by `"onStoreDocument-{name}"`, honoring `debounce`/`max_debounce`; supports `execute_now`, `is_debounced`, `is_currently_executing`.

### Message protocol (`MessageType` in `types.rs`)

`Unknown=-1, Sync=0, Awareness=1, Auth=2, QueryAwareness=3, SyncReply=4, Stateless=5, BroadcastStateless=6, Close=7, SyncStatus=8, Ping=9, Pong=10`. Within a `Sync`/`SyncReply` message the sub-type is the standard Yjs sync protocol: `SyncStep1=0`, `SyncStep2=1`, `Update=2`. These integer values are wire contract — do not renumber.

**Fragment chunking (opt-in divergence from upstream).** `FragmentStart=100`,
`FragmentData=101`, `FragmentEnd=102` carry an application-level fragmentation protocol for
networks that drop large WebSocket messages. Numbers sit above the upstream range
(`0..=10`, incl. `Ping=9`/`Pong=10`) so upstream compatibility is preserved. Outbound
chunking is enabled by `Configuration.message_chunk_size` (bytes; `0` = off, the default)
and installed as a transparent `ChunkingSink` around the connection's `WebSocketSink`.
Inbound reassembly (`FragmentBuffer` on `Connection`) is **always on** — the client decides
whether to fragment. The matching client is the in-repo `provider/` (TypeScript); the two
must be deployed in lockstep (a client using the old `10/11/12` numbering is incompatible).

### Extension / hook system

`types.rs` defines `#[async_trait] trait Extension: Send + Sync` — the single extension point for persistence, auth, logging, scaling, etc. Every method has a default no-op, plus `priority()` (default 100) and `name()`. Full lifecycle, roughly in order:

`on_configure` · `on_listen` · `on_upgrade` · `on_connect` · `connected` · `on_authenticate` · `on_token_sync` · `on_create_document` · `on_load_document` · `after_load_document` · `before_handle_message` · `before_handle_awareness` · `before_sync` · `before_broadcast_stateless` · `on_stateless` · `on_change` · `on_store_document` · `after_store_document` · `on_awareness_update` · `on_request` · `on_disconnect` · `before_unload_document` · `after_unload_document` · `on_destroy`.

Return conventions (note: **not** a `HookError` enum):

- Most hooks return `HookResult = Result<Option<Context>, Box<dyn Error + Send + Sync>>`. Returning `Ok(Some(ctx))` contributes a `Context` (an `Arc<dyn Any + Send + Sync>`) attached to the connection (e.g. `on_authenticate` returning user info). `Ok(None)` is the no-op default.
- `on_load_document` returns `LoadDocumentResult` (`Ok(Some(LoadedDocument::Update(bytes)))` to seed the doc from storage).
- Returning `Err(..)` aborts the operation (e.g. failed auth closes the connection). To stop *remaining* hooks without it being treated as a failure, return a `SkipFurtherHooksError` (from `hocuspocu-rs-common`) — the store path downcasts to detect it.

Prefer adding an `Extension` over editing the core; that matches upstream and keeps the protocol code untouched.

### Origins and multi-server semantics

`TransactionOrigin` (in `types.rs`) is `Connection(..) | Redis | Local(..)` and gates whether storage hooks run: `should_skip_store_hooks` returns `true` for `Redis` (updates arriving from another node via Redis must not be re-persisted) and for `Local` when its `skip_store_hooks` flag is set; `Connection` updates always persist. Preserve this when adding update paths, or you will get redundant writes / store loops across nodes.

## Conventions

- **Mirror the TS source.** Module names, hook names, payload field names, message-type numbers, and close-event codes deliberately echo `@hocuspocus/*`. Check the upstream equivalent before adding or renaming anything protocol-facing.
- **Concurrency:** shared state is `Arc<...>` behind `tokio::sync::RwLock`/`Mutex`; documents are `Arc<Document>` in a `RwLock<HashMap>`. `Hocuspocus`/`Server` are used as `Arc<Self>` (not `Clone`). Reuse the existing locking patterns rather than introducing new schemes, and watch lock-ordering across the load/unload maps.
- **Awareness uses `yrs`'s native `Awareness`** — never hand-roll an awareness encoding; that was a deliberate wire-compatibility fix.
- Public API is surfaced through each crate's `lib.rs` re-exports; add new public types there.
