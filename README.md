# hocuspocu-rs

A Rust port of [Hocuspocus](https://hocuspocus.dev) — the real-time
collaborative-editing backend for [Yjs](https://yjs.dev) — built on
[`yrs`](https://docs.rs/yrs) and `tokio-tungstenite`.

It is a **drop-in, wire-compatible peer** for `@hocuspocus/server` **4.1.0**:
existing Yjs clients (`@hocuspocus/provider`, `y-websocket`) interoperate with it
unchanged. The binary message format, sync/awareness protocol, close-event codes,
hook lifecycle, and debounce/unload semantics all mirror the upstream TypeScript
implementation.

> Crate versions track the upstream npm version (`4.1.0`).

## Why a Rust port?

The standout difference is memory. Driving identical workloads with the real
`@hocuspocus/provider` client head-to-head against `@hocuspocus/server` 4.1.0 on
the same machine ([`interop/stress.sh`](interop/)):

| | hocuspocu-rs | @hocuspocus/server 4.1.0 |
|---|---|---|
| Idle RSS | **~2.4 MB** | ~62 MB |
| RSS, 1000 connections (100 docs) | **~42 MB** | ~125 MB |
| RSS, 20 s throughput soak (peak) | **~46 MB** | ~152 MB |
| Sustained throughput (routed ops/s) | ~2.1k | ~2.0k |
| Fan-out latency, 200 receivers — p50 | ~3.9 ms | **~2.0 ms** |
| Fan-out latency, 200 receivers — p99 | ~15 ms | **~9 ms** |

Memory is the decisive win — **~26× lower at idle and ~3× lower under load**, with
no GC pauses. Sustained throughput is on par. Broadcast latency is low single-digit
milliseconds but higher than the JS server at the median: hocuspocu-rs writes to
each client from a per-connection async task, where Node broadcasts in one inline
synchronous loop. Reach for this port when memory footprint and a small, predictable
process matter; the JS server still has the edge on raw fan-out latency. Reproduce
everything with [`interop/stress.sh`](interop/).

## Install

```sh
cargo add hocuspocu-rs
```

```toml
[dependencies]
hocuspocu-rs = "4.1"
```

## Quick start

```rust
use hocuspocu_rs::{Server, ServerConfiguration, Configuration};

#[tokio::main]
async fn main() {
    let server = Server::with_config(ServerConfiguration {
        port: 8088,
        address: "127.0.0.1".to_string(),
        config: Configuration::default(),
        ..Default::default()
    });
    server.listen(None).await.unwrap();
}
```

Point any Yjs client at it:

```js
import { HocuspocusProvider } from "@hocuspocus/provider";
new HocuspocusProvider({ url: "ws://127.0.0.1:8088", name: "my-doc", document });
```

A complete runnable server (with optional in-memory persistence and lifecycle
logging) lives in [`hocuspocu-rs/examples/server.rs`](hocuspocu-rs/examples/server.rs):

```sh
HP_PORT=8088 HP_PERSIST=1 HP_LOG=1 cargo run -p hocuspocu-rs --example server
```

## Extensions & hooks

Behaviour — persistence, auth, logging, multi-server scaling — is added through
the `Extension` trait, mirroring upstream's hook system. Every method has a
no-op default, so implement only what you need:

```rust
use async_trait::async_trait;
use hocuspocu_rs::{Extension, OnAuthenticatePayload, HookResult};

struct Auth;

#[async_trait]
impl Extension for Auth {
    async fn on_authenticate(&self, p: &OnAuthenticatePayload) -> HookResult {
        if p.token != "let-me-in" {
            return Err("unauthorized".into()); // closes the connection
        }
        Ok(None)
    }
}
```

The full lifecycle (in order):

`on_configure` · `on_listen` · `on_upgrade` · `on_connect` · `connected` ·
`on_authenticate` · `on_token_sync` · `on_create_document` · `on_load_document` ·
`after_load_document` · `before_handle_message` · `before_handle_awareness` ·
`before_sync` · `before_broadcast_stateless` · `on_stateless` · `on_change` ·
`on_store_document` · `after_store_document` · `on_awareness_update` ·
`on_request` · `on_disconnect` · `before_unload_document` ·
`after_unload_document` · `on_destroy`

## Crates

- **`hocuspocu-rs`** — the server (this is what you depend on).
- **`hocuspocu-rs-common`** — shared protocol types (`CloseEvent`, `AuthMessageType`,
  `WsReadyState`, …), re-exported by the server crate.

## Compatibility & testing

- `cargo test` — unit + integration tests, including `wire_protocol_test` (the
  binary-format guard).
- [`interop/`](interop/) — multi-user tests driven by the **real
  `@hocuspocus/provider` 4.1.0`**, a persistence round-trip, and the head-to-head
  benchmark. The same interop suite passes against the upstream JS server.

## License

MIT. This is a port of Hocuspocus © Tiptap GmbH; see [LICENSE](LICENSE) for the
combined attribution.
