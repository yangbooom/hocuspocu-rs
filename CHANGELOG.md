# Changelog

All notable changes to this project are documented here. The version tracks the
upstream `@hocuspocus/server` npm version it targets.

## [Unreleased]

### Changed
- Cap per-connection WebSocket buffers at 16 KiB (tungstenite defaults to a 128 KiB
  read buffer per socket). Per-connection memory drops ~148 KiB → ~39 KiB, so the
  server now uses ~3× less memory than `@hocuspocus/server` 4.1.0 under load (and
  ~26× at idle) instead of more. Message-size limits are unchanged (64 MiB), so
  large initial syncs are unaffected.
- Replace the per-message `tokio::spawn` + `Mutex` WebSocket sink with one ordered
  writer task per connection (mpsc channel). This guarantees per-client frame
  ordering and lifts sustained throughput ~1790 → ~2090 routed ops/s, on par with
  the JS server.

## [4.1.0] - 2026-05-29

Initial public release: a Rust port of Hocuspocus, wire-compatible with
`@hocuspocus/server` 4.1.0.

### Added
- `hocuspocu-rs` — the server crate (`Hocuspocus`, `Server`, `ServerConfiguration`,
  `Configuration`, `DirectConnection`, the `Extension` trait, and the full hook
  lifecycle).
- `hocuspocu-rs-common` — shared protocol types (`CloseEvent` and coded close
  reasons, `AuthMessageType`, `WsReadyState`, routing key, `SkipFurtherHooksError`).
- lib0-compatible varint codec and the document-name-prefixed message framing
  used on the wire.
- Yjs sync protocol (SyncStep1/SyncStep2/Update), awareness via `yrs`'s native
  `Awareness`, stateless messages, and the auth handshake (token + provider
  version, `\0`-separated session ids, multi-document multiplexing per socket).
- Verified against the real `@hocuspocus/provider` 4.1.0 client: multi-user
  convergence, awareness propagation, late-join, persistence round-trip.

[4.1.0]: https://github.com/yangbooom/hocuspocu-rs/releases/tag/v4.1.0
