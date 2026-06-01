# hocuspocu-rs-common

Shared, dependency-light protocol types for [`hocuspocu-rs`](https://crates.io/crates/hocuspocu-rs)
— a Rust port of [Hocuspocus](https://hocuspocus.dev), mirroring `@hocuspocus/common`.

Provides `CloseEvent` and the coded close reasons (`reset_connection`,
`unauthorized`, `forbidden`, …), `AuthMessageType`, `WsReadyState`, the routing
key helper, and `SkipFurtherHooksError`.

You normally don't depend on this crate directly — `hocuspocu-rs` re-exports it
as `hocuspocu_rs::common`. See the [main crate](https://crates.io/crates/hocuspocu-rs).

## License

MIT. A port of Hocuspocus © Tiptap GmbH.
