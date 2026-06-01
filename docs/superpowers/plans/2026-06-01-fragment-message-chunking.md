# Fragment Message Chunking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in, bidirectional application-level message fragmentation to hocuspocu-rs (split large WS messages into `FragmentStart/Data/End` frames and reassemble them) plus a matching in-repo TypeScript provider, so the server works through network intermediaries that drop large WebSocket messages.

**Architecture:** Outbound chunking is a transparent `WebSocketSink` wrapper (`ChunkingSink`) installed per connection when the global `Configuration.message_chunk_size > 0` — every existing send path is covered with no call-site changes. Inbound reassembly is unconditional: `Connection::process_messages` recognizes the three fragment message types, buffers chunks by `unique_id` in a per-connection `FragmentBuffer`, and re-dispatches the reassembled frame through the normal `MessageReceiver`. Fragment message types are `100/101/102` (above upstream's `0..=10`, keeping `Ping=9`/`Pong=10`).

**Tech Stack:** Rust (tokio, yrs, tokio-tungstenite, uuid), lib0-compatible varint codec; TypeScript provider run under `tsx` for interop.

**Spec:** `docs/superpowers/specs/2026-06-01-fragment-message-chunking-design.md`

---

## File structure

| File | Responsibility |
| --- | --- |
| `hocuspocu-rs/src/types.rs` | `MessageType::{FragmentStart=100,FragmentData=101,FragmentEnd=102}` + `TryFrom`; `Configuration.message_chunk_size`. |
| `hocuspocu-rs/src/outgoing_message.rs` | `write_fragment_start/data/end` frame builders. |
| `hocuspocu-rs/src/fragment.rs` (new) | `FragmentBuffer` (inbound reassembly) + `ChunkingSink` (outbound sink wrapper). |
| `hocuspocu-rs/src/lib.rs` | `pub mod fragment;` + re-exports. |
| `hocuspocu-rs/src/connection.rs` | `fragment_buffers` field + inbound fragment handling in `process_messages`. |
| `hocuspocu-rs/src/hocuspocus.rs` | `configure()` copies `message_chunk_size`. |
| `hocuspocu-rs/src/client_connection.rs` | wrap the sink in `setup_new_connection` when chunking is on. |
| `hocuspocu-rs/examples/server.rs` | `HP_CHUNK` env → `Configuration.message_chunk_size`. |
| `hocuspocu-rs/tests/wire_protocol_test.rs` | fragment unit tests. |
| `provider/**` (new) | cloned TypeScript provider, renumbered to `100/101/102`. |
| `interop/fragment_interop.ts` (new) + `interop/package.json` | end-to-end fragmentation interop test (tsx). |
| `CLAUDE.md` | document the protocol extension + migration. |

All Rust commands run from the worktree root: `/Users/kimuyb/Paprika/dev/hocuspocu-rs/.claude/worktrees/fragment-chunking`.

---

## Task 1: Message types + Configuration field

**Files:**
- Modify: `hocuspocu-rs/src/types.rs` (`MessageType` enum + `TryFrom`, `Configuration` + `Default`)
- Test: `hocuspocu-rs/tests/wire_protocol_test.rs`

- [ ] **Step 1: Write the failing test**

Append to `hocuspocu-rs/tests/wire_protocol_test.rs`:

```rust
#[test]
fn test_fragment_message_type_numbers() {
    assert_eq!(MessageType::FragmentStart as u64, 100);
    assert_eq!(MessageType::FragmentData as u64, 101);
    assert_eq!(MessageType::FragmentEnd as u64, 102);
    // Ping/Pong are preserved for upstream compatibility.
    assert_eq!(MessageType::Ping as u64, 9);
    assert_eq!(MessageType::Pong as u64, 10);
    assert_eq!(MessageType::try_from(100u64), Ok(MessageType::FragmentStart));
    assert_eq!(MessageType::try_from(101u64), Ok(MessageType::FragmentData));
    assert_eq!(MessageType::try_from(102u64), Ok(MessageType::FragmentEnd));
    // chunking is off by default.
    assert_eq!(Configuration::default().message_chunk_size, 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test wire_protocol_test test_fragment_message_type_numbers`
Expected: FAIL — `no variant named FragmentStart` / `no field message_chunk_size`.

- [ ] **Step 3: Add the enum variants + TryFrom arms**

In `hocuspocu-rs/src/types.rs`, in `pub enum MessageType` add after `Pong = 10,`:

```rust
    FragmentStart = 100,
    FragmentData = 101,
    FragmentEnd = 102,
```

In `impl TryFrom<u64> for MessageType`, add before `_ => Err(()),`:

```rust
            100 => Ok(MessageType::FragmentStart),
            101 => Ok(MessageType::FragmentData),
            102 => Ok(MessageType::FragmentEnd),
```

- [ ] **Step 4: Add the Configuration field**

In `pub struct Configuration`, add after `pub extensions: Vec<Arc<dyn Extension>>,`:

```rust
    pub message_chunk_size: usize,
```

In `impl Default for Configuration`, add inside the returned `Self { … }` after `extensions: Vec::new(),`:

```rust
            message_chunk_size: 0,
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --test wire_protocol_test test_fragment_message_type_numbers`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add hocuspocu-rs/src/types.rs hocuspocu-rs/tests/wire_protocol_test.rs
git commit -m "feat: add fragment message types and Configuration.message_chunk_size"
```

---

## Task 2: Fragment frame builders on OutgoingMessage

**Files:**
- Modify: `hocuspocu-rs/src/outgoing_message.rs`
- Test: `hocuspocu-rs/tests/wire_protocol_test.rs`

- [ ] **Step 1: Write the failing test**

Append to `hocuspocu-rs/tests/wire_protocol_test.rs`:

```rust
#[test]
fn test_fragment_frame_builders_roundtrip() {
    // FragmentData: [addr][101][var_string id][var_uint index][var_uint8array chunk]
    let frame = OutgoingMessage::new("doc")
        .write_fragment_data("abc", 3, &[9, 8, 7])
        .to_vec();
    let mut d = Decoder::new(&frame);
    assert_eq!(d.read_var_string().unwrap(), "doc");
    assert_eq!(d.read_var_uint().unwrap(), 101);
    assert_eq!(d.read_var_string().unwrap(), "abc");
    assert_eq!(d.read_var_uint().unwrap(), 3);
    assert_eq!(d.read_var_uint8_array().unwrap(), vec![9, 8, 7]);
    assert!(!d.has_content());

    // FragmentStart / FragmentEnd: [addr][type][var_string id]
    for (build, ty) in [
        (OutgoingMessage::new("doc").write_fragment_start("id1").to_vec(), 100u64),
        (OutgoingMessage::new("doc").write_fragment_end("id1").to_vec(), 102u64),
    ] {
        let mut d = Decoder::new(&build);
        assert_eq!(d.read_var_string().unwrap(), "doc");
        assert_eq!(d.read_var_uint().unwrap(), ty);
        assert_eq!(d.read_var_string().unwrap(), "id1");
        assert!(!d.has_content());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test wire_protocol_test test_fragment_frame_builders_roundtrip`
Expected: FAIL — `no method named write_fragment_data`.

- [ ] **Step 3: Implement the builders**

In `hocuspocu-rs/src/outgoing_message.rs`, add these methods inside `impl OutgoingMessage`, before `pub fn to_vec(self)`:

```rust
    pub fn write_fragment_start(mut self, unique_id: &str) -> Self {
        self.message_type = Some(MessageType::FragmentStart);
        encoding::write_var_uint(&mut self.encoder, MessageType::FragmentStart as u64);
        encoding::write_var_string(&mut self.encoder, unique_id);
        self
    }

    pub fn write_fragment_data(mut self, unique_id: &str, index: usize, chunk: &[u8]) -> Self {
        self.message_type = Some(MessageType::FragmentData);
        encoding::write_var_uint(&mut self.encoder, MessageType::FragmentData as u64);
        encoding::write_var_string(&mut self.encoder, unique_id);
        encoding::write_var_uint(&mut self.encoder, index as u64);
        encoding::write_var_uint8_array(&mut self.encoder, chunk);
        self
    }

    pub fn write_fragment_end(mut self, unique_id: &str) -> Self {
        self.message_type = Some(MessageType::FragmentEnd);
        encoding::write_var_uint(&mut self.encoder, MessageType::FragmentEnd as u64);
        encoding::write_var_string(&mut self.encoder, unique_id);
        self
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test wire_protocol_test test_fragment_frame_builders_roundtrip`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add hocuspocu-rs/src/outgoing_message.rs hocuspocu-rs/tests/wire_protocol_test.rs
git commit -m "feat: add fragment frame builders to OutgoingMessage"
```

---

## Task 3: FragmentBuffer (inbound reassembly logic)

**Files:**
- Create: `hocuspocu-rs/src/fragment.rs`
- Modify: `hocuspocu-rs/src/lib.rs`
- Test: inline `#[cfg(test)]` in `hocuspocu-rs/src/fragment.rs`

- [ ] **Step 1: Create the module with FragmentBuffer + failing tests**

Create `hocuspocu-rs/src/fragment.rs`:

```rust
//! Application-level message fragmentation: splitting large outbound WebSocket
//! messages into `FragmentStart/Data/End` frames (`ChunkingSink`) and reassembling
//! inbound fragment frames (`FragmentBuffer`). Wire-compatible with the matching
//! TypeScript provider in `provider/`.

use std::collections::HashMap;

/// Accumulates inbound fragment chunks for one `unique_id` until the series is complete.
/// Mirrors the upstream fork: completeness is "end received AND chunks are contiguous
/// `0..=max_index`" — there is no total-count field, so it relies on in-order WS delivery.
#[derive(Default)]
pub struct FragmentBuffer {
    chunks: HashMap<u64, Vec<u8>>,
    received_end: bool,
}

impl FragmentBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_chunk(&mut self, index: u64, data: Vec<u8>) {
        self.chunks.insert(index, data);
    }

    pub fn mark_end(&mut self) {
        self.received_end = true;
    }

    pub fn is_complete(&self) -> bool {
        if !self.received_end || self.chunks.is_empty() {
            return false;
        }
        let max_index = *self.chunks.keys().max().unwrap();
        self.chunks.len() as u64 == max_index + 1
    }

    /// Concatenate chunks in index order. Caller must check `is_complete()` first.
    pub fn combine(&self) -> Vec<u8> {
        let mut indices: Vec<&u64> = self.chunks.keys().collect();
        indices.sort_unstable();
        let mut out = Vec::new();
        for i in indices {
            out.extend_from_slice(&self.chunks[i]);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_without_end() {
        let mut b = FragmentBuffer::new();
        b.add_chunk(0, vec![1, 2]);
        assert!(!b.is_complete());
    }

    #[test]
    fn incomplete_with_gap() {
        let mut b = FragmentBuffer::new();
        b.add_chunk(0, vec![1]);
        b.add_chunk(2, vec![3]); // missing index 1
        b.mark_end();
        assert!(!b.is_complete());
    }

    #[test]
    fn complete_and_combines_in_order() {
        let mut b = FragmentBuffer::new();
        // add out of order on purpose
        b.add_chunk(2, vec![5, 6]);
        b.add_chunk(0, vec![1, 2]);
        b.add_chunk(1, vec![3, 4]);
        b.mark_end();
        assert!(b.is_complete());
        assert_eq!(b.combine(), vec![1, 2, 3, 4, 5, 6]);
    }
}
```

- [ ] **Step 2: Register the module**

In `hocuspocu-rs/src/lib.rs`, add with the other `pub mod` lines (alphabetical near `pub mod encoding;`):

```rust
pub mod fragment;
```

And add a re-export with the others (after `pub use document::Document;`):

```rust
pub use fragment::{ChunkingSink, FragmentBuffer};
```

> Note: `ChunkingSink` does not exist yet (Task 4). To keep this task compiling on its own,
> add only `pub use fragment::FragmentBuffer;` now and extend it to
> `pub use fragment::{ChunkingSink, FragmentBuffer};` in Task 4.

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p hocuspocu-rs fragment::tests`
Expected: PASS (3 tests)

- [ ] **Step 4: Commit**

```bash
git add hocuspocu-rs/src/fragment.rs hocuspocu-rs/src/lib.rs
git commit -m "feat: add FragmentBuffer for inbound reassembly"
```

---

## Task 4: ChunkingSink (outbound sink wrapper)

**Files:**
- Modify: `hocuspocu-rs/src/fragment.rs`, `hocuspocu-rs/src/lib.rs`
- Test: inline `#[cfg(test)]` in `hocuspocu-rs/src/fragment.rs`

- [ ] **Step 1: Write the failing tests**

In `hocuspocu-rs/src/fragment.rs`, add to the existing `#[cfg(test)] mod tests` block:

```rust
    use crate::encoding::{write_var_string, write_var_uint, Decoder};
    use crate::types::WebSocketSink;
    use hocuspocus_common::WsReadyState;
    use std::sync::{Arc, Mutex};

    struct CaptureSink {
        sent: Mutex<Vec<Vec<u8>>>,
    }
    impl WebSocketSink for CaptureSink {
        fn send(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.sent.lock().unwrap().push(data);
            Ok(())
        }
        fn close(&self, _: u16, _: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        fn ready_state(&self) -> WsReadyState {
            WsReadyState::Open
        }
    }

    fn build_message(payload_len: usize) -> Vec<u8> {
        let mut m = Vec::new();
        write_var_string(&mut m, "doc");
        write_var_uint(&mut m, 0); // MessageType::Sync
        m.extend((0..payload_len).map(|i| i as u8));
        m
    }

    #[test]
    fn small_message_is_sent_raw() {
        let cap = Arc::new(CaptureSink { sent: Mutex::new(Vec::new()) });
        let sink = ChunkingSink::new(cap.clone(), 1024);
        let msg = build_message(3);
        sink.send(msg.clone()).unwrap();
        let sent = cap.sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "small message must not be fragmented");
        assert_eq!(sent[0], msg);
    }

    #[test]
    fn large_message_fragments_and_reassembles() {
        let cap = Arc::new(CaptureSink { sent: Mutex::new(Vec::new()) });
        let sink = ChunkingSink::new(cap.clone(), 8);
        let msg = build_message(50);
        sink.send(msg.clone()).unwrap();

        let frames = cap.sent.lock().unwrap().clone();
        assert!(frames.len() >= 3, "expected start + data + end, got {}", frames.len());

        let mut buf = FragmentBuffer::new();
        let mut last_type = 0u64;
        for frame in &frames {
            let mut d = Decoder::new(frame);
            assert_eq!(d.read_var_string().unwrap(), "doc");
            last_type = d.read_var_uint().unwrap();
            let _id = d.read_var_string().unwrap();
            match last_type {
                100 => {}
                101 => {
                    let idx = d.read_var_uint().unwrap();
                    let chunk = d.read_var_uint8_array().unwrap();
                    buf.add_chunk(idx, chunk);
                }
                102 => buf.mark_end(),
                other => panic!("unexpected frame type {}", other),
            }
        }
        assert_eq!(last_type, 102, "last frame must be FragmentEnd");
        assert!(buf.is_complete());
        assert_eq!(buf.combine(), msg, "reassembled bytes must equal original message");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p hocuspocu-rs fragment::tests`
Expected: FAIL — `cannot find type ChunkingSink` / `no function new`.

- [ ] **Step 3: Implement ChunkingSink**

In `hocuspocu-rs/src/fragment.rs`, add these imports at the top (below the existing `use std::collections::HashMap;`):

```rust
use std::sync::Arc;

use hocuspocus_common::WsReadyState;

use crate::encoding::Decoder;
use crate::outgoing_message::OutgoingMessage;
use crate::types::WebSocketSink;
```

And add the type (after the `FragmentBuffer` impl, before the `#[cfg(test)]` block):

```rust
/// A `WebSocketSink` decorator that splits any outbound message larger than `chunk_size`
/// into `FragmentStart` + N×`FragmentData` + `FragmentEnd` frames, all forwarded to the
/// inner sink. Messages `<= chunk_size` pass through unchanged. The inner sink preserves
/// order (single FIFO writer), and each frame carries `unique_id`, so concurrent large
/// sends that interleave on the wire still reassemble correctly on the receiver.
pub struct ChunkingSink {
    inner: Arc<dyn WebSocketSink>,
    chunk_size: usize,
}

impl ChunkingSink {
    pub fn new(inner: Arc<dyn WebSocketSink>, chunk_size: usize) -> Self {
        Self { inner, chunk_size }
    }
}

impl WebSocketSink for ChunkingSink {
    fn send(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if data.len() <= self.chunk_size {
            return self.inner.send(data);
        }

        // The fragment frames are addressed with the same first var_string (the message
        // address, possibly `name\0session`) as the original message. If it can't be read,
        // fall back to sending raw rather than dropping the message.
        let address = match Decoder::new(&data).read_var_string() {
            Ok(addr) => addr,
            Err(_) => return self.inner.send(data),
        };

        let id = uuid::Uuid::new_v4().to_string();
        self.inner
            .send(OutgoingMessage::new(&address).write_fragment_start(&id).to_vec())?;
        for (index, chunk) in data.chunks(self.chunk_size).enumerate() {
            self.inner.send(
                OutgoingMessage::new(&address)
                    .write_fragment_data(&id, index, chunk)
                    .to_vec(),
            )?;
        }
        self.inner
            .send(OutgoingMessage::new(&address).write_fragment_end(&id).to_vec())
    }

    fn close(&self, code: u16, reason: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.close(code, reason)
    }

    fn ready_state(&self) -> WsReadyState {
        self.inner.ready_state()
    }
}
```

- [ ] **Step 4: Upgrade the lib re-export**

In `hocuspocu-rs/src/lib.rs`, change the line added in Task 3 from `pub use fragment::FragmentBuffer;` to:

```rust
pub use fragment::{ChunkingSink, FragmentBuffer};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p hocuspocu-rs fragment::tests`
Expected: PASS (5 tests)

- [ ] **Step 6: Commit**

```bash
git add hocuspocu-rs/src/fragment.rs hocuspocu-rs/src/lib.rs
git commit -m "feat: add ChunkingSink outbound fragmentation wrapper"
```

---

## Task 5: Inbound fragment handling in Connection

**Files:**
- Modify: `hocuspocu-rs/src/connection.rs`

Inbound end-to-end behavior is verified by the interop test (Task 10); the reassembly
logic itself is unit-tested in Tasks 3–4. This task is the wiring, verified by `cargo build`
+ `cargo clippy` + existing tests staying green.

- [ ] **Step 1: Add imports and the buffer field**

In `hocuspocu-rs/src/connection.rs`, change the top imports:

```rust
use std::collections::{HashMap, VecDeque};
```
(the file currently imports only `VecDeque`), and add:

```rust
use crate::fragment::FragmentBuffer;
```

In `pub struct Connection`, add a field (after `processing: Mutex<()>,`):

```rust
    fragment_buffers: Mutex<HashMap<String, FragmentBuffer>>,
```

In `Connection::new`, in the `Arc::new(Self { … })` initializer (after `processing: Mutex::new(()),`):

```rust
            fragment_buffers: Mutex::new(HashMap::new()),
```

- [ ] **Step 2: Insert fragment handling in `process_messages`**

In `process_messages`, the loop currently runs the `before_handle_message_callback` block and then does `let receiver = MessageReceiver::new(raw_update, None);`. Insert the following block **between** the end of the `before_handle_message_callback` block (the closing `}` of `{ let cb = self.before_handle_message_callback.read().await; … }`) and the `let receiver = …` line:

```rust
            // ── Inbound fragment reassembly (types 100/101/102) ──
            // Unconditional: whether the client fragments is decided by the client's own
            // chunk size, so the server always reassembles. Fragment frames are consumed
            // here and never reach MessageReceiver; a completed series yields the original
            // frame, which is dispatched exactly like a normal message.
            let apply_bytes: Vec<u8> = {
                let mut fdec = Decoder::new(&raw_update);
                let _ = fdec.read_var_string(); // address (already validated above)
                match fdec.read_var_uint() {
                    Ok(t) if t == MessageType::FragmentStart as u64 => {
                        if let Ok(id) = fdec.read_var_string() {
                            let mut bufs = self.fragment_buffers.lock().await;
                            if bufs.contains_key(&id) {
                                tracing::warn!("FragmentStart for already-active fragment: {}", id);
                            }
                            bufs.insert(id, FragmentBuffer::new());
                        }
                        self.message_queue.lock().await.pop_front();
                        continue;
                    }
                    Ok(t) if t == MessageType::FragmentData as u64 => {
                        let id = fdec.read_var_string();
                        let index = fdec.read_var_uint();
                        let chunk = fdec.read_var_uint8_array();
                        if let (Ok(id), Ok(index), Ok(chunk)) = (id, index, chunk) {
                            let mut bufs = self.fragment_buffers.lock().await;
                            match bufs.get_mut(&id) {
                                Some(buf) => buf.add_chunk(index, chunk),
                                None => tracing::warn!("FragmentData for unknown fragment: {}", id),
                            }
                        }
                        self.message_queue.lock().await.pop_front();
                        continue;
                    }
                    Ok(t) if t == MessageType::FragmentEnd as u64 => {
                        let combined = if let Ok(id) = fdec.read_var_string() {
                            let mut bufs = self.fragment_buffers.lock().await;
                            match bufs.get_mut(&id) {
                                Some(buf) => {
                                    buf.mark_end();
                                    if buf.is_complete() {
                                        let bytes = buf.combine();
                                        bufs.remove(&id);
                                        Some(bytes)
                                    } else {
                                        None
                                    }
                                }
                                None => {
                                    tracing::warn!("FragmentEnd for unknown fragment: {}", id);
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        match combined {
                            Some(bytes) => bytes, // fall through and dispatch the reassembled frame
                            None => {
                                self.message_queue.lock().await.pop_front();
                                continue;
                            }
                        }
                    }
                    _ => raw_update.clone(), // normal (non-fragment) message
                }
            };
```

- [ ] **Step 3: Use the reassembled bytes for dispatch**

Change the existing line:

```rust
            let receiver = MessageReceiver::new(raw_update, None);
```
to:

```rust
            let receiver = MessageReceiver::new(apply_bytes, None);
```

(Leave the rest of the loop — the `before_handle_message` call on `raw_update`, the result
handling, and the trailing `queue.pop_front()` — unchanged.)

- [ ] **Step 4: Build, lint, and run the existing suite**

Run:
```bash
cargo build -p hocuspocu-rs
cargo clippy -p hocuspocu-rs --all-targets
cargo test -p hocuspocu-rs
```
Expected: build + clippy clean; all existing tests still pass (50 baseline + the fragment unit tests added so far).

- [ ] **Step 5: Commit**

```bash
git add hocuspocu-rs/src/connection.rs
git commit -m "feat: reassemble inbound fragment frames in Connection"
```

---

## Task 6: Wire global chunk size into connections

**Files:**
- Modify: `hocuspocu-rs/src/hocuspocus.rs` (`configure()`)
- Modify: `hocuspocu-rs/src/client_connection.rs` (`setup_new_connection`)

- [ ] **Step 1: Copy the field in `configure()`**

In `hocuspocu-rs/src/hocuspocus.rs`, inside `pub async fn configure`, after the line
`config.unload_immediately = configuration.unload_immediately;`, add:

```rust
        config.message_chunk_size = configuration.message_chunk_size;
```

- [ ] **Step 2: Wrap the sink in `setup_new_connection`**

In `hocuspocu-rs/src/client_connection.rs`, ensure these are imported (the file already
imports `ConnectionConfiguration`/types via `use crate::types::*;` or similar — confirm
`WebSocketSink` and `std::sync::Arc` are in scope; add if missing):

```rust
use crate::fragment::ChunkingSink;
```

In `setup_new_connection`, the code currently reads:

```rust
        document
            .add_connection(
                &conn_id,
                &hook_payload.socket_id,
                hook_payload.connection_config.read_only,
                &message_address,
                self.websocket.clone(),
            )
            .await;

        let connection = Connection::new(
            conn_id.clone(),
            self.websocket.clone(),
            hook_payload.request.clone(),
            document.clone(),
            hook_payload.socket_id.clone(),
            hook_payload.context.clone(),
            hook_payload.connection_config.read_only,
            session_id,
            hook_payload.provider_version.clone(),
        );
```

Replace the two `self.websocket.clone()` arguments with a once-wrapped sink. Insert this
just before the `document.add_connection(...)` call:

```rust
        let chunk_size = self.hocuspocus.configuration.read().await.message_chunk_size;
        let sink: Arc<dyn WebSocketSink> = if chunk_size > 0 {
            Arc::new(ChunkingSink::new(self.websocket.clone(), chunk_size))
        } else {
            self.websocket.clone()
        };
```

Then change `self.websocket.clone()` → `sink.clone()` in the `add_connection` call, and
`self.websocket.clone()` → `sink` in the `Connection::new` call.

- [ ] **Step 3: Build, lint, test**

Run:
```bash
cargo build -p hocuspocu-rs
cargo clippy -p hocuspocu-rs --all-targets
cargo test -p hocuspocu-rs
```
Expected: clean; all tests pass.

- [ ] **Step 4: Commit**

```bash
git add hocuspocu-rs/src/hocuspocus.rs hocuspocu-rs/src/client_connection.rs
git commit -m "feat: install ChunkingSink when Configuration.message_chunk_size is set"
```

---

## Task 7: HP_CHUNK env in the example server

**Files:**
- Modify: `hocuspocu-rs/examples/server.rs`

- [ ] **Step 1: Read the env and set the field**

In `hocuspocu-rs/examples/server.rs`, near where `debounce` is read (before the
`Server::with_config` call), add:

```rust
    let message_chunk_size: usize = std::env::var("HP_CHUNK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
```

In the `Configuration { … }` literal inside `Server::with_config`, add the field next to
`extensions,`:

```rust
            message_chunk_size,
```

(Keep the trailing `..Configuration::default()`.)

- [ ] **Step 2: Update the doc comment**

At the top doc comment listing env vars, add a line documenting `HP_CHUNK` (bytes; `0` =
off; e.g. `HP_CHUNK=61440` for 60 KB).

- [ ] **Step 3: Build the example**

Run: `cargo build -p hocuspocu-rs --example server`
Expected: builds.

- [ ] **Step 4: Commit**

```bash
git add hocuspocu-rs/examples/server.rs
git commit -m "feat: HP_CHUNK env to enable outbound chunking in example server"
```

---

## Task 8: Full Rust verification gate

**Files:** none (verification only)

- [ ] **Step 1: fmt + clippy + tests**

Run:
```bash
cargo fmt --all
cargo clippy --all-targets
cargo test
```
Expected: fmt makes no further changes after re-run; clippy reports no new warnings in the
fragment code; all tests pass (baseline 50 + new fragment tests). If clippy flags the new
code, fix inline (prefer a fix over a blanket allow) and re-run.

- [ ] **Step 2: Commit any fmt/clippy fixups**

```bash
git add -A
git commit -m "chore: fmt + clippy for fragment chunking" || echo "nothing to commit"
```

---

## Task 9: Clone the TypeScript provider, renumbered

**Files:**
- Create: `provider/**` (copied), `provider/package.json`, `provider/tsconfig.json`

- [ ] **Step 1: Copy the walla provider into `provider/`**

Run (from worktree root):
```bash
cp -R /Users/kimuyb/Paprika/dev/walla-next/apps/dashboard/utils/yjs/hocuspocus-provider provider
ls provider
```
Expected: `provider/` contains `types.ts`, `MessageSender.ts`, `MessageReceiver.ts`,
`FragmentBuffer.ts`, `HocuspocusProvider.ts`, `HocuspocusProviderWebsocket.ts`,
`OutgoingMessages/`, `index.ts`, etc.

- [ ] **Step 2: Renumber the fragment types**

Run:
```bash
sed -i '' \
  -e 's/FragmentStart = 10,/FragmentStart = 100,/' \
  -e 's/FragmentData = 11,/FragmentData = 101,/' \
  -e 's/FragmentEnd = 12,/FragmentEnd = 102,/' \
  provider/types.ts
grep -nE "Fragment(Start|Data|End) = " provider/types.ts
```
Expected: shows `FragmentStart = 100`, `FragmentData = 101`, `FragmentEnd = 102`.

- [ ] **Step 3: Verify nothing else hardcodes 10/11/12 for fragments**

Run:
```bash
grep -rnE "= 1[012][,;]|10\)|11\)|12\)" provider --include=*.ts | grep -iE "fragment" || echo "no other hardcoded fragment numbers (good)"
```
Expected: no fragment-related hardcoded `10/11/12` outside `types.ts` (fragment logic
references `MessageType.Fragment*` symbolically).

- [ ] **Step 4: Add package.json**

Create `provider/package.json`:

```json
{
  "name": "@hocuspocu-rs/provider",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "description": "TypeScript Hocuspocus provider with fragment chunking (types 100/101/102), source of truth for hocuspocu-rs",
  "main": "index.ts",
  "dependencies": {
    "@hocuspocus/common": "^3.4.0",
    "lib0": "^0.2.108",
    "y-protocols": "^1.0.6",
    "yjs": "^13.6.27"
  },
  "peerDependencies": {
    "ws": "^8.18.0"
  }
}
```

- [ ] **Step 5: Add a tsconfig for typechecking**

Create `provider/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": false,
    "skipLibCheck": true,
    "noEmit": true,
    "esModuleInterop": true,
    "types": ["node"]
  },
  "include": ["**/*.ts"]
}
```

- [ ] **Step 6: Install deps and typecheck**

Run:
```bash
cd provider && npm install && npm install --no-save ws @types/node typescript && npx tsc --noEmit; cd ..
```
Expected: typecheck passes (warnings tolerated). If a module is missing, add it to
`dependencies` and re-run. Do not edit logic — only fix missing deps.

- [ ] **Step 7: Commit**

```bash
git add provider
git commit -m "feat: add in-repo TypeScript provider with fragment chunking (100/101/102)"
```

---

## Task 10: Fragmentation interop test (end-to-end)

**Files:**
- Modify: `interop/package.json` (add `tsx`)
- Create: `interop/fragment_interop.ts`

This proves the renumbered TS provider and the Rust server fragment + reassemble correctly
in both directions (client→server inbound reassembly, server→client outbound chunking).

- [ ] **Step 1: Add tsx + provider deps to interop**

In `interop/package.json`, add to `dependencies`: `"lib0": "^0.2.108"` and
`"@hocuspocus/common": "^3.4.0"`, and to a new `devDependencies`: `"tsx": "^4.22.0"`. Add a
script `"fragment": "tsx fragment_interop.ts"`. Then run:
```bash
cd interop && npm install; cd ..
```

- [ ] **Step 2: Write the interop test**

Create `interop/fragment_interop.ts`:

```ts
// Drives the Rust example server (started with HP_CHUNK set) using the in-repo TS provider,
// syncing a document far larger than the chunk size to force fragmentation both ways.
import * as Y from 'yjs';
import { WebSocket } from 'ws';
import { HocuspocusProvider, HocuspocusProviderWebsocket } from '../provider/index';

const URL = process.env.HP_URL ?? 'ws://127.0.0.1:8088';
const CHUNK = Number(process.env.HP_CHUNK ?? 61440); // 60 KB, must match the server
const DOC = 'fragment-doc';
const BIG_LEN = CHUNK * 3; // ~180 KB of text → multiple fragments

function makeProvider(doc: Y.Doc) {
  const websocket = new HocuspocusProviderWebsocket({ url: URL, WebSocketPolyfill: WebSocket as any });
  return new HocuspocusProvider({
    websocketProvider: websocket,
    name: DOC,
    document: doc,
    token: 'interop',
    messageChunkSize: CHUNK,
  } as any);
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  // Client A: write a large value → forces inbound (client→server) fragmentation.
  const docA = new Y.Doc();
  const provA = makeProvider(docA);
  await wait(1000);
  docA.getText('t').insert(0, 'x'.repeat(BIG_LEN));
  await wait(1500);

  // Client B: joins fresh → server sends the large state → outbound (server→client)
  // fragmentation, reassembled by B.
  const docB = new Y.Doc();
  const provB = makeProvider(docB);
  await wait(2000);

  const lenA = docA.getText('t').length;
  const lenB = docB.getText('t').length;
  provA.destroy();
  provB.destroy();

  if (lenA !== BIG_LEN || lenB !== BIG_LEN) {
    console.error(`FAIL: expected ${BIG_LEN}, got A=${lenA} B=${lenB}`);
    process.exit(1);
  }
  console.log(`OK: both clients converged on ${BIG_LEN} chars through fragmentation`);
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
```

> If the provider's constructor option names differ (`websocketProvider`, `messageChunkSize`,
> `token`), align them with `provider/HocuspocusProvider.ts` and
> `provider/HocuspocusProviderWebsocket.ts` — do not change the provider, change the test.

- [ ] **Step 3: Run it against a chunking-enabled server**

Run:
```bash
cargo build -p hocuspocu-rs --example server
HP_PORT=8088 HP_CHUNK=61440 ./target/debug/examples/server &
SERVER_PID=$!
sleep 1
( cd interop && HP_URL=ws://127.0.0.1:8088 HP_CHUNK=61440 npm run fragment )
RESULT=$?
kill $SERVER_PID
test $RESULT -eq 0
```
Expected: prints `OK: both clients converged …` and exits 0.

- [ ] **Step 4: Sanity-check the existing interop still passes (opt-in proof)**

Run (server WITHOUT chunking, real upstream provider unaffected):
```bash
HP_PORT=8089 ./target/debug/examples/server &
SERVER_PID=$!
sleep 1
( cd interop && HP_URL=ws://127.0.0.1:8089 HP_USERS=4 node multi_user_interop.mjs )
RESULT=$?
kill $SERVER_PID
test $RESULT -eq 0
```
Expected: existing interop passes — default behavior unchanged.

- [ ] **Step 5: Commit**

```bash
git add interop/package.json interop/package-lock.json interop/fragment_interop.ts
git commit -m "test: end-to-end fragmentation interop with in-repo TS provider"
```

---

## Task 11: Documentation

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Document the protocol extension**

In `CLAUDE.md`, in the `### Message protocol` section, after the `MessageType` list, add a
paragraph:

```markdown
**Fragment chunking (opt-in divergence from upstream).** `FragmentStart=100`,
`FragmentData=101`, `FragmentEnd=102` carry an application-level fragmentation protocol for
networks that drop large WebSocket messages. Numbers sit above the upstream range
(`0..=10`, incl. `Ping=9`/`Pong=10`) so upstream compatibility is preserved. Outbound
chunking is enabled by `Configuration.message_chunk_size` (bytes; `0` = off, the default)
and installed as a transparent `ChunkingSink` around the connection's `WebSocketSink`.
Inbound reassembly (`FragmentBuffer` on `Connection`) is **always on** — the client decides
whether to fragment. The matching client is the in-repo `provider/` (TypeScript); the two
must be deployed in lockstep (a client using the old `10/11/12` numbering is incompatible).
```

- [ ] **Step 2: Note the provider directory**

In the workspace-layout / `interop/` description area, add a sentence: the top-level
`provider/` directory is the source-of-truth TypeScript provider (fragment chunking), run
under `tsx` for the fragmentation interop test.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: document fragment chunking protocol and provider/"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** message types (T1), config (T1/T6), builders (T2), `FragmentBuffer`
  (T3), `ChunkingSink` (T4), inbound wiring (T5), sink install (T6), example env (T7),
  verification (T8), provider clone+renumber (T9), interop both directions (T10), docs (T11).
- **Type consistency:** builder `index: usize` writes `as u64`; `FragmentBuffer` keys are
  `u64`; inbound reads `read_var_uint() -> u64`. `ChunkingSink::new(Arc<dyn WebSocketSink>,
  usize)`. `MessageType::Fragment* as u64` used for comparisons.
- **No resource caps** on inbound (strict fork parity) — by design; documented trust
  assumption (authenticated, established connections only).
- **`Connection::send` / `Connection::new` signatures are unchanged** — the wrapped sink is
  passed in by `setup_new_connection`.
