//! Application-level message fragmentation: splitting large outbound WebSocket
//! messages into `FragmentStart/Data/End` frames (`ChunkingSink`) and reassembling
//! inbound fragment frames (`FragmentBuffer`). Wire-compatible with the matching
//! TypeScript provider in `provider/`.

use std::collections::HashMap;
use std::sync::Arc;

use hocuspocus_common::WsReadyState;

use crate::encoding::Decoder;
use crate::outgoing_message::OutgoingMessage;
use crate::types::WebSocketSink;

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

    /// `index` is the wire fragment index (a lib0 varuint), decoded as `u64` on the
    /// inbound path. Outbound (`OutgoingMessage::write_fragment_data`) takes `usize` to
    /// match `.enumerate()`; the two sides never call each other, so no cast is needed.
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
        max_index
            .checked_add(1)
            .is_some_and(|expected| self.chunks.len() as u64 == expected)
    }

    /// Concatenate chunks in index order. Caller must check `is_complete()` first.
    pub fn combine(&self) -> Vec<u8> {
        debug_assert!(
            self.is_complete(),
            "combine() called on an incomplete FragmentBuffer"
        );
        let mut indices: Vec<&u64> = self.chunks.keys().collect();
        indices.sort_unstable();
        let mut out = Vec::new();
        for i in indices {
            out.extend_from_slice(&self.chunks[i]);
        }
        out
    }
}

/// A `WebSocketSink` decorator that splits any outbound message larger than `chunk_size`
/// into `FragmentStart` + N×`FragmentData` + `FragmentEnd` frames, all forwarded to the
/// inner sink. Messages `<= chunk_size` (and any message when `chunk_size == 0`) pass
/// through unchanged.
///
/// `send` does not hold a lock across the frame sequence, so two concurrent large sends on
/// the same connection may interleave their frames on the wire. That is safe: every frame
/// carries `unique_id` and the receiver reassembles per id (see `FragmentBuffer`);
/// non-fragmented messages are a single `inner.send` and cannot be split.
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
        if self.chunk_size == 0 || data.len() <= self.chunk_size {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{write_var_string, write_var_uint, Decoder};
    use crate::types::MessageType;
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
                t if t == MessageType::FragmentStart as u64 => {}
                t if t == MessageType::FragmentData as u64 => {
                    let idx = d.read_var_uint().unwrap();
                    let chunk = d.read_var_uint8_array().unwrap();
                    buf.add_chunk(idx, chunk);
                }
                t if t == MessageType::FragmentEnd as u64 => buf.mark_end(),
                other => panic!("unexpected frame type {}", other),
            }
        }
        assert_eq!(last_type, MessageType::FragmentEnd as u64, "last frame must be FragmentEnd");
        assert!(buf.is_complete());
        assert_eq!(buf.combine(), msg, "reassembled bytes must equal original message");
    }

    #[test]
    fn zero_chunk_size_passes_through() {
        let cap = Arc::new(CaptureSink { sent: Mutex::new(Vec::new()) });
        let sink = ChunkingSink::new(cap.clone(), 0);
        let msg = build_message(50);
        sink.send(msg.clone()).unwrap();
        let sent = cap.sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "chunk_size 0 must disable chunking");
        assert_eq!(sent[0], msg);
    }

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

    #[test]
    fn complete_with_single_chunk() {
        let mut b = FragmentBuffer::new();
        b.add_chunk(0, vec![42, 43]);
        b.mark_end();
        assert!(b.is_complete());
        assert_eq!(b.combine(), vec![42, 43]);
    }
}
