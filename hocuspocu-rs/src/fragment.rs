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

    #[test]
    fn complete_with_single_chunk() {
        let mut b = FragmentBuffer::new();
        b.add_chunk(0, vec![42, 43]);
        b.mark_end();
        assert!(b.is_complete());
        assert_eq!(b.combine(), vec![42, 43]);
    }
}
