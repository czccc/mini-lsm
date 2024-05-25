use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let current_size = self.data.len() + self.offsets.len() * 2 + 2;
        if current_size + key.len() + value.len() + 6 > self.block_size
            && !self.first_key.is_empty()
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        if self.first_key.is_empty() {
            self.first_key.append(key.into_inner());
            self.data.put_u16((key.len()) as u16);
            self.data.put(key.raw_ref());
        } else {
            let mut overlap_len = 0;
            for i in 0..key.len().min(self.first_key.len()) {
                if key.raw_ref()[i] == self.first_key.raw_ref()[i] {
                    overlap_len += 1;
                } else {
                    break;
                }
            }
            self.data.put_u16(overlap_len as u16);
            self.data.put_u16((key.len() - overlap_len) as u16);
            self.data.put(&key.raw_ref()[overlap_len..]);
        }
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
