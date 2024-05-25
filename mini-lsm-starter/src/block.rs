mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buffer = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        for item in &self.data {
            buffer.put_u8(*item);
        }
        for item in &self.offsets {
            buffer.put_u16(*item);
        }
        buffer.put_u16(self.offsets.len() as u16);
        buffer.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(mut data: &[u8]) -> Self {
        if data.len() <= 2 {
            return Self {
                data: vec![],
                offsets: vec![],
            };
        }

        let mut num_elements = data;
        num_elements.advance(data.len() - 2);
        let num_elements = num_elements.get_u16() as usize;

        let mut block = Block {
            data: Vec::with_capacity(data.len() - num_elements * 2 - 2),
            offsets: Vec::with_capacity(num_elements),
        };

        for _ in 0..(data.len() - num_elements * 2 - 2) {
            block.data.push(data.get_u8());
        }

        for _ in 0..num_elements {
            block.offsets.push(data.get_u16());
        }
        block
    }
}
