use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use crc32fast;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::{bloom::Bloom, FileObject},
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            let first_key = std::mem::take(&mut self.first_key);
            let last_key = std::mem::take(&mut self.last_key);
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(first_key.into()),
                last_key: KeyBytes::from_bytes(last_key.into()),
            });
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

            let block_data = builder.build().encode();
            let checksum = crc32fast::hash(block_data.as_ref());
            self.data.put(block_data);
            self.data.put_u32(checksum);

            let _ = self.builder.add(key, value);
        }
        if self.first_key.is_empty() {
            self.first_key.put(key.into_inner());
        }
        self.last_key = key.to_key_vec().into_inner();
        self.key_hashes
            .push(farmhash::fingerprint32(key.into_inner()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(self.first_key.into()),
            last_key: KeyBytes::from_bytes(self.last_key.into()),
        });

        let block_data = self.builder.build().encode();
        let checksum = crc32fast::hash(block_data.as_ref());
        self.data.put(block_data);
        self.data.put_u32(checksum);

        let mut meta_buf = Vec::new();
        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(self.meta.as_slice(), &mut meta_buf);
        self.data.put(meta_buf.as_ref());
        self.data.put_u32(crc32fast::hash(meta_buf.as_ref()));
        self.data.put_u32(block_meta_offset as u32);

        let bloom_filter_offset = self.data.len();

        let mut bloom_filter_buf = Vec::new();
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        bloom.encode(&mut bloom_filter_buf);

        self.data.put(bloom_filter_buf.as_ref());
        self.data
            .put_u32(crc32fast::hash(bloom_filter_buf.as_ref()));
        self.data.put_u32(bloom_filter_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data)?;
        SsTable::open(id, block_cache, file)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
