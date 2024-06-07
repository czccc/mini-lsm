#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut size = 4;
        for meta in block_meta {
            size += 8 + meta.first_key.raw_len() + meta.last_key.raw_len();
        }
        buf.reserve(size);
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_ref().len() as u16);
            buf.put(meta.first_key.as_key_slice().into_inner());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_ref().len() as u16);
            buf.put(meta.last_key.as_key_slice().into_inner());
            buf.put_u64(meta.last_key.ts());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let block_meta_num = buf.get_u32() as usize;
        let mut block_meta = Vec::with_capacity(block_meta_num);
        for _ in 0..block_meta_num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key_data = buf.copy_to_bytes(first_key_len);
            let first_key_ts = buf.get_u64();
            let first_key = KeyBytes::from_bytes_with_ts(first_key_data, first_key_ts);
            let last_key_len = buf.get_u16() as usize;
            let last_key_data = buf.copy_to_bytes(last_key_len);
            let last_key_ts = buf.get_u64();
            let last_key = KeyBytes::from_bytes_with_ts(last_key_data, last_key_ts);
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let (bloom_filter_offset, bloom) = {
            let bloom_filter_offset = file.read(file.1 - 4, 4)?.as_slice().get_u32();
            let bloom_filter_size = file.1 - 4 - (bloom_filter_offset) as u64;
            let bloom_filter_buf = file.read(bloom_filter_offset as u64, bloom_filter_size)?;
            let mut bloom_filter_buf: &[u8] = bloom_filter_buf.as_ref();
            let bloom_buf = bloom_filter_buf[..(bloom_filter_size - 4) as usize].as_ref();
            bloom_filter_buf.advance((bloom_filter_size - 4) as usize);
            let bloom_filter_checksum = bloom_filter_buf.get_u32();
            if bloom_filter_checksum != crc32fast::hash(bloom_buf) {
                return Err(anyhow!("checksum invalid!"));
            }
            let bloom = Bloom::decode(bloom_buf)?;
            (bloom_filter_offset, bloom)
        };

        let (block_meta_offset, block_meta) = {
            let block_meta_offset = file
                .read((bloom_filter_offset - 4) as u64, 4)?
                .as_slice()
                .get_u32();
            let block_meta_size = (bloom_filter_offset - 4 - block_meta_offset) as u64;
            let block_meta_buf = file.read(block_meta_offset as u64, block_meta_size)?;
            let mut block_meta_buf: &[u8] = block_meta_buf.as_ref();

            let meta_buf = block_meta_buf[..(block_meta_size - 4) as usize].as_ref();
            block_meta_buf.advance((block_meta_size - 4) as usize);
            let block_meta_checksum = block_meta_buf.get_u32();
            if block_meta_checksum != crc32fast::hash(meta_buf) {
                return Err(anyhow!("checksum invalid!"));
            }
            let block_meta = BlockMeta::decode_block_meta(
                file.read(block_meta_offset as u64, block_meta_size)?
                    .as_slice(),
            );

            (block_meta_offset, block_meta)
        };

        let first_key = block_meta
            .first()
            .map_or(KeyBytes::default(), |x| x.first_key.clone());
        let last_key = block_meta
            .last()
            .map_or(KeyBytes::default(), |x| x.last_key.clone());

        let ss_table = Self {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        };
        Ok(ss_table)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let next_offset = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);

        let block_size = next_offset - offset;
        let block_data_with_checksum = self.file.read(offset as u64, block_size as u64)?;
        let mut block_data_with_checksum = block_data_with_checksum.as_slice();
        let block_data = block_data_with_checksum[..block_size - 4].as_ref();

        block_data_with_checksum.advance(block_size - 4);
        let checksum = block_data_with_checksum.get_u32();
        if checksum != crc32fast::hash(block_data) {
            return Err(anyhow!("checksum invalid!"));
        }

        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = &self.block_cache {
            block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        if key < self.first_key.as_key_slice() {
            return 0;
        }
        for (i, meta) in self.block_meta.iter().enumerate() {
            if key <= meta.last_key.as_key_slice() {
                return i;
            }
        }
        self.block_meta.len() - 1
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.first_key.as_key_slice() <= KeySlice::from_slice(key, TS_RANGE_END)
            && KeySlice::from_slice(key, TS_RANGE_BEGIN) <= self.last_key.as_key_slice()
            && self
                .bloom
                .as_ref()
                .is_some_and(|bloom| bloom.may_contain(farmhash::fingerprint32(key)))
    }

    pub(crate) fn contains_range(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        match upper {
            Bound::Excluded(key) if key <= self.first_key().key_ref() => {
                return false;
            }
            Bound::Included(key) if key < self.first_key().key_ref() => {
                return false;
            }
            _ => {}
        }
        match lower {
            Bound::Excluded(key) if key >= self.last_key().key_ref() => {
                return false;
            }
            Bound::Included(key) if key > self.last_key().key_ref() => {
                return false;
            }
            _ => {}
        }
        true
    }
}
