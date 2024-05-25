use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut current = None;
        if let Some(sstable) = sstables.first() {
            let iter = SsTableIterator::create_and_seek_to_first(sstable.clone())?;
            let _ = current.insert(iter);
        }
        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut next_sst_idx = 1;
        for x in &sstables {
            if x.last_key().as_key_slice() < key {
                next_sst_idx += 1;
            } else {
                break;
            }
        }
        let mut current = None;
        if let Some(sstable) = sstables.get(next_sst_idx - 1) {
            let iter = SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
            let _ = current.insert(iter);
        }
        Ok(Self {
            current,
            next_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .expect("access iterator when valid")
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("access iterator when valid")
            .value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map_or(false, |x| x.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_mut().unwrap().is_valid() {
            if self.next_sst_idx >= self.sstables.len() {
                return Ok(());
            }
            let iter = SsTableIterator::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?;
            let _ = self.current.insert(iter);
            self.next_sst_idx += 1;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
