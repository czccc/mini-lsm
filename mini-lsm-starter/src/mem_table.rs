#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `KeySlice` from a bound of `&[u8]`.
pub(crate) fn map_bound_u8(bound: Bound<&[u8]>) -> Bound<KeySlice> {
    match bound {
        Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, TS_DEFAULT)),
        Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_DEFAULT)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `KeyBytes` from a bound of `KeySlice`.
pub(crate) fn map_bound_slice(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(key) => Bound::Included(key.to_key_vec().into_key_bytes()),
        Bound::Excluded(key) => Bound::Excluded(key.to_key_vec().into_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skiplist = SkipMap::new();
        let wal = Wal::recover(path, &skiplist)?;
        Ok(Self {
            map: Arc::new(skiplist),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(
            KeySlice::for_testing_from_slice_with_ts(key, TS_DEFAULT),
            value,
        )
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::for_testing_from_slice_with_ts(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(map_bound_u8(lower), map_bound_u8(upper))
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.map
            .get(&key.to_key_vec().into_key_bytes())
            .map(|x| x.value().to_owned())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.approximate_size.fetch_add(
            key.raw_len() + value.len(),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.map.insert(
            key.to_key_vec().into_key_bytes(),
            Bytes::copy_from_slice(value),
        );
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let lower = map_bound_slice(lower);
        let upper = map_bound_slice(upper);
        let mut iter = MemTableIterator::new(
            self.map.clone(),
            |map| map.range((lower, upper)),
            (KeyBytes::new(), Bytes::new()),
        );
        let _ = iter.next();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(
                KeySlice::from_slice(entry.key().key_ref(), entry.key().ts()),
                entry.value(),
            );
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|iter| {
            let entry = iter.iter.next();
            match entry {
                Some(entry) => {
                    *iter.item = (entry.key().clone(), entry.value().clone());
                    Ok(())
                }
                None => {
                    *iter.item = (KeyBytes::new(), Bytes::new());
                    Ok(())
                }
            }
        })
    }
}
