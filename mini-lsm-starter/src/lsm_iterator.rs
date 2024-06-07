use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeyBytes, KeySlice},
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
}

// fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
//     match bound {
//         Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
//             Bytes::copy_from_slice(x.key_ref()),
//             x.ts(),
//         )),
//         Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
//             Bytes::copy_from_slice(x.key_ref()),
//             x.ts(),
//         )),
//         Bound::Unbounded => Bound::Unbounded,
//     }
// }

impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        Ok(Self { inner: iter, upper })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
            && match &self.upper {
                Bound::Included(key) => self.inner.key().key_ref() <= key,
                Bound::Excluded(key) => self.inner.key().key_ref() < key,
                Bound::Unbounded => true,
            }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        let prev_key = self.inner.key().key_ref().to_vec();
        self.inner.next()?;
        while self.inner.is_valid()
            && (self.inner.value().is_empty() || self.inner.key().key_ref() == prev_key)
        {
            self.inner.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            Err(anyhow::anyhow!("Iter has errored!"))
        } else if self.iter.is_valid() {
            self.iter.next().map_err(|e| {
                self.has_errored = true;
                e
            })
        } else {
            Ok(())
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
