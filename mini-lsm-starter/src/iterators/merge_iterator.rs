use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            .filter(|x| x.is_valid())
            .enumerate()
            .map(|(id, x)| HeapWrapper(id, x))
            .collect();
        let current = iters.pop().map(|x| HeapWrapper(x.0, x.1));
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map_or(KeySlice::default(), |x| x.1.key())
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().map_or(b"", |x| x.1.value())
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map_or(false, |x| x.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }

        let key = self.current.as_ref().unwrap().1.key();
        while let Some(mut iter) = self.iters.pop().map(|x| HeapWrapper(x.0, x.1)) {
            if iter.1.key() <= key {
                iter.1.next()?;
                if iter.1.is_valid() {
                    self.iters.push(iter);
                }
            } else {
                if iter.1.is_valid() {
                    self.iters.push(iter);
                }
                break;
            }
        }

        self.current.as_mut().unwrap().1.next()?;
        if self.current.as_mut().unwrap().1.is_valid() {
            self.iters.push(self.current.take().unwrap());
        }
        self.current = self.iters.pop().map(|x| HeapWrapper(x.0, x.1));
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let mut count = 0;
        count += self
            .current
            .as_ref()
            .map_or(0, |x| x.1.num_active_iterators());
        for x in &self.iters {
            count += x.1.num_active_iterators();
        }
        count
    }
}
