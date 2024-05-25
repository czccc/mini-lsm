use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    use_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, use_a: true };
        iter.update_use();
        Ok(iter)
    }

    fn skip_same(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.b.key() == self.a.key() {
            if self.use_a {
                self.b.next()?;
            } else {
                self.a.next()?;
            }
        }
        Ok(())
    }

    fn update_use(&mut self) {
        if self.a.is_valid() && self.b.is_valid() {
            self.use_a = self.a.key() <= self.b.key();
        } else {
            self.use_a = self.a.is_valid();
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.use_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.skip_same()?;
            self.a.next()?;
        } else {
            self.skip_same()?;
            self.b.next()?;
        }
        self.update_use();
        Ok(())
    }
}
