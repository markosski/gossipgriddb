use std::iter::Peekable;

use sstable::Table;

use crate::{item::Item, store::sstable_store::partition_store::decode_item};

pub(crate) struct OwningSSIterator {
    // Table is boxed so its memory address is stable
    table: Box<Table>,
    // We store the pointer to the iterator and cast it to have an erased lifetime.
    // This is safe because `iter` only references `table`, which is moved with us
    // and is Boxed (so it doesn't change address).
    iter: *mut dyn sstable::SSIterator,
}

impl OwningSSIterator {
    pub(crate) fn new(table: Table) -> Self {
        let boxed_table = Box::new(table);
        let iter_ptr = {
            // We create the iterator from the boxed table.
            let iter = boxed_table.iter();

            // We cast the boxed iterator into a raw pointer to a trait object.
            // By casting `*mut dyn sstable::SSIterator` we erase the specific underlying
            // iterator type and its lifetime cleanly.
            let iter_box: Box<dyn sstable::SSIterator> = Box::new(iter);
            Box::into_raw(iter_box)
        };

        Self {
            table: boxed_table,
            iter: iter_ptr,
        }
    }
}

impl Drop for OwningSSIterator {
    fn drop(&mut self) {
        unsafe {
            // Drop the iterator before the table
            let _ = Box::from_raw(self.iter);
        }
    }
}

impl Iterator for OwningSSIterator {
    type Item = (Vec<u8>, Item);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let iter = &mut *self.iter;
            if let Some((k, v)) = iter.next() {
                // Ensure decoding failure doesn't panic during iteration, but logs it
                if let Ok(item) = decode_item(v.as_slice()) {
                    return Some((k.to_vec(), item));
                }
            }
        }
        None
    }
}

pub(crate) struct MergeIterator<'a> {
    iters: Vec<Peekable<Box<dyn Iterator<Item = (Vec<u8>, Item)> + 'a>>>,
}

impl<'a> MergeIterator<'a> {
    pub(crate) fn new(iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Item)> + 'a>>) -> Self {
        Self {
            iters: iters.into_iter().map(|it| it.peekable()).collect(),
        }
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = (Vec<u8>, Item);

    fn next(&mut self) -> Option<Self::Item> {
        let mut min_key: Option<&[u8]> = None;

        // Find out the "minimum" key currently available
        for it in &mut self.iters {
            if let Some((k, _)) = it.peek() {
                match min_key {
                    None => min_key = Some(k),
                    Some(m_k) => {
                        if k.as_slice() < m_k {
                            min_key = Some(k)
                        }
                    }
                }
            }
        }

        let min_key = min_key?.to_vec();

        // There might be multiple iterators with this same exact key.
        // We evaluate iterators in order from oldest to newest.
        // Thus, the value we actually want to yield is the one from the NEWEST iterator
        // (the highest index in the `iters` array that has this key).
        let mut result = None;

        for it in &mut self.iters {
            if let Some((k, _)) = it.peek() {
                if k == &min_key {
                    // Consume it! and set it as the result.
                    // Later iterators will overwrite earlier ones since they are newer.
                    result = it.next();
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::HLC;
    use crate::item::ItemStatus;

    fn test_item(msg: &str) -> Item {
        Item {
            message: msg.as_bytes().to_vec(),
            status: ItemStatus::Active,
            hlc: HLC::new(),
        }
    }

    #[test]
    fn test_merge_iterator_merging_and_overriding() {
        let source1: Vec<(Vec<u8>, Item)> = vec![
            (b"a".to_vec(), test_item("v1-a")),
            (b"b".to_vec(), test_item("v1-b")),
        ];
        let source2: Vec<(Vec<u8>, Item)> = vec![
            (b"b".to_vec(), test_item("v2-b")),
            (b"c".to_vec(), test_item("v2-c")),
        ];

        // Newer source is later in the vector
        let iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Item)>>> = vec![
            Box::new(source1.into_iter()),
            Box::new(source2.into_iter()),
        ];

        let mut merge_iter = MergeIterator::new(iters);

        // "a" should come from source1
        let (k, v) = merge_iter.next().expect("should have 'a'");
        assert_eq!(k, b"a");
        assert_eq!(v.message, b"v1-a");

        // "b" should come from source2 (overriding source1)
        let (k, v) = merge_iter.next().expect("should have 'b'");
        assert_eq!(k, b"b");
        assert_eq!(v.message, b"v2-b");

        // "c" should come from source2
        let (k, v) = merge_iter.next().expect("should have 'c'");
        assert_eq!(k, b"c");
        assert_eq!(v.message, b"v2-c");

        assert!(merge_iter.next().is_none());
    }

    #[test]
    fn test_merge_iterator_complex_sorting() {
        let source1: Vec<(Vec<u8>, Item)> = vec![
            (b"a".to_vec(), test_item("v1-a")),
            (b"aa".to_vec(), test_item("v1-aa")),
        ];
        let source2: Vec<(Vec<u8>, Item)> = vec![
            (b"a".to_vec(), test_item("v2-a")),
            (b"ab".to_vec(), test_item("v2-ab")),
        ];

        let iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Item)>>> = vec![
            Box::new(source1.into_iter()),
            Box::new(source2.into_iter()),
        ];

        let mut merge_iter = MergeIterator::new(iters);

        // "a" from source2 (overriding)
        let (k, v) = merge_iter.next().unwrap();
        assert_eq!(k, b"a");
        assert_eq!(v.message, b"v2-a");

        // "aa" from source1
        let (k, v) = merge_iter.next().unwrap();
        assert_eq!(k, b"aa");
        assert_eq!(v.message, b"v1-aa");

        // "ab" from source2
        let (k, v) = merge_iter.next().unwrap();
        assert_eq!(k, b"ab");
        assert_eq!(v.message, b"v2-ab");

        assert!(merge_iter.next().is_none());
    }

    #[test]
    fn test_merge_iterator_with_empty_sources() {
        let iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Item)>>> = vec![
            Box::new(std::iter::empty()),
            Box::new(std::iter::empty()),
        ];
        let mut merge_iter = MergeIterator::new(iters);
        assert!(merge_iter.next().is_none());
    }

    #[test]
    fn test_merge_iterator_single_source() {
        let source = vec![(b"a".to_vec(), test_item("v1"))];
        let iters: Vec<Box<dyn Iterator<Item = (Vec<u8>, Item)>>> =
            vec![Box::new(source.into_iter())];
        let mut merge_iter = MergeIterator::new(iters);
        let (k, v) = merge_iter.next().unwrap();
        assert_eq!(k, b"a");
        assert_eq!(v.message, b"v1");
        assert!(merge_iter.next().is_none());
    }
}
