use crate::{clock::HLC, store::StorageKey};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct Item {
    pub message: Vec<u8>,
    pub status: ItemStatus,
    pub hlc: HLC,
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ItemEntry {
    pub storage_key: StorageKey,
    pub item: Item,
}

impl ItemEntry {
    pub fn new(storage_key: StorageKey, item: Item) -> ItemEntry {
        ItemEntry { storage_key, item }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum ItemStatus {
    Active,
    Tombstone(u64),
}

#[cfg(test)]
mod tests {
    use crate::store::{PartitionKey, RangeKey};

    use super::*;

    #[test]
    fn test_storage_key_equality() {
        let pk1 = PartitionKey("partition1".to_string());
        let rk1 = RangeKey("range1".to_string());
        let key1 = StorageKey::new(pk1.clone(), Some(rk1.clone()));
        let key2 = StorageKey::new(pk1.clone(), Some(rk1.clone()));
        let key3 = StorageKey::new(pk1.clone(), None);

        // Keys with same partition and range should be equal
        assert_eq!(key1, key2);

        // Keys with same partition but different range should not be equal
        assert_ne!(key1, key3);
    }
}
