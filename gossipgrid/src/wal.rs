use bincode::{Decode, Encode};
use gossipgrid_wal::FramedWalRecord;
use serde::{Deserialize, Serialize};

use crate::{
    clock::HLC,
    item::{Item, ItemStatus},
};

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub enum WalRecord {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        hlc: u64,
    },
    Delete {
        key: Vec<u8>,
        hlc: u64,
    },
}

pub struct FramedWalRecordItem {
    pub item: Item,
    pub lsn: u64,
    pub key_bytes: Vec<u8>,
}

impl From<FramedWalRecord<WalRecord>> for FramedWalRecordItem {
    fn from(record: FramedWalRecord<WalRecord>) -> FramedWalRecordItem {
        match record {
            FramedWalRecord {
                lsn,
                record: WalRecord::Put { key, value, hlc },
            } => FramedWalRecordItem {
                item: Item {
                    message: value,
                    status: ItemStatus::Active,
                    hlc: HLC {
                        timestamp: hlc,
                        counter: 0,
                    },
                },
                lsn,
                key_bytes: key,
            },
            FramedWalRecord {
                lsn,
                record: WalRecord::Delete { key, hlc },
            } => FramedWalRecordItem {
                item: Item {
                    message: vec![],
                    status: ItemStatus::Tombstone(hlc),
                    hlc: HLC {
                        timestamp: hlc,
                        counter: 0,
                    },
                },
                lsn,
                key_bytes: key,
            },
        }
    }
}
