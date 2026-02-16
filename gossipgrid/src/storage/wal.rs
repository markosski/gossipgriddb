use bincode::{Decode, Encode};

use serde::{Deserialize, Serialize};

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
