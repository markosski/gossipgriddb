use std::{
    cmp::Ordering,
    ops::{Bound, RangeBounds},
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Encode, Decode, Ord, PartialOrd, PartialEq, Eq, Serialize, Deserialize, Default,
)]
pub struct HLC {
    pub timestamp: u64,
    pub counter: u64,
}

impl HLC {
    pub fn new() -> HLC {
        HLC {
            timestamp: 0,
            counter: 0,
        }
    }

    pub fn compare(&self, other: &HLC) -> Ordering {
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.counter.cmp(&other.counter),
        }
    }

    pub fn tick_hlc(&self, now: u64) -> HLC {
        if now > self.timestamp {
            HLC {
                timestamp: now,
                counter: 0,
            }
        } else {
            HLC {
                timestamp: self.timestamp,
                counter: self.counter + 1,
            }
        }
    }

    /// Merge a node-level HLC and an item-level HLC, then advance (tick) the result
    /// so the returned HLC is strictly after both inputs with respect to HLC ordering.
    pub fn merge_and_tick(node_hlc: &HLC, item_hlc: &HLC, now: u64) -> HLC {
        let merged = HLC::merge(node_hlc, item_hlc, now);
        merged.tick_hlc(now)
    }

    /// Convenience: merge `self` with node_hlc and tick.
    pub fn tick_with_node(&self, node_hlc: &HLC, now: u64) -> HLC {
        HLC::merge_and_tick(node_hlc, self, now)
    }

    pub fn merge(local: &HLC, remote: &HLC, now: u64) -> HLC {
        let merged_pt = local.timestamp.max(remote.timestamp).max(now);

        let merged_lc = if merged_pt == local.timestamp && merged_pt == remote.timestamp {
            local.counter.max(remote.counter) + 1
        } else if merged_pt == local.timestamp {
            local.counter + 1
        } else if merged_pt == remote.timestamp {
            remote.counter + 1
        } else {
            0 // wall clock moved forward
        };

        HLC {
            timestamp: merged_pt,
            counter: merged_lc,
        }
    }
}

impl RangeBounds<HLC> for HLC {
    fn start_bound(&self) -> Bound<&HLC> {
        Bound::Included(self)
    }

    fn end_bound(&self) -> Bound<&HLC> {
        Bound::Included(self)
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

pub fn now_seconds() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_compare() {
        let hlc1 = HLC {
            timestamp: 100,
            counter: 0,
        };
        let hlc2 = HLC {
            timestamp: 200,
            counter: 0,
        };
        let hlc3 = HLC {
            timestamp: 100,
            counter: 1,
        };

        assert_ne!(hlc1, hlc3);
        assert_eq!(hlc1.compare(&hlc2), Ordering::Less);
        assert_eq!(hlc2.compare(&hlc1), Ordering::Greater);
        assert_eq!(hlc1.compare(&hlc3), Ordering::Less);
        assert_eq!(hlc3.compare(&hlc1), Ordering::Greater);
        assert_eq!(hlc1.compare(&hlc3), Ordering::Less);
    }
}
