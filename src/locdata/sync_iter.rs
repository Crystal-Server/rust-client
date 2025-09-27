use std::collections::HashMap;

use crate::{locdata::sync::SyncEvent, netdata::variable::Variable};

/// A return value from an iterator containing data about a Sync from another player
#[derive(Debug, Clone, Default)]
pub struct SyncIter {
    pub player_id: u64,
    pub player_name: String,
    pub slot: usize,
    pub event: SyncEvent,
    pub kind: i16,
    pub variables: HashMap<String, Variable>,
}
