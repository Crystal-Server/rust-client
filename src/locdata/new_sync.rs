use std::collections::HashMap;

use crate::{locdata::sync::SyncType, netdata::variable::Variable};

#[derive(Debug, Clone)]
pub(crate) struct NewSync {
    pub slot: usize,
    pub sync_type: SyncType,
    pub kind: i16,
    pub variables: HashMap<String, Variable>,
}
