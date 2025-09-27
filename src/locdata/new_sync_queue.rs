use crate::locdata::sync::SyncType;

#[derive(Debug, Clone)]
pub(crate) struct NewSyncQueue {
    pub slot: usize,
    pub kind: i16,
    pub sync_type: SyncType,
}
