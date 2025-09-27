use crate::netdata::{bdb::SetBdbFile, optional_variable::OptionalVariable};

pub type PlayerVariableServerUpdate =
    Box<dyn FnMut(u64, String, OptionalVariable) + core::marker::Sync + Send>;
pub type SyncVariableServerUpdate =
    Box<dyn FnMut(u64, String, OptionalVariable) + core::marker::Sync + Send>;
pub type FetchBdbServerUpdate = Box<dyn FnMut(String, Option<Vec<u8>>) + core::marker::Sync + Send>;
pub type ExistsBdbServerUpdate = Box<dyn FnMut(String, bool) + core::marker::Sync + Send>;
pub type WriteBdbServerUpdate = Box<dyn FnMut(String, SetBdbFile) + core::marker::Sync + Send>;

pub(crate) enum ServerUpdateCallback {
    /// Callback, Player ID
    PlayerVariable(Option<PlayerVariableServerUpdate>, u64),
    /// Callback, Player ID, Sync Slot
    SyncVariable(Option<SyncVariableServerUpdate>, u64, usize),
    /// Callback
    FetchBdb(Option<FetchBdbServerUpdate>),
    /// Callback
    ExistsBdb(Option<ExistsBdbServerUpdate>),
    /// Callback
    WriteBdb(Option<WriteBdbServerUpdate>),
}

impl std::fmt::Debug for ServerUpdateCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::PlayerVariable(func, pid) => f
                .debug_tuple("PlayerVariable")
                .field(&if func.is_some() { "Some<...>" } else { "None" })
                .field(pid)
                .finish(),
            Self::SyncVariable(func, pid, slot) => f
                .debug_tuple("SyncVariable")
                .field(&if func.is_some() { "Some<...>" } else { "None" })
                .field(pid)
                .field(slot)
                .finish(),
            Self::FetchBdb(func) => f
                .debug_tuple("FetchBdb")
                .field(&if func.is_some() { "Some<...>" } else { "None" })
                .finish(),
            Self::ExistsBdb(func) => f
                .debug_tuple("ExistsBdb")
                .field(&if func.is_some() { "Some<...>" } else { "None" })
                .finish(),
            Self::WriteBdb(func) => f
                .debug_tuple("WriteBdb")
                .field(&if func.is_some() { "Some<...>" } else { "None" })
                .finish(),
        }
    }
}