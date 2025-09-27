use num_enum::TryFromPrimitive;

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum SyncEvent {
    #[default]
    New = 0,
    Step = 1,
    End = 2,
    Once = 3,
}

/// The target syncronization type
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum SyncType {
    Once = 0,
    #[default]
    Normal = 1,
}
