use num_enum::TryFromPrimitive;

#[derive(Default, Debug, Copy, Clone, TryFromPrimitive)]
#[repr(u8)]
pub(crate) enum ChangeFriendStatus {
    // Outgoing
    Request = 0,
    Cancel = 1,
    // Incoming
    Accept = 2,
    Deny = 3,
    //
    Remove = 4,
    // Misc
    Friend = 5,
    #[default]
    NotFriend = 6,
}
