pub mod file_permissions;
pub mod permission_target;

use num_enum::TryFromPrimitive;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct BdbPermission: u8 {
        const Read = 1 << 0;
        const Write = 1 << 1;
        const Create = 1 << 2;
        const Delete = 1 << 3;
        const ManagePermissions = 1 << 4;
        const CreateAndManagePermissions = 1 << 5;
    }
}

#[derive(Debug, Copy, Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum SetBdbFile {
    /// The file (and permissions) were set correctly.
    Ok = 0,
    /// The BDB file is too big.
    TooBig = 1,
    /// There are too many BDB files registered.
    TooManyFiles = 2,
    /// The player doesn't have sufficient permissions to perform this action.
    InsufficientPermissions = 3,
    /// An internal error happened.
    Error = 4,
}
