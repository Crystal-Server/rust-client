use num_enum::TryFromPrimitive;

/// The return success value of a registration attempt
#[derive(Default, Copy, Clone, Debug, TryFromPrimitive, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum RegistrationCode {
    Ok = 0,
    AccountExists = 1,
    UsedEmail = 2,
    InvalidEmail = 3,
    ShortPassword = 4,
    InvalidName = 5,
    ShortName = 6,
    DifferentPasswords = 7,
    #[default]
    Error = 8,
    LongName = 9,
    GlobalBan = 10,
    LongPassword = 11,
    MaxAccounts = 12,
}
