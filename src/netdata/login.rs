use chrono::{DateTime, Utc};

/// The return success value of a login attempt
#[derive(Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LoginCode {
    Ok(Option<String>) = 0,
    NoUser = 1,
    WrongPassword = 2,
    Unauthenticated = 3,
    Unverified = 4,
    AlreadyIn = 5,
    GameBan(String, DateTime<Utc>) = 6,
    GlobalBan(String, DateTime<Utc>) = 7,
    #[default]
    Error = 8,
    MaxPlayers = 9,
}

#[derive(Debug, Clone)]
pub(crate) enum LoginPassw {
    Passw(String),
    Token(String),
}
