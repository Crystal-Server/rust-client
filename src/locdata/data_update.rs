use chrono::{DateTime, Utc};

use crate::netdata::{
    admin_action::AdminAction, administrator::Administrator, bdb::BdbPermission, login::LoginCode,
    optional_variable::OptionalVariable, register::RegistrationCode, variable::Variable,
};

/// Data updates that have been triggered from the server.
#[derive(Debug, Clone, PartialEq)]
pub enum DataUpdate {
    /// Return Code
    Registration(RegistrationCode),
    /// Return Code
    Login(LoginCode),
    /// Player ID, Player Name, Login Token
    LoginOk(u64, String, Option<String>),
    /// Return Code
    LoginBan(LoginCode),
    /// Player ID, Player Name, Room
    PlayerLoggedIn(u64, String, String),
    /// Player ID
    PlayerLoggedOut(u64),
    /// Player ID or Server, Message ID, Payload
    P2P(Option<u64>, i16, Vec<Variable>),
    /// Player ID, Variable Name, Variable Value
    UpdateVariable(u64, String, OptionalVariable),
    /// Player ID, Sync ID, Variable Name, Variable Value
    UpdateSyncVariable(u64, usize, String, OptionalVariable),
    /// Player ID, Sync ID
    UpdateSyncRemoval(u64, usize),
    /// (Optional) File, Section, Key, Value
    UpdateGameIni(Option<String>, String, String, OptionalVariable),
    /// (Optional) File, Section, Key, Value
    UpdatePlayerIni(Option<String>, String, String, OptionalVariable),
    /// Version
    UpdateGameVersion(f64),
    /// Admin Action
    AdminAction(AdminAction),
    /// Player ID, Administrator Permissions
    UpdateAdministrator(u64, Option<Administrator>),
    /// Name, Value, Permissions
    FetchBdb(String, Option<Vec<u8>>, Option<BdbPermission>),
    /// Player ID
    ChangeFriendStatus(u64),
    /// Message
    ServerMessage(String),
    Reconnecting,
    Disconnected,
    /// Reason
    Kicked(String),
    /// Reason, Unban Time
    Banned(String, DateTime<Utc>),
    /// Notification Message
    ServerNotification(String),
    /// Player ID
    ChangeGameMaster(u64),
    /// Player ID
    ChangeSessionMaster(u64),
}
