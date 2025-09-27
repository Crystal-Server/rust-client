use std::collections::HashMap;

use crate::{
    locdata::sync::SyncType,
    netdata::{
        admin_action::AdminAction, bdb::file_permissions::BdbFilePermissions,
        change_friend_status::ChangeFriendStatus, login::LoginPassw, player_request::PlayerRequest,
        sync::SelfSync, sync_update::SyncUpdate, variable::Variable,
        variable_update::VariableUpdate,
    },
};

#[derive(Debug, Clone)]
#[doc(hidden)]
pub(crate) enum WritePacket {
    /// Hash, Lib Version, Device ID, Game ID, Game Version, Game Session
    InitializationHandshake([u64; 4], u64, String, String, f64, String),
    /// Username, Passw/Token, Game Token, Variables, Syncs, Room
    Login(
        String,
        LoginPassw,
        String,
        HashMap<String, Variable>,
        Vec<Option<SelfSync>>,
        String,
    ),
    /// Username, Email, Passw, Repeat Passw
    Register(String, String, String, String),
    /// Player ID, Callback Index, Variable Name
    RequestPlayerVariable(PlayerRequest, u64, String),
    /// Player ID, Message ID, Payload
    P2P(i16, PlayerRequest, Vec<Variable>),
    /// Game Version
    UpdateGameVersion(f64),
    /// Game Session
    UpdateGameSession(String),
    /// Variables
    UpdatePlayerVariable(Vec<VariableUpdate>),
    Ping(),
    /// Variables
    GameIniWrite(Vec<VariableUpdate>),
    /// Variables
    PlayerIniWrite(Vec<VariableUpdate>),
    /// Room
    UpdateRoom(String),
    /// Vec<(Slot, Kind, Sync Type, Value)>
    NewSync(Vec<(u64, i16, SyncType, HashMap<String, Variable>)>),
    /// Sync Update
    UpdateSync(Vec<SyncUpdate>),
    /// Achievement ID
    UpdateAchievement(u64),
    /// Highscore ID, Score
    UpdateHighscore(u64, f64),
    /// Admin Action, Player ID
    AdminAction(AdminAction, u64),
    /// Player ID, Callback Index, Sync Slot, Variable Name
    RequestSyncVariable(u64, u64, u64, String),
    Logout(),
    /// BDB Name
    RequestBdb(u64, String),
    /// Callback Index, BDB Name, Data, File Permissions
    SetBdb(u64, String, Vec<u8>, Option<BdbFilePermissions>),
    /// Change Friend Status, Player ID
    RequestChangeFriendStatus(ChangeFriendStatus, u64),
    /// Packets
    PacketCrunch(Vec<WritePacket>),
    /// Callback Index, BDB Name
    ExistsBdb(u64, String),
    /// Name, Value
    GlobalVariableWrite(Vec<VariableUpdate>),
}
