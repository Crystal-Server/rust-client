use std::collections::HashMap;

use integer_hasher::{IntMap, IntSet};

use crate::{
    leb::Leb,
    locdata::sync::SyncType,
    netdata::{
        achievement::Achievement,
        admin_action::AdminAction,
        administrator::Administrator,
        bdb::{BdbPermission, SetBdbFile},
        change_friend_status::ChangeFriendStatus,
        client_sync::ClientSync,
        highscore::Highscore,
        login::LoginCode,
        optional_variable::OptionalVariable,
        register::RegistrationCode,
        sync_update::SyncUpdate,
        variable::Variable,
        variable_update::VariableUpdate,
    },
};

#[derive(Debug, Clone)]
pub(crate) enum ReadPacket {
    /// Registration Code
    Registration(RegistrationCode),
    /// Login Code
    Login(LoginCode),
    /// Login Code
    LoginBan(LoginCode),
    /// Player ID, Player Name, Token, Savefile, Friends, Incoming Friends, Outgoing Friends, Game Achievements, Game Master, Session Master, Global Variables
    LoginOk(
        u64,
        String,
        Option<String>,
        HashMap<String, Variable>,
        IntSet<Leb<u64>>,
        IntSet<Leb<u64>>,
        IntSet<Leb<u64>>,
        IntMap<Leb<u64>, Achievement>,
        u64,
        u64,
        HashMap<String, Variable>,
    ),
    /// Player ID, Player Name, Player Variables, Player Syncs, Room
    PlayerLoggedIn(
        u64,
        String,
        HashMap<String, Variable>,
        Vec<Option<ClientSync>>,
        String,
    ),
    /// Player ID
    PlayerLoggedOut(u64),
    /// Game Save, Game Achievements, Game Highscores, Game Administrators, Version
    SyncGameInfo(
        HashMap<String, Variable>,
        IntMap<Leb<u64>, Achievement>,
        IntMap<Leb<u64>, Highscore>,
        IntMap<Leb<u64>, Administrator>,
        f64,
    ),
    /// Player ID or Server, Message ID, Data
    P2P(Option<Leb<u64>>, i16, Vec<Variable>),
    /// Player ID, Variables
    UpdatePlayerVariable(u64, Vec<VariableUpdate>),
    /// Ping (ms)
    Ping(Option<f64>),
    ClearPlayers(),
    /// Variables
    GameIniWrite(Vec<VariableUpdate>),
    /// Player ID, Vec<(Slot, Kind, Type, Variables)>
    NewSync(u64, Vec<(u64, i16, SyncType, HashMap<String, Variable>)>),
    /// Player ID, Room
    PlayerChangedRooms(u64, String),
    /// Player ID, Sync Variables
    UpdateSync(u64, Vec<SyncUpdate>),
    /// Player ID, Highscore ID, Score
    HighscoreUpdate(u64, u64, f64),
    /// Player ID, Player Syncs, Player Variables
    UpdatePlayerData(u64, Vec<Option<ClientSync>>, HashMap<String, Variable>),
    /// Callback Index, Variable
    RequestPlayerVariable(u64, OptionalVariable),
    // Admin Action
    AdminAction(AdminAction),
    /// Callback Index, Variable
    RequestSyncVariable(u64, OptionalVariable),
    /// Game Version
    ChangeGameVersion(f64),
    /// Player ID, Administrator
    ModifyAdministrator(u64, Administrator),
    /// Administrator ID
    RemoveAdministrator(u64),
    ForceDisconnection(),
    /// Variables
    PlayerIniWrite(Vec<VariableUpdate>),
    /// Callback Index, BDB Data, File Permissions
    RequestBdb(u64, Option<Vec<u8>>, Option<BdbPermission>),
    /// Change Friend Status, Player ID
    ChangeFriendStatus(ChangeFriendStatus, u64),
    Handshake(),
    /// Message
    ServerMessage(String),
    /// Target Host
    ChangeConnection(String),
    /// Packets
    PacketCrunch(Vec<ReadPacket>),
    /// Callback Index, Exists
    ExistsBdb(u64, bool),
    /// Callback Index, Status
    SetBdb(u64, SetBdbFile),
    /// Player ID
    SetGameMaster(u64),
    /// Player ID
    SetSessionMaster(u64),
    /// Name, Value
    SetGlobalVariable(String, OptionalVariable),
}
