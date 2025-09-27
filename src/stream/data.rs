use std::collections::{HashMap, HashSet};

use integer_hasher::{IntMap, IntSet};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle, time::Instant};

use crate::{
    CallbackDataUpdate, CallbackDisconnected, CallbackLogin, CallbackP2P, CallbackRegister,
    CallbackRoom,
    client::ClientError,
    leb::Leb,
    locdata::{new_sync_queue::NewSyncQueue, player_queue::PlayerQueue},
    netdata::{
        achievement::Achievement, administrator::Administrator,
        callback_server_update::CallbackServerUpdate, highscore::Highscore,
        packets::write::WritePacket, player::Player, sync::SelfSync, variable::Variable,
    },
};

#[derive(Default)]
pub(crate) struct StreamData {
    pub use_webtransport: bool,
    
    pub thread: Option<JoinHandle<()>>,
    pub write_mpsc: Option<UnboundedSender<WritePacket>>,
    pub last_host: Option<String>,

    pub is_connected: bool,
    pub is_loggedin: bool,
    pub is_connecting: bool,
    pub is_reconnecting: bool,

    pub game_id: String,
    pub version: f64,
    pub session: String,
    pub game_token: String,
    pub room: String,

    pub func_room: Option<CallbackRoom>,
    pub func_p2p: Option<CallbackP2P>,
    pub func_register: Option<CallbackRegister>,
    pub func_login: Option<CallbackLogin>,
    pub func_disconnected: Option<CallbackDisconnected>,
    pub func_data_update: Option<CallbackDataUpdate>,

    pub player_id: Option<u64>,
    pub player_name: Option<String>,
    pub player_save: HashMap<String, Variable>,
    pub player_open_save: String,
    pub player_friends: IntSet<u64>,
    pub player_incoming_friends: IntSet<u64>,
    pub player_outgoing_friends: IntSet<u64>,

    pub game_save: HashMap<String, Variable>,
    pub game_open_save: String,
    pub game_achievements: IntMap<Leb<u64>, Achievement>,
    pub game_highscores: IntMap<Leb<u64>, Highscore>,
    pub game_administrators: IntMap<Leb<u64>, Administrator>,
    pub game_version: f64,

    pub global_variables: HashMap<String, Variable>,

    pub players: IntMap<u64, Player>,
    pub players_logout: IntSet<u64>,
    pub player_queue: IntMap<u64, PlayerQueue>,
    pub variables: HashMap<String, Variable>,
    pub syncs: Vec<Option<SelfSync>>,
    pub syncs_remove: Vec<usize>,

    pub game_master: Option<u64>,
    pub session_master: Option<u64>,

    pub ping: f64,
    pub last_ping: Option<Instant>,

    pub new_sync_queue: Vec<NewSyncQueue>,
    pub update_variable: HashSet<String>,
    pub update_playerini: HashSet<String>,
    pub update_gameini: HashSet<String>,
    pub update_globalvari: HashSet<String>,
    pub call_disconnected: bool,

    pub callback_server_update: IntMap<u64, Option<CallbackServerUpdate>>,
    pub callback_server_index: u64,

    pub handshake_completed: bool,

    pub registered_errors: Vec<ClientError>,
}

impl StreamData {
    pub async fn clear(&mut self, full: bool) {
        self.is_loggedin = false;
        self.call_disconnected = true;

        self.player_name.take();
        self.player_id.take();
        self.player_save.clear();
        self.player_open_save.clear();
        self.player_queue.clear();
        self.players_logout.clear();
        self.new_sync_queue.clear();
        self.update_variable.clear();
        self.update_playerini.clear();
        self.update_globalvari.clear();
        self.callback_server_update.clear();
        self.callback_server_index = 0;
        self.players.clear();

        if full {
            self.is_connecting = false;
            self.is_connected = false;
            self.is_reconnecting = false;

            self.game_save.clear();
            self.game_open_save.clear();
            self.game_achievements.clear();
            self.game_highscores.clear();
            self.game_administrators.clear();
            self.update_gameini.clear();

            self.last_ping.take();
            self.handshake_completed = false;
        }
    }
}
