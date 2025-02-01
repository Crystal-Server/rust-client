use crate::core::types::NewSync;

use super::{
    buffer::Buffer,
    leb::Leb,
    types::{
        self, Achievement, AdminAction, Administrator, CallbackServerUpdate, ChangeFriendStatus,
        DataUpdate, FetchBdbServerUpdate, Highscore, LoginCode, LoginPassw, NewSyncQueue,
        OptionalVariable, Player, PlayerQueue, PlayerRequest, PlayerVariableServerUpdate,
        RegistrationCode, SelfSync, ServerUpdateCallback, SyncEvent, SyncIter, SyncType,
        SyncUpdate, SyncVariableServerUpdate, Variable, VariableUpdate,
    },
};
use age::{
    x25519::{Identity, Recipient},
    Decryptor, Encryptor,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{
    stream::{SplitSink, SplitStream},
    AsyncReadExt, AsyncWriteExt, SinkExt, Stream, StreamExt,
};
use integer_hasher::{IntMap, IntSet};
use machineid_rs::{Encryption, HWIDComponent, IdBuilder};
use num_enum::TryFromPrimitive;
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Error, ErrorKind, Result as IoResult},
    iter,
    str::FromStr,
    sync::Arc,
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock},
    task::JoinHandle,
    time::Instant,
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
#[cfg(feature = "__dev")]
use tracing::{info, warn};

pub struct CrystalServer {
    writer: Option<Arc<Mutex<StreamWriter>>>,
    data: Arc<RwLock<StreamData>>,
}

struct StreamHandler;

struct StreamReader {
    stream: Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,

    pub identity: Option<Identity>,
}

struct StreamWriter {
    stream: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,

    pub recipient: Option<Recipient>,
}

type CallbackRoom = Box<dyn FnMut() -> String + Sync + Send>;
type CallbackP2P = Box<dyn FnMut(Option<u64>, i16, Vec<Variable>) + Sync + Send>;
type CallbackRegister = Box<dyn FnMut(RegistrationCode) + Sync + Send>;
type CallbackLogin = Box<dyn FnMut(LoginCode, Option<DateTime<Utc>>, Option<String>) + Sync + Send>;
type CallbackBanned = Box<dyn FnMut(String, DateTime<Utc>) + Sync + Send>;
type CallbackKicked = Box<dyn FnMut(String) + Sync + Send>;
type CallbackDisconnected = Box<dyn FnMut() + Sync + Send>;
type CallbackLoginToken = Box<dyn FnMut(String) + Sync + Send>;
type CallbackDataUpdate = Box<dyn FnMut(DataUpdate) + Sync + Send>;
type CallbackBDB = Box<dyn FnMut(String, Option<Vec<u8>>) + Sync + Send>;

#[derive(Default)]
pub struct StreamData {
    thread: Option<JoinHandle<()>>,
    last_host: Option<String>,

    is_connected: bool,
    is_loggedin: bool,
    is_connecting: bool,
    is_reconnecting: bool,

    game_id: String,
    version: f64,
    session: String,
    game_token: String,
    room: String,

    func_room: Option<CallbackRoom>,
    func_p2p: Option<CallbackP2P>,
    func_register: Option<CallbackRegister>,
    func_login: Option<CallbackLogin>,
    func_banned: Option<CallbackBanned>,
    func_kicked: Option<CallbackKicked>,
    func_disconnected: Option<CallbackDisconnected>,
    func_login_token: Option<CallbackLoginToken>,
    func_data_update: Option<CallbackDataUpdate>,
    func_bdb: Option<CallbackBDB>,

    player_id: Option<u64>,
    player_name: Option<String>,
    player_save: HashMap<String, Variable>,
    player_open_save: String,
    player_friends: IntSet<u64>,
    player_incoming_friends: IntSet<u64>,
    player_outgoing_friends: IntSet<u64>,

    game_save: HashMap<String, Variable>,
    game_open_save: String,
    game_achievements: IntMap<Leb<u64>, Achievement>,
    game_highscores: IntMap<Leb<u64>, Highscore>,
    game_administrators: IntMap<Leb<u64>, Administrator>,
    game_version: f64,

    players: IntMap<u64, Player>,
    players_logout: IntSet<u64>,
    player_queue: IntMap<u64, PlayerQueue>,
    variables: HashMap<String, Variable>,
    syncs: Vec<Option<SelfSync>>,
    syncs_remove: Vec<usize>,

    ping: f64,
    last_ping: Option<Instant>,

    new_sync_queue: Vec<NewSyncQueue>,
    update_variable: HashMap<String, OptionalVariable>,
    update_playerini: HashMap<String, OptionalVariable>,
    update_gameini: HashMap<String, OptionalVariable>,
    callback_server_update: Vec<Option<CallbackServerUpdate>>,
    call_disconnected: bool,

    handshake_completed: bool,

    registered_errors: Vec<ClientError>,
}

#[derive(Debug)]
pub enum ReaderError {
    StreamError(String),
    StreamEmpty(String),
    StreamClosed(String),
    Unknown(String),
}

#[derive(Debug)]
pub enum WriterError {
    StreamError(String),
    StreamClosed(String),
    Unknown(String),
}

#[derive(Debug)]
pub enum ClientError {
    HandlerResult(IoResult<()>),
    HandlerResultString(String),
    HandlerPanic(String),
}

macro_rules! unwrap_return {
    ($value: expr, $return: expr) => {
        if $value.is_err() {
            return $return;
        }
    };
    ($value: expr) => {
        if $value.is_err() {
            return;
        }
    };
}

impl StreamData {
    pub async fn clear(&mut self, full: bool) {
        self.is_loggedin = false;
        self.is_connected = false;
        self.is_connecting = false;
        self.is_reconnecting = false;

        self.player_name = None;
        self.player_id = None;
        self.player_save.clear();
        self.player_open_save.clear();
        self.player_queue.clear();
        self.players_logout.clear();
        self.new_sync_queue.clear();
        self.update_variable.clear();
        self.update_playerini.clear();
        self.callback_server_update.clear();
        if full {
            self.game_save.clear();
            self.game_open_save.clear();
            self.game_achievements.clear();
            self.game_highscores.clear();
            self.game_administrators.clear();
            self.update_gameini.clear();
            self.last_ping = None;
            self.handshake_completed = false;
        }
    }
}

impl StreamHandler {
    pub async fn split_stream(
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> (StreamReader, StreamWriter) {
        let split = stream.split();
        (
            StreamReader {
                stream: Some(split.1),

                identity: None,
            },
            StreamWriter {
                stream: Some(split.0),

                recipient: None,
            },
        )
    }
}

impl StreamReader {
    #[inline(always)]
    pub async fn read(&mut self) -> Result<Buffer, ReaderError> {
        if let Some(stream) = self.stream.as_mut() {
            if let Some(Ok(frame)) = stream.next().await {
                if frame.is_binary() {
                    let data = frame.into_data();
                    if !data.is_empty() && data.len() <= 0x40_000_000 {
                        if let Some(identity) = &self.identity {
                            let len = data.len();
                            if let Ok(decryptor) =
                                Decryptor::new_async_buffered(data.to_vec().as_slice()).await
                            {
                                if let Ok(mut reader) =
                                    decryptor.decrypt_async(iter::once(identity as _))
                                {
                                    let mut output = Vec::with_capacity(len);
                                    if reader.read_to_end(&mut output).await.is_ok() {
                                        Ok(Buffer::new(Cursor::new(output)))
                                    } else {
                                        Err(ReaderError::Unknown(String::from(
                                            "decryptor reader errored out while reading",
                                        )))
                                    }
                                } else {
                                    Err(ReaderError::Unknown(String::from(
                                        "unable to make decryptor reader",
                                    )))
                                }
                            } else {
                                Err(ReaderError::Unknown(String::from(
                                    "unable to make decryptor",
                                )))
                            }
                        } else {
                            Ok(Buffer::new(Cursor::new(data.to_vec())))
                        }
                    } else {
                        Err(ReaderError::StreamEmpty(format!(
                            "tried to read {} byte(s) from ws",
                            data.len(),
                        )))
                    }
                } else if frame.is_close() {
                    Err(ReaderError::StreamClosed(String::from(
                        "ws stream requested to close",
                    )))
                } else {
                    Err(ReaderError::Unknown(format!(
                        "obtained an unexpected code for ws: {frame:?}",
                    )))
                }
            } else {
                Err(ReaderError::StreamClosed(String::from(
                    "unable to obtain the next ws frame",
                )))
            }
        } else {
            Err(ReaderError::Unknown(String::from(
                "no stream open to read from",
            )))
        }
    }

    /*#[inline(always)]
    pub async fn shutdown(&mut self) {
        self.stream = None;
    }*/
}

impl StreamWriter {
    #[inline(always)]
    pub async fn prepare_buffer(&self, mut buffer: Buffer) -> IoResult<Buffer> {
        if let Some(recipient) = &self.recipient {
            let encryptor = Encryptor::with_recipients(iter::once(recipient as _))
                .expect("unable to make encryptor");
            let mut output = Vec::with_capacity(buffer.len()? as usize);
            let mut writer = encryptor.wrap_async_output(&mut output).await?;
            writer.write_all(buffer.container.get_ref()).await?;
            writer.flush().await?;
            writer.close().await?;
            buffer.container = Cursor::new(output);
        }
        Ok(buffer)
    }

    #[inline(always)]
    pub async fn write(&mut self, data: Buffer) -> Result<(), WriterError> {
        if let Ok(mut data) = self.prepare_buffer(data).await {
            self.write_raw(&mut data).await
        } else {
            Err(WriterError::StreamError(String::from(
                "unable to fetch new buffer",
            )))
        }
    }

    #[inline(always)]
    pub async fn write_raw(&mut self, data: &mut Buffer) -> Result<(), WriterError> {
        /*#[cfg(feature = "__dev")]
        info!("wrote data: {:?}", data.container.get_ref().to_str_lossy());*/
        let data = {
            let mut d = Vec::new();
            unwrap_return!(
                data.read_all(&mut d),
                Err(WriterError::Unknown(String::from(
                    "unable to convert buffer into bytes"
                )))
            );
            d
        };
        if let Some(stream) = self.stream.as_mut() {
            unwrap_return!(
                stream
                    .send(Message::Binary(Bytes::copy_from_slice(&data)))
                    .await,
                Err(WriterError::StreamError(format!(
                    "unable to write {:?} byte(s) to a ws",
                    data.len(),
                )))
            );
            unwrap_return!(
                stream.flush().await,
                Err(WriterError::StreamError(String::from(
                    "unable to flush the writer of a ws {:?}",
                )))
            );
            Ok(())
        } else {
            Err(WriterError::Unknown(String::from(
                "no stream open to write to",
            )))
        }
    }

    #[inline(always)]
    pub async fn shutdown(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.close().await;
        }
    }
}

#[derive(Debug, Clone)]
enum ReadPacket {
    /// Registration Code
    Registration(RegistrationCode),
    /// Login Code
    Login(LoginCode),
    /// Login Code, Reason, Unban time
    LoginBan(LoginCode, String, i64),
    /// Player ID, Player Name, Token, Savefile, Friends, Incoming Friends, Outgoing Friends, Game Achievements
    LoginOk(
        u64,
        String,
        Option<String>,
        HashMap<String, Variable>,
        IntSet<Leb<u64>>,
        IntSet<Leb<u64>>,
        IntSet<Leb<u64>>,
        IntMap<Leb<u64>, Achievement>,
    ),
    /// Player ID, Player Name, Player Variables, Player Syncs, Room
    PlayerLoggedIn(
        u64,
        String,
        HashMap<String, Variable>,
        Vec<Option<types::Sync>>,
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
    /// Player ID, Vec<Slot, Kind, Type, Variables>
    NewSync(u64, Vec<(u64, i16, SyncType, HashMap<String, Variable>)>),
    /// Player ID, Room
    PlayerChangedRooms(u64, String),
    /// Player ID, Sync Variables
    UpdateSync(u64, Vec<SyncUpdate>),
    /// Player ID, Highscore ID, Score
    HighscoreUpdate(u64, u64, f64),
    /// Player ID, Player Syncs, Player Variables
    UpdatePlayerData(u64, Vec<Option<types::Sync>>, HashMap<String, Variable>),
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
    /// Callback Index, BDB
    RequestBdb(u64, Option<Vec<u8>>),
    /// Change Friend Status, Player ID
    ChangeFriendStatus(ChangeFriendStatus, u64),
    /// Key
    Handshake(Option<String>),
    /// Message
    ServerMessage(String),
    /// Target Host
    ChangeConnection(String),
}

#[derive(Debug, Clone)]
enum WritePacket {
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
    P2P(PlayerRequest, i16, Vec<Variable>),
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
    /// Vec<Slot, Kind, Sync Type, Variables>
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
    /// Callback Index, BDB Name
    RequestBdb(u64, String),
    /// BDB Name, Data
    SetBdb(String, Vec<u8>),
    /// Change Friend Status, Player ID
    RequestChangeFriendStatus(ChangeFriendStatus, u64),
    /// Key
    Handshake(String),
}

impl CrystalServer {
    pub fn init(game_id: &str) -> Self {
        Self {
            writer: None,
            data: Arc::new(RwLock::new(StreamData {
                game_id: game_id.to_owned(),
                ..Default::default()
            })),
        }
    }

    pub async fn connect(&mut self) {
        {
            let mut lock = self.data.write().await;
            if let Some(thread) = &mut lock.thread {
                if !thread.is_finished() {
                    thread.abort();
                }
            }
            lock.is_connecting = true;
        }
        let url = if let Some(url) = self.data.read().await.last_host.clone() {
            url
        } else if cfg!(feature = "__local") {
            String::from("ws://localhost:16562")
        } else {
            String::from("ws://server.crystal-server.co:16562")
        };
        match tokio_tungstenite::connect_async(url).await {
            Ok((ws, _)) => {
                let stream = StreamHandler::split_stream(ws).await;
                let writer = Arc::new(Mutex::new(stream.1));
                self.writer = Some(writer.clone());
                let thread =
                    tokio::spawn(Self::stream_handler(stream.0, writer, self.data.clone()));
                {
                    let mut lock = self.data.write().await;
                    lock.thread = Some(thread);
                }
            }
            Err(_e) => {
                #[cfg(feature = "__dev")]
                info!("Connection error: {_e:?}");
                let mut dlock = self.data.write().await;
                if dlock.last_host.take().is_some() {
                    drop(dlock);
                    Box::pin(self.connect()).await;
                } else {
                    dlock.is_connecting = false;
                }
            }
        }
    }

    async fn stream_handler(
        mut reader: StreamReader,
        writer: Arc<Mutex<StreamWriter>>,
        data: Arc<RwLock<StreamData>>,
    ) {
        let cdata = data.clone();
        let cwriter = writer.clone();

        let task = tokio::spawn(async move {
            macro_rules! write_packet {
                ($packet: expr) => {
                    unwrap_return!(
                        writer
                            .lock()
                            .await
                            .write(Self::get_packet_write(&$packet)?)
                            .await,
                        Err(Error::from(ErrorKind::BrokenPipe))
                    );
                };
            }

            loop {
                match reader.read().await {
                    Ok(buffer) => {
                        if let Ok(packet) = CrystalServer::get_packet_read(buffer) {
                            #[cfg(feature = "__dev")]
                            info!("reading packet: {packet:?}");
                            match packet {
                                ReadPacket::Handshake(key) => {
                                    if let Some(key) = key {
                                        let encode_read = Identity::generate();
                                        let encodepub_read = encode_read.to_public();
                                        reader.identity = Some(encode_read);
                                        match Recipient::from_str(&key) {
                                            Ok(key) => {
                                                {
                                                    let mut dlock = data.write().await;
                                                    dlock.is_connecting = false;
                                                    dlock.is_reconnecting = false;
                                                    dlock.is_connected = true;
                                                }
                                                writer.lock().await.recipient = Some(key);
                                            }
                                            Err(_e) => {
                                                #[cfg(feature = "__dev")]
                                                warn!("error while parsing key: {_e:?}");
                                                {
                                                    let mut dlock = data.write().await;
                                                    dlock.clear(true).await;
                                                    dlock.registered_errors.push(
                                                        ClientError::HandlerResultString(String::from(
                                                            "unable to set write key for data writer",
                                                        )),
                                                    );
                                                }
                                                return Ok(());
                                            }
                                        }
                                        write_packet!(WritePacket::Handshake(
                                            encodepub_read.to_string(),
                                        ));
                                    } else {
                                        let hwid = if let Ok(hwid) =
                                            IdBuilder::new(Encryption::SHA256)
                                                .add_component(HWIDComponent::CPUID)
                                                .add_component(HWIDComponent::MacAddress)
                                                .add_component(HWIDComponent::SystemID)
                                                .build(None)
                                        {
                                            hwid
                                        } else {
                                            return Err(Error::from(ErrorKind::InvalidData));
                                        };
                                        let dlock = data.read().await;
                                        write_packet!(WritePacket::InitializationHandshake(
                                            [
                                                0x3a0b1a04c51a2811,
                                                0x97a18f1dc9ee891d,
                                                0xfc7eb64f732a37fd,
                                                0xbe65cbabde15c305,
                                            ],
                                            1,
                                            hwid,
                                            dlock.game_id.clone(),
                                            dlock.version,
                                            dlock.session.clone()
                                        ));
                                    }
                                }
                                ReadPacket::SyncGameInfo(
                                    game_save,
                                    game_achievements,
                                    game_highscores,
                                    game_administrators,
                                    game_version,
                                ) => {
                                    let mut dlock = data.write().await;
                                    dlock.game_save = game_save;
                                    dlock.game_achievements = game_achievements;
                                    dlock.game_highscores = game_highscores;
                                    dlock.game_administrators = game_administrators;
                                    dlock.game_version = game_version;
                                    dlock.handshake_completed = true;
                                }
                                ReadPacket::Ping(ping) => {
                                    if let Some(ping) = ping {
                                        let mut dlock = data.write().await;
                                        dlock.ping = ping;
                                        dlock.last_ping = Some(Instant::now());
                                    } else {
                                        write_packet!(WritePacket::Ping());
                                    }
                                }
                                ReadPacket::ForceDisconnection() => {
                                    return Err(Error::new(
                                        ErrorKind::BrokenPipe,
                                        "forced disconnection registered",
                                    ));
                                }
                                ReadPacket::Registration(code) => {
                                    let mut dlock = data.write().await;
                                    if let Some(reg) = &mut dlock.func_register {
                                        reg(code);
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::Registration(code));
                                    }
                                }
                                ReadPacket::Login(code) => {
                                    let mut dlock = data.write().await;
                                    if let Some(reg) = &mut dlock.func_login {
                                        reg(code, None, None);
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::Login(code));
                                    }
                                }
                                ReadPacket::LoginOk(
                                    pid,
                                    pname,
                                    token,
                                    savefile,
                                    friends,
                                    incoming_friends,
                                    outgoing_friends,
                                    game_achievements,
                                ) => {
                                    let mut dlock = data.write().await;
                                    dlock.player_id = Some(pid);
                                    dlock.player_name = Some(pname.clone());
                                    dlock.player_save = savefile;
                                    dlock.game_achievements = game_achievements;
                                    dlock.player_friends =
                                        IntSet::from_iter(friends.iter().map(|pid| **pid));
                                    dlock.player_incoming_friends =
                                        IntSet::from_iter(incoming_friends.iter().map(|pid| **pid));
                                    dlock.player_outgoing_friends =
                                        IntSet::from_iter(outgoing_friends.iter().map(|pid| **pid));
                                    dlock.is_loggedin = true;
                                    if let Some(log) = &mut dlock.func_login {
                                        log(LoginCode::Ok, None, None);
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::LoginOk(pid, pname));
                                    }
                                    if let Some(token) = token {
                                        if let Some(log) = &mut dlock.func_login_token {
                                            log(token)
                                        }
                                    }
                                }
                                ReadPacket::LoginBan(code, reason, unban_time) => {
                                    let mut dlock = data.write().await;
                                    if let Some(log) = &mut dlock.func_login {
                                        log(
                                            code,
                                            DateTime::from_timestamp(unban_time, 0),
                                            Some(reason.clone()),
                                        );
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::LoginBan(code, reason.clone(), unban_time));
                                    }
                                }
                                ReadPacket::PlayerLoggedIn(pid, pname, vari, syncs, room) => {
                                    let mut dlock = data.write().await;
                                    dlock.players.insert(
                                        pid,
                                        Player {
                                            name: pname.clone(),
                                            room: room.clone(),
                                            syncs,
                                            variables: vari,
                                        },
                                    );
                                    // TODO: Handle dlock.player_queue
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::PlayerLoggedIn(pid, pname, room));
                                    }
                                }
                                ReadPacket::PlayerLoggedOut(pid) => {
                                    let mut dlock = data.write().await;
                                    dlock.players.remove(&pid);
                                    dlock.player_queue.remove(&pid);
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::PlayerLoggedOut(pid));
                                    }
                                }
                                ReadPacket::P2P(pid, mid, payload) => {
                                    let mut dlock = data.write().await;
                                    if let Some(p2p) = &mut dlock.func_p2p {
                                        p2p(pid.map(|v| *v), mid, payload.clone());
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::P2P(pid.map(|v| *v), mid, payload));
                                    }
                                }
                                ReadPacket::UpdatePlayerVariable(pid, upds) => {
                                    let mut dlock = data.write().await;
                                    if let Some(player) = dlock.players.get_mut(&pid) {
                                        for upd in &upds {
                                            if let OptionalVariable::Some(value) = upd.value.clone()
                                            {
                                                player
                                                    .variables
                                                    .insert(upd.name.clone(), value.clone());
                                            } else {
                                                player.variables.remove(&upd.name);
                                            }
                                        }
                                        for upd in upds {
                                            if let Some(dup) = &mut dlock.func_data_update {
                                                dup(DataUpdate::UpdateVariable(
                                                    pid, upd.name, upd.value,
                                                ));
                                            }
                                        }
                                    } else {
                                        if !dlock.player_queue.contains_key(&pid) {
                                            dlock
                                                .player_queue
                                                .entry(pid)
                                                .or_insert(PlayerQueue::default());
                                        }
                                        let pq = dlock
                                            .player_queue
                                            .get_mut(&pid)
                                            .expect("expected to have data stored");
                                        for upd in upds {
                                            pq.variables.insert(upd.name, upd.value);
                                        }
                                    }
                                }
                                ReadPacket::UpdateSync(pid, upds) => {
                                    // There's surely a better way to do this, more performantly.
                                    let mut dlock = data.write().await;
                                    let mut exists = IntSet::default();
                                    let mut add_pq = IntSet::default();
                                    if let Some(player) = dlock.players.get_mut(&pid) {
                                        for upd in &upds {
                                            if let Some(Some(sync)) = player.syncs.get_mut(upd.slot)
                                            {
                                                exists.insert(upd.slot);
                                                if let Some(vari) = &upd.variables {
                                                    for (vname, value) in vari {
                                                        if let OptionalVariable::Some(value) =
                                                            value.clone()
                                                        {
                                                            player
                                                                .variables
                                                                .insert(vname.clone(), value);
                                                        } else {
                                                            player.variables.remove(vname);
                                                        }
                                                    }
                                                } else if upd.remove_sync {
                                                    sync.is_ending = true;
                                                }
                                            } else {
                                                add_pq.insert(pid);
                                            }
                                        }
                                    } else {
                                        add_pq.insert(pid);
                                    }
                                    for pid in exists {
                                        for upd in &upds {
                                            if let Some(dup) = &mut dlock.func_data_update {
                                                if let Some(vari) = &upd.variables {
                                                    for (vname, value) in vari {
                                                        dup(DataUpdate::UpdateSyncVariable(
                                                            pid as u64,
                                                            upd.slot,
                                                            vname.clone(),
                                                            value.clone(),
                                                        ));
                                                    }
                                                } else if upd.remove_sync {
                                                    dup(DataUpdate::UpdateSyncRemoval(
                                                        pid as u64, upd.slot,
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                    for pid in add_pq {
                                        for upd in &upds {
                                            dlock
                                                .player_queue
                                                .entry(pid)
                                                .or_insert(PlayerQueue::default());
                                            let pq = dlock
                                                .player_queue
                                                .get_mut(&pid)
                                                .expect("expected to have data stored");
                                            if let Some(vari) = &upd.variables {
                                                pq.syncs.entry(upd.slot).or_insert(HashMap::new());
                                                let su = pq
                                                    .syncs
                                                    .get_mut(&upd.slot)
                                                    .expect("expected to have data stored");
                                                for (vname, value) in vari {
                                                    su.insert(vname.clone(), value.clone());
                                                }
                                            } else if upd.remove_sync
                                                && !pq.remove_syncs.contains(&upd.slot)
                                            {
                                                pq.remove_syncs.push(upd.slot);
                                            }
                                        }
                                    }
                                }
                                ReadPacket::ClearPlayers() => {
                                    let mut dlock = data.write().await;
                                    for pid in dlock.players.keys().cloned().collect::<Vec<u64>>() {
                                        if let Some(dup) = &mut dlock.func_data_update {
                                            dup(DataUpdate::PlayerLoggedOut(pid));
                                        }
                                    }
                                    dlock.players.clear();
                                }
                                ReadPacket::GameIniWrite(upds) => {
                                    let mut dlock = data.write().await;
                                    for upd in upds {
                                        if let OptionalVariable::Some(value) = upd.value.clone() {
                                            dlock.game_save.insert(upd.name.clone(), value.clone());
                                        } else {
                                            dlock.game_save.remove(&upd.name);
                                        }
                                        if let Some(dup) = &mut dlock.func_data_update {
                                            let keys = upd
                                                .name
                                                .split(">")
                                                .map(|entry| {
                                                    urlencoding::decode(entry).expect(
                                                        "unable to decode uri on gameini update",
                                                    )
                                                })
                                                .collect::<Vec<_>>();
                                            if keys.len() == 3 {
                                                dup(DataUpdate::UpdateGameIni(
                                                    Some(
                                                        keys.first()
                                                            .expect("unable to fetch key 0")
                                                            .to_string(),
                                                    ),
                                                    keys.get(1)
                                                        .expect("unable to fetch key 1")
                                                        .to_string(),
                                                    keys.get(2)
                                                        .expect("unable to fetch key 2")
                                                        .to_string(),
                                                    upd.value,
                                                ));
                                            } else {
                                                dup(DataUpdate::UpdateGameIni(
                                                    None,
                                                    keys.first()
                                                        .expect("unable to fetch key 0 on empty")
                                                        .to_string(),
                                                    keys.get(1)
                                                        .expect("unable to fetch key 1 on empty")
                                                        .to_string(),
                                                    upd.value,
                                                ));
                                            }
                                        }
                                    }
                                }
                                ReadPacket::NewSync(pid, upds) => {
                                    let mut dlock = data.write().await;
                                    for (slot, kind, stype, vari) in upds {
                                        if let Some(player) = dlock.players.get_mut(&pid) {
                                            player.syncs.insert(
                                                slot as usize,
                                                Some(types::Sync {
                                                    kind,
                                                    sync_type: stype,
                                                    variables: vari,
                                                    event: SyncEvent::New,
                                                    is_ending: false,
                                                }),
                                            );
                                        } else {
                                            dlock
                                                .player_queue
                                                .entry(pid)
                                                .or_insert(PlayerQueue::default());
                                            let pq = dlock
                                                .player_queue
                                                .get_mut(&pid)
                                                .expect("expected to have data stored");
                                            pq.new_syncs.push(NewSync {
                                                kind,
                                                slot: slot as usize,
                                                sync_type: stype,
                                                variables: vari,
                                            });
                                        }
                                    }
                                }
                                ReadPacket::PlayerChangedRooms(pid, room) => {
                                    let mut dlock = data.write().await;
                                    if let Some(player) = dlock.players.get_mut(&pid) {
                                        player.room = room;
                                    }
                                }
                                ReadPacket::HighscoreUpdate(pid, hid, score) => {
                                    let mut dlock = data.write().await;
                                    if let Some(highscore) =
                                        dlock.game_highscores.get_mut(&Leb(hid))
                                    {
                                        highscore.scores.insert(Leb(pid), score);
                                    }
                                }
                                ReadPacket::UpdatePlayerData(pid, syncs, vari) => {
                                    let mut dlock = data.write().await;
                                    if let Some(player) = dlock.players.get_mut(&pid) {
                                        player.syncs = syncs;
                                        player.variables = vari;
                                    }
                                }
                                ReadPacket::RequestPlayerVariable(index, vari) => {
                                    let mut dlock = data.write().await;
                                    if let Some(csu) =
                                        dlock.callback_server_update.remove(index as usize)
                                    {
                                        if let ServerUpdateCallback::PlayerVariable(
                                            callback,
                                            pid,
                                            name,
                                        ) = csu.callback
                                        {
                                            if let Some(player) = dlock.players.get_mut(&pid) {
                                                if let OptionalVariable::Some(value) = vari.clone()
                                                {
                                                    player.variables.insert(name.clone(), value);
                                                } else {
                                                    player.variables.remove(&*name);
                                                }
                                            }
                                            if let Some(mut callback) = callback {
                                                callback(pid, name, vari);
                                            }
                                        }
                                    }
                                    dlock.callback_server_update.remove(index as usize);
                                }
                                ReadPacket::AdminAction(aa) => {
                                    let mut dlock = data.write().await;
                                    match aa.clone() {
                                        AdminAction::Ban(reason, unban_time) => {
                                            if let Some(callback) = &mut dlock.func_banned {
                                                callback(
                                                    reason.clone(),
                                                    DateTime::from_timestamp(unban_time, 0)
                                                        .expect("unable to parse ban timestamp"),
                                                );
                                            }
                                            if let Some(dup) = dlock.func_data_update.as_mut() {
                                                dup(DataUpdate::Banned(
                                                    reason,
                                                    DateTime::from_timestamp(unban_time, 0)
                                                        .expect("unable to parse ban timestamp"),
                                                ));
                                            }
                                        }
                                        AdminAction::Kick(reason) => {
                                            if let Some(callback) = &mut dlock.func_kicked {
                                                callback(reason.clone());
                                            }
                                            if let Some(dup) = dlock.func_data_update.as_mut() {
                                                dup(DataUpdate::Kicked(reason));
                                            }
                                        }
                                        AdminAction::Unban => {
                                            panic!("this should never happen");
                                        }
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::AdminAction(aa));
                                    }
                                }
                                ReadPacket::RequestSyncVariable(index, vari) => {
                                    let mut dlock = data.write().await;
                                    if let Some(csu) =
                                        dlock.callback_server_update.remove(index as usize)
                                    {
                                        if let ServerUpdateCallback::SyncVariable(
                                            callback,
                                            pid,
                                            name,
                                            slot,
                                        ) = csu.callback
                                        {
                                            let obtained = if let Some(player) =
                                                dlock.players.get_mut(&pid)
                                            {
                                                if let Some(Some(sync)) = player.syncs.get_mut(slot)
                                                {
                                                    if let OptionalVariable::Some(value) =
                                                        vari.clone()
                                                    {
                                                        sync.variables.insert(name.clone(), value);
                                                    } else {
                                                        sync.variables.remove(&*name);
                                                    }
                                                }
                                                true
                                            } else {
                                                false
                                            };
                                            if obtained {
                                                if let Some(dup) = &mut dlock.func_data_update {
                                                    dup(DataUpdate::UpdateSyncVariable(
                                                        pid,
                                                        slot,
                                                        name.clone(),
                                                        vari.clone(),
                                                    ));
                                                }
                                            }
                                            if let Some(mut callback) = callback {
                                                callback(pid, name, vari);
                                            }
                                        }
                                    }
                                    dlock.callback_server_update.remove(index as usize);
                                }
                                ReadPacket::ChangeGameVersion(ver) => {
                                    let mut dlock = data.write().await;
                                    dlock.game_version = ver;
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::UpdateGameVersion(ver));
                                    }
                                }
                                ReadPacket::ModifyAdministrator(pid, admin) => {
                                    let mut dlock = data.write().await;
                                    dlock.game_administrators.insert(Leb(pid), admin);
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::UpdateAdministrator(pid, Some(admin)));
                                    }
                                }
                                ReadPacket::RemoveAdministrator(pid) => {
                                    let mut dlock = data.write().await;
                                    dlock.game_administrators.remove(&Leb(pid));
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::UpdateAdministrator(pid, None));
                                    }
                                }
                                ReadPacket::PlayerIniWrite(upds) => {
                                    let mut dlock = data.write().await;
                                    for upd in upds {
                                        if let OptionalVariable::Some(value) = upd.value.clone() {
                                            dlock.game_save.insert(upd.name.clone(), value.clone());
                                        } else {
                                            dlock.game_save.remove(&upd.name);
                                        }
                                        if let Some(dup) = &mut dlock.func_data_update {
                                            let keys = upd
                                                .name
                                                .split(">")
                                                .map(|entry| {
                                                    urlencoding::decode(entry).expect(
                                                        "unable to decode uri on playerini update",
                                                    )
                                                })
                                                .collect::<Vec<_>>();
                                            if keys.len() == 3 {
                                                dup(DataUpdate::UpdatePlayerIni(
                                                    Some(
                                                        keys.first()
                                                            .expect("unable to fetch key 0")
                                                            .to_string(),
                                                    ),
                                                    keys.get(1)
                                                        .expect("unable to fetch key 1")
                                                        .to_string(),
                                                    keys.first()
                                                        .expect("unable to fetch key 2")
                                                        .to_string(),
                                                    upd.value,
                                                ));
                                            } else {
                                                dup(DataUpdate::UpdatePlayerIni(
                                                    None,
                                                    keys.first()
                                                        .expect("unable to fetch key 0 on empty")
                                                        .to_string(),
                                                    keys.get(1)
                                                        .expect("unable to fetch key 1 on empty")
                                                        .to_string(),
                                                    upd.value,
                                                ));
                                            }
                                        }
                                    }
                                }
                                ReadPacket::RequestBdb(index, bdb) => {
                                    let mut dlock = data.write().await;
                                    if let Some(csu) =
                                        dlock.callback_server_update.remove(index as usize)
                                    {
                                        if let Some(callback) = &mut dlock.func_bdb {
                                            callback(csu.name.clone(), bdb.clone());
                                        }
                                        if let Some(dup) = &mut dlock.func_data_update {
                                            dup(DataUpdate::FetchBdb(csu.name.clone(), bdb));
                                        }
                                    }
                                }
                                ReadPacket::ChangeFriendStatus(cfs, pid) => {
                                    let mut dlock = data.write().await;
                                    // Tl;dr: This is stupid
                                    match cfs {
                                        ChangeFriendStatus::Request => {
                                            dlock.player_incoming_friends.insert(pid);
                                            dlock.player_friends.remove(&pid);
                                            dlock.player_outgoing_friends.remove(&pid);
                                        }
                                        ChangeFriendStatus::Accept | ChangeFriendStatus::Friend => {
                                            dlock.player_friends.insert(pid);
                                            dlock.player_incoming_friends.remove(&pid);
                                            dlock.player_outgoing_friends.remove(&pid);
                                        }
                                        ChangeFriendStatus::Deny
                                        | ChangeFriendStatus::Cancel
                                        | ChangeFriendStatus::Remove
                                        | ChangeFriendStatus::NotFriend => {
                                            dlock.player_friends.remove(&pid);
                                            dlock.player_incoming_friends.remove(&pid);
                                            dlock.player_outgoing_friends.remove(&pid);
                                        }
                                    }
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::ChangeFriendStatus(pid));
                                    }
                                }
                                ReadPacket::ServerMessage(msg) => {
                                    let mut dlock = data.write().await;
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::ServerMessage(msg.clone()));
                                    }
                                }
                                ReadPacket::ChangeConnection(host) => {
                                    let mut dlock = data.write().await;
                                    dlock.is_connecting = true;
                                    dlock.is_reconnecting = true;
                                    if let Some(dup) = &mut dlock.func_data_update {
                                        dup(DataUpdate::Reconnecting());
                                    }
                                    if let Ok((ws, _)) =
                                        tokio_tungstenite::connect_async(&host).await
                                    {
                                        let (sread, swrite) = StreamHandler::split_stream(ws).await;
                                        *writer.lock().await = swrite;
                                        reader = sread;
                                        dlock.last_host = Some(host);
                                    } else {
                                        return Err(Error::new(
                                            ErrorKind::ConnectionRefused,
                                            "unable to connect to new host",
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    Err(_e) => {
                        return Err(Error::new(ErrorKind::BrokenPipe, "invalid read data"));
                    }
                }
            }
        });
        if let Err(e) = task.await {
            {
                let mut dlock = cdata.write().await;
                dlock.clear(true).await;
                dlock.registered_errors.push(if e.is_panic() {
                    ClientError::HandlerPanic(format!("{:?}", e.into_panic()))
                } else {
                    ClientError::HandlerResult(Ok(())) // TODO: Change this into the actual error
                });
                if dlock.call_disconnected {
                    if let Some(func) = dlock.func_disconnected.as_mut() {
                        func();
                    }
                    if let Some(dup) = dlock.func_data_update.as_mut() {
                        dup(DataUpdate::Disconnected());
                    }
                }
            }
            cwriter.lock().await.shutdown().await;
        }
    }

    #[inline(always)]
    fn get_packet_write(data: &WritePacket) -> IoResult<Buffer> {
        #[cfg(feature = "__dev")]
        info!("writing packet: {data:?}");
        let mut b = Buffer::empty();
        match data {
            WritePacket::InitializationHandshake(
                hash,
                lib_version,
                device_id,
                game_id,
                version,
                session,
            ) => {
                b.write_leb_u64(0)?;
                for hash in hash {
                    b.write_u64(*hash)?;
                }
                b.write_leb_u64(*lib_version)?;
                b.write_string(device_id)?;
                b.write_string(game_id)?;
                b.write_f64(*version)?;
                b.write_string(session)?;
            }
            WritePacket::Login(username, passw, game_token, variables, syncs, room) => {
                b.write_leb_u64(1)?;
                b.write_string(username)?;
                match passw {
                    LoginPassw::Token(token) => {
                        b.write_bool(true)?;
                        b.write_string(token)?;
                    }
                    LoginPassw::Passw(passw) => {
                        b.write_bool(false)?;
                        b.write_string(passw)?;
                    }
                }
                b.write_string(game_token)?;
                b.write(variables)?;
                b.write(syncs)?;
                b.write_string(room)?;
            }
            WritePacket::Register(username, email, passw, repeat_passw) => {
                b.write_leb_u64(2)?;
                b.write_string(username)?;
                b.write_string(email)?;
                b.write_string(passw)?;
                b.write_string(repeat_passw)?;
            }
            WritePacket::RequestPlayerVariable(player_request, index, name) => {
                b.write_leb_u64(3)?;
                match player_request {
                    PlayerRequest::ID(pid) => {
                        b.write_bool(true)?;
                        b.write_leb_u64(*pid)?;
                    }
                    _ => {
                        b.write_bool(false)?;
                    }
                }
                b.write_string(name)?;
                b.write_leb_u64(*index)?;
            }
            WritePacket::P2P(player_request, mid, payload) => {
                b.write_leb_u64(4)?;
                b.write(player_request)?;
                b.write_i16(*mid)?;
                b.write(payload)?;
            }
            WritePacket::UpdateGameVersion(version) => {
                b.write_leb_u64(5)?;
                b.write_f64(*version)?;
            }
            WritePacket::UpdateGameSession(session) => {
                b.write_leb_u64(6)?;
                b.write_string(session)?;
            }
            WritePacket::UpdatePlayerVariable(updates) => {
                b.write_leb_u64(7)?;
                b.write(updates)?;
            }
            WritePacket::Ping() => {
                b.write_leb_u64(8)?;
            }
            WritePacket::GameIniWrite(updates) => {
                b.write_leb_u64(9)?;
                b.write(updates)?;
            }
            WritePacket::PlayerIniWrite(updates) => {
                b.write_leb_u64(10)?;
                b.write(updates)?;
            }
            WritePacket::UpdateRoom(room) => {
                b.write_leb_u64(11)?;
                b.write_string(room)?;
            }
            WritePacket::NewSync(upds) => {
                b.write_leb_u64(12)?;
                b.write_leb_u64(upds.len() as u64)?;
                for (slot, kind, sync_type, variables) in upds {
                    b.write_leb_u64(*slot)?;
                    b.write_i16(*kind)?;
                    b.write_u8(*sync_type as u8)?;
                    b.write(variables)?;
                }
            }
            WritePacket::UpdateSync(updates) => {
                b.write_leb_u64(13)?;
                b.write(updates)?;
            }
            WritePacket::UpdateAchievement(aid) => {
                b.write_leb_u64(14)?;
                b.write_leb_u64(*aid)?;
            }
            WritePacket::UpdateHighscore(hid, score) => {
                b.write_leb_u64(15)?;
                b.write_leb_u64(*hid)?;
                b.write_f64(*score)?;
            }
            WritePacket::AdminAction(admin_action, pid) => {
                b.write_leb_u64(16)?;
                b.write_leb_u64(*pid)?;
                match admin_action {
                    AdminAction::Ban(reason, time) => {
                        b.write_u8(0)?;
                        b.write_string(reason)?;
                        b.write_i64(*time)?;
                    }
                    AdminAction::Unban => {
                        b.write_u8(1)?;
                    }
                    AdminAction::Kick(reason) => {
                        b.write_u8(2)?;
                        b.write_string(reason)?;
                    }
                }
            }
            WritePacket::RequestSyncVariable(pid, index, slot, name) => {
                b.write_leb_u64(17)?;
                b.write_leb_u64(*pid)?;
                b.write_string(name)?;
                b.write_leb_u64(*index)?;
                b.write_leb_u64(*slot)?;
            }
            WritePacket::Logout() => {
                b.write_leb_u64(18)?;
            }
            WritePacket::RequestBdb(index, name) => {
                b.write_leb_u64(19)?;
                b.write_leb_u64(*index)?;
                b.write_string(name)?;
            }
            WritePacket::SetBdb(name, payload) => {
                b.write_leb_u64(20)?;
                b.write_string(name)?;
                b.write_bytes(payload)?;
            }
            WritePacket::RequestChangeFriendStatus(status, pid) => {
                b.write_leb_u64(21)?;
                b.write_u8(*status as u8)?;
                b.write_leb_u64(*pid)?;
            }
            WritePacket::Handshake(key) => {
                b.write_leb_u64(22)?;
                b.write_string(key)?;
            }
        }
        Ok(b)
    }

    #[inline(always)]
    fn get_packet_read(mut b: Buffer) -> IoResult<ReadPacket> {
        let event = b.read_leb_u64()?;
        match event {
            0 => {
                let code = RegistrationCode::try_from_primitive(b.read_u8()?).unwrap_or_default();
                Ok(ReadPacket::Registration(code))
            }
            1 => {
                let code = LoginCode::try_from_primitive(b.read_u8()?).unwrap_or_default();
                match code {
                    LoginCode::GameBan => {
                        let reason = b.read_string()?;
                        let unban_time = b.read_i64()?;
                        Ok(ReadPacket::LoginBan(code, reason, unban_time))
                    }
                    LoginCode::Ok => {
                        let pid = b.read_leb_u64()?;
                        let name = b.read_string()?;
                        let token = b.read()?;
                        let player_save = b.read()?;
                        let friends = b.read()?;
                        let incoming_friends = b.read()?;
                        let outgoing_friends = b.read()?;
                        let achievements = b.read()?;
                        Ok(ReadPacket::LoginOk(
                            pid,
                            name,
                            token,
                            player_save,
                            friends,
                            incoming_friends,
                            outgoing_friends,
                            achievements,
                        ))
                    }
                    _ => Ok(ReadPacket::Login(code)),
                }
            }
            2 => {
                let pid = b.read_leb_u64()?;
                let name = b.read_string()?;
                let room = b.read_string()?;
                let variables = b.read()?;
                let syncs = b.read()?;
                Ok(ReadPacket::PlayerLoggedIn(
                    pid, name, variables, syncs, room,
                ))
            }
            3 => {
                let pid = b.read_leb_u64()?;
                Ok(ReadPacket::PlayerLoggedOut(pid))
            }
            4 => {
                let game_save = b.read()?;
                let game_achievements = b.read()?;
                let game_highscores = b.read()?;
                let game_administrators = b.read()?;
                let version = b.read_f64()?;
                Ok(ReadPacket::SyncGameInfo(
                    game_save,
                    game_achievements,
                    game_highscores,
                    game_administrators,
                    version,
                ))
            }
            5 => {
                let pid = b.read()?;
                let mid = b.read_i16()?;
                let payload = b.read()?;
                Ok(ReadPacket::P2P(pid, mid, payload))
            }
            6 => {
                let pid = b.read_leb_u64()?;
                let updates = b.read()?;
                Ok(ReadPacket::UpdatePlayerVariable(pid, updates))
            }
            7 => {
                let ping = b.read()?;
                Ok(ReadPacket::Ping(ping))
            }
            8 => Ok(ReadPacket::ClearPlayers()),
            9 => {
                let updates = b.read()?;
                Ok(ReadPacket::GameIniWrite(updates))
            }
            10 => {
                let pid = b.read_leb_u64()?;
                let mut data = Vec::new();
                for _ in 0..b.read_leb_u64()? {
                    let slot = b.read_leb_u64()?;
                    let kind = b.read_i16()?;
                    let sync_type = SyncType::try_from_primitive(b.read_u8()?).unwrap_or_default();
                    let variables = b.read()?;
                    data.push((slot, kind, sync_type, variables));
                }
                Ok(ReadPacket::NewSync(pid, data))
            }
            11 => {
                let pid = b.read_leb_u64()?;
                let room = b.read_string()?;
                Ok(ReadPacket::PlayerChangedRooms(pid, room))
            }
            12 => {
                let pid = b.read_leb_u64()?;
                let sync_updates = b.read()?;
                Ok(ReadPacket::UpdateSync(pid, sync_updates))
            }
            13 => {
                let pid = b.read_leb_u64()?;
                let hid = b.read_leb_u64()?;
                let score = b.read_f64()?;
                Ok(ReadPacket::HighscoreUpdate(pid, hid, score))
            }
            14 => {
                let pid = b.read_leb_u64()?;
                let syncs = b.read()?;
                let variables = b.read()?;
                Ok(ReadPacket::UpdatePlayerData(pid, syncs, variables))
            }
            15 => {
                let slot = b.read_leb_u64()?;
                let variable = b.read()?;
                Ok(ReadPacket::RequestPlayerVariable(slot, variable))
            }
            16 => {
                if b.read_bool()? {
                    let reason = b.read_string()?;
                    Ok(ReadPacket::AdminAction(AdminAction::Kick(reason)))
                } else {
                    let reason = b.read_string()?;
                    let time = b.read_i64()?;
                    Ok(ReadPacket::AdminAction(AdminAction::Ban(reason, time)))
                }
            }
            17 => {
                let slot = b.read_leb_u64()?;
                let variable = b.read()?;
                Ok(ReadPacket::RequestSyncVariable(slot, variable))
            }
            18 => {
                let version = b.read_f64()?;
                Ok(ReadPacket::ChangeGameVersion(version))
            }
            19 => {
                if b.read_bool()? {
                    let pid = b.read_leb_u64()?;
                    let administrator = b.read()?;
                    Ok(ReadPacket::ModifyAdministrator(pid, administrator))
                } else {
                    let pid = b.read_leb_u64()?;
                    Ok(ReadPacket::RemoveAdministrator(pid))
                }
            }
            20 => Ok(ReadPacket::ForceDisconnection()),
            21 => {
                let updates = b.read()?;
                Ok(ReadPacket::PlayerIniWrite(updates))
            }
            22 => {
                let index = b.read_leb_u64()?;
                let data = b.read()?;
                Ok(ReadPacket::RequestBdb(index, data))
            }
            23 => {
                let status =
                    ChangeFriendStatus::try_from_primitive(b.read_u8()?).unwrap_or_default();
                let pid = b.read_leb_u64()?;
                Ok(ReadPacket::ChangeFriendStatus(status, pid))
            }
            24 => Ok(ReadPacket::Handshake(b.read()?)),
            25 => Ok(ReadPacket::ServerMessage(b.read_string()?)),
            26 => Ok(ReadPacket::ChangeConnection(b.read_string()?)),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("unknown event {event}, erroring out"),
            )),
        }
    }

    /// Update the Crystal Server runtime.
    /// This function sends a lot of data in a compressed manner,
    /// if this is NOT called, Crystal Server WILL not work PROPERLY.
    #[inline(always)]
    pub async fn update(&self) -> IoResult<()> {
        if self.is_connected().await {
            //let timestamp = Utc::now().timestamp();
            let mut dlock = self.data.write().await;
            if let Some(ping) = dlock.last_ping {
                if ping.elapsed().as_secs_f64() >= 90.0 {
                    self.disconnect().await;
                    if let Some(dup) = self.data.write().await.func_data_update.as_mut() {
                        dup(DataUpdate::Disconnected());
                    }
                }
            }
            if let Some(room_callback) = &mut dlock.func_room {
                let result = room_callback();
                if dlock.room != result {
                    self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateRoom(
                        result.clone(),
                    ))?)
                    .await?;
                }
                dlock.room = result;
            }
            let lroom = dlock.room.clone();
            for pid in dlock.players.keys().cloned().collect::<Vec<u64>>() {
                let player_logged_out = dlock.players_logout.contains(&pid);
                let mut is_all_none = true;
                let player = dlock
                    .players
                    .get_mut(&pid)
                    .ok_or(Error::new(ErrorKind::NotFound, "player not found"))?;
                for sync in &mut player.syncs {
                    if let Some(msync) = sync {
                        is_all_none = false;
                        if msync.event == SyncEvent::End {
                            drop(sync.take());
                            continue;
                        }
                        if msync.sync_type == SyncType::Once
                            || player.room != lroom
                            || player_logged_out
                        {
                            msync.event = SyncEvent::End;
                        }
                    }
                }
                if is_all_none && player_logged_out {
                    dlock.players.remove(&pid);
                    dlock.players_logout.remove(&pid);
                    if dlock.player_queue.contains_key(&pid) {
                        return Err(Error::new(
                            ErrorKind::AlreadyExists,
                            "found player queue but expected none",
                        ));
                    }
                }
            }
            if !dlock.syncs.is_empty() && !dlock.syncs_remove.is_empty() {
                let mut upds = Vec::new();
                for remove in dlock.syncs_remove.drain(..) {
                    upds.push(SyncUpdate {
                        slot: remove,
                        remove_sync: true,
                        variables: None,
                    });
                }
                for (index, sync) in dlock.syncs.iter_mut().enumerate() {
                    if let Some(sync) = sync {
                        if !sync.to_sync.is_empty() {
                            let mut variupd = HashMap::new();
                            for upd in sync.to_sync.drain() {
                                let vari = if let Some(vari) = sync.variables.get(&upd).cloned() {
                                    OptionalVariable::Some(vari)
                                } else {
                                    OptionalVariable::None
                                };
                                variupd.insert(upd, vari);
                            }
                            upds.push(SyncUpdate {
                                slot: index,
                                remove_sync: false,
                                variables: Some(variupd),
                            });
                        }
                    }
                }
                if !upds.is_empty() {
                    self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateSync(upds))?)
                        .await?;
                }
            }
            if !dlock.new_sync_queue.is_empty() {
                let mut data = Vec::new();
                let nsqvec = dlock.new_sync_queue.clone();
                dlock.new_sync_queue.clear();
                for nsq in nsqvec {
                    if let Some(sync) = dlock.syncs.get(nsq.slot).ok_or(Error::new(
                        ErrorKind::NotFound,
                        "expected sync but found none",
                    ))? {
                        data.push((
                            nsq.slot as u64,
                            nsq.kind,
                            nsq.sync_type,
                            sync.variables.clone(),
                        ));
                    } else {
                        return Err(Error::new(
                            ErrorKind::NotFound,
                            "expected sync on reserved slot but found none",
                        ));
                    }
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::NewSync(data))?)
                    .await?;
            }
            if !dlock.update_variable.is_empty() {
                let mut data = Vec::new();
                for (name, value) in dlock.update_variable.drain() {
                    data.push(VariableUpdate { name, value });
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::UpdatePlayerVariable(
                    data,
                ))?)
                .await?;
            }
            if !dlock.update_playerini.is_empty() {
                let mut data: Vec<VariableUpdate> = Vec::new();
                for (name, value) in dlock.update_playerini.drain() {
                    data.push(VariableUpdate { name, value });
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::PlayerIniWrite(data))?)
                    .await?;
            }
            if !dlock.update_gameini.is_empty() {
                let mut data: Vec<VariableUpdate> = Vec::new();
                for (name, value) in dlock.update_gameini.drain() {
                    data.push(VariableUpdate { name, value });
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::GameIniWrite(data))?)
                    .await?;
            }
        } else {
            let mut dlock = self.data.write().await;
            if dlock.is_loggedin {
                dlock.is_loggedin = false;
            }
            dlock.last_ping.take();
        }
        Ok(())
    }

    /// Set the room callback.
    pub async fn callback_set_room(&self, callback: CallbackRoom) {
        self.data.write().await.func_room = Some(callback);
    }

    /// Set the p2p callback.
    pub async fn callback_set_p2p(&self, callback: CallbackP2P) {
        self.data.write().await.func_p2p = Some(callback);
    }

    /// Set the register callback.
    pub async fn callback_set_register(&self, callback: CallbackRegister) {
        self.data.write().await.func_register = Some(callback);
    }

    /// Set the login callback.
    pub async fn callback_set_login(&self, callback: CallbackLogin) {
        self.data.write().await.func_login = Some(callback);
    }

    /// Set the banned callback.
    pub async fn callback_set_banned(&self, callback: CallbackBanned) {
        self.data.write().await.func_banned = Some(callback);
    }

    /// Set the kicked callback.
    pub async fn callback_set_kicked(&self, callback: CallbackKicked) {
        self.data.write().await.func_kicked = Some(callback);
    }

    /// Set the disconnected callback.
    pub async fn callback_set_disconnected(&self, callback: CallbackDisconnected) {
        self.data.write().await.func_disconnected = Some(callback);
    }

    /// Set the login token callback.
    pub async fn callback_set_login_token(&self, callback: CallbackLoginToken) {
        self.data.write().await.func_login_token = Some(callback);
    }

    /// Set the p2p callback.
    pub async fn callback_set_data_update(&self, callback: CallbackDataUpdate) {
        self.data.write().await.func_data_update = Some(callback);
    }

    /// Checks if the client has an active connection to the server.
    pub async fn is_connected(&self) -> bool {
        let dlock = self.data.read().await;
        dlock.is_connected && dlock.handshake_completed && {
            if let Some(thread) = &dlock.thread {
                !thread.is_finished()
            } else {
                true
            }
        }
    }

    /// Checks if the client is trying to establish an active connection to the server.
    pub async fn is_connecting(&self) -> bool {
        let dlock = self.data.read().await;
        dlock.is_connecting || (!dlock.handshake_completed && dlock.is_connected)
    }

    /// Check if the client is logged into the game.
    pub async fn is_loggedin(&self) -> bool {
        self.is_connected().await && {
            let dlock = self.data.read().await;
            dlock.is_loggedin && dlock.handshake_completed
        }
    }

    /// Obtain the ping between the client and the server.
    pub async fn get_ping(&self) -> f64 {
        self.data.read().await.ping
    }

    /// Sets the Game Token so that the player is able to log-in to the servers.
    pub async fn set_game_token(&self, token: &str) {
        self.data.write().await.game_token = token.to_owned();
    }

    /// Disconnects the client from the server on an on-going active connection.
    pub async fn disconnect(&self) {
        let mut lock = self.data.write().await;
        if let Some(thread) = lock.thread.take() {
            thread.abort();
        }
        lock.clear(true).await;
    }

    async fn internal_iosend(&self, data: Buffer) -> IoResult<()> {
        if let Some(writer) = &self.writer {
            match writer.lock().await.write(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    #[cfg(feature = "__dev")]
                    info!("unable to send data to server with error: {e:?}");
                    Err(Error::new(ErrorKind::BrokenPipe, format!("{e:?}")))
                }
            }
        } else {
            Err(Error::new(
                ErrorKind::BrokenPipe,
                "stream is not open for writing",
            ))
        }
    }

    async fn internal_login(&self, username: &str, loginpassw: LoginPassw) -> IoResult<()> {
        self.internal_iosend({
            let dlock = self.data.read().await;
            Self::get_packet_write(&WritePacket::Login(
                username.to_owned(),
                loginpassw,
                dlock.game_token.clone(),
                dlock.variables.clone(),
                dlock.syncs.clone(),
                dlock.room.clone(),
            ))?
        })
        .await
    }

    /// Try to log into an existing Crystal Server account.
    ///
    /// To receive the login result use `set_login_callback` to set the callback.
    /// If the login was successful it will also call what was set on `set_login_token_callback`.
    pub async fn login(&self, username: &str, passw: &str) -> IoResult<()> {
        self.internal_login(username, LoginPassw::Passw(passw.to_owned()))
            .await
    }

    /// Try to log into an existing Crystal Server account using a login-token.
    /// This token is only usable once, and will be regenerated and received
    ///
    /// To receive the login result use `set_login_callback` to set the callback.
    /// If the login was successful on `set_login_token_callback`.
    pub async fn login_with_token(&self, username: &str, token: &str) -> IoResult<()> {
        self.internal_login(username, LoginPassw::Token(token.to_owned()))
            .await
    }

    /// Try to register a new Crystal Server account.
    ///
    /// To receive the register result use `set_register_callback`.
    pub async fn register(
        &self,
        username: &str,
        email: &str,
        passw: &str,
        repeat_passw: &str,
    ) -> IoResult<()> {
        self.internal_iosend(Self::get_packet_write(&WritePacket::Register(
            username.to_owned(),
            email.to_owned(),
            passw.to_owned(),
            repeat_passw.to_owned(),
        ))?)
        .await
    }

    /// Get the current Player ID if logged in, otherwise returns None.
    pub async fn get_player_id(&self) -> Option<u64> {
        self.data.read().await.player_id
    }

    /// Get the current Player Name if logged in, otherwise returns None.
    pub async fn get_player_name(&self) -> Option<String> {
        self.data.read().await.player_name.clone()
    }

    /// Sets the player variable with the name and value provided.
    pub async fn set_variable(&self, name: &str, value: Variable) {
        self.data
            .write()
            .await
            .update_variable
            .insert(name.to_owned(), OptionalVariable::Some(value));
    }

    /// Removes the player variable with the provided name.
    pub async fn remove_variable(&self, name: &str) {
        self.data
            .write()
            .await
            .update_variable
            .insert(name.to_owned(), OptionalVariable::None);
    }

    /// Stream to fetch all current player (excluding this client) data.
    pub async fn iter_other_players(&self) -> impl Stream<Item = (u64, Player)> {
        let data = self.data.clone();
        async_stream::stream! {
            for id in data.read().await.players.keys().cloned().collect::<Vec<u64>>() {
                if let Some(player) = data.read().await.players.get(&id).cloned() {
                    yield (id, player);
                }
            }
        }
    }

    /// Gets the current amount of players. (excluding this client.)
    pub async fn other_players_count(&self) -> usize {
        self.data.read().await.players.len()
    }

    /// Gets a player data if the player is connected.
    pub async fn get_other_player(&self, pid: u64) -> Option<Player> {
        self.data.read().await.players.get(&pid).cloned()
    }

    /// Gets a player data through the name if the player is connected.
    pub async fn get_other_player_name(&self, name: &str) -> Option<(u64, Player)> {
        self.data
            .read()
            .await
            .players
            .iter()
            .find(|(_pid, player)| player.name == name)
            .map(|(pid, player)| (*pid, player.clone()))
    }

    /// Request a player variable update.
    /// This allows you to fetch a player's variable
    /// even if the server is currently not syncing it
    /// to you.
    pub async fn request_other_player_variable(
        &self,
        pid: u64,
        name: &str,
        callback: Option<PlayerVariableServerUpdate>,
    ) -> IoResult<()> {
        let mut dlock = self.data.write().await;
        if dlock.players.contains_key(&pid) {
            let index = if let Some((index, csu)) = dlock
                .callback_server_update
                .iter_mut()
                .enumerate()
                .find(|(_, csu)| csu.is_none())
            {
                *csu = Some(CallbackServerUpdate {
                    name: name.to_owned(),
                    callback: ServerUpdateCallback::PlayerVariable(callback, pid, name.to_owned()),
                });
                index
            } else {
                dlock
                    .callback_server_update
                    .push(Some(CallbackServerUpdate {
                        name: name.to_owned(),
                        callback: ServerUpdateCallback::PlayerVariable(
                            callback,
                            pid,
                            name.to_owned(),
                        ),
                    }));
                dlock.callback_server_update.len() - 1
            };
            self.internal_iosend(Self::get_packet_write(
                &WritePacket::RequestPlayerVariable(
                    PlayerRequest::ID(pid),
                    index as u64,
                    name.to_owned(),
                ),
            )?)
            .await?;
            Ok(())
        } else {
            Err(Error::new(ErrorKind::NotFound, "unable to find player id"))
        }
    }

    /// Sends peer-to-peer payload to the desired players.
    pub async fn p2p(
        &self,
        target: PlayerRequest,
        message_id: i16,
        payload: Vec<Variable>,
    ) -> IoResult<()> {
        self.internal_iosend(Self::get_packet_write(&WritePacket::P2P(
            target, message_id, payload,
        ))?)
        .await
    }

    /// Sets the current game version.
    /// This function will return an Error if it's not connected to the server,
    /// but this can be ignored safely.
    pub async fn set_version(&self, version: f64) -> IoResult<()> {
        self.data.write().await.version = version;
        self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateGameVersion(
            version,
        ))?)
        .await
    }

    /// Gets the current set version.
    pub async fn get_version(&self) -> f64 {
        self.data.read().await.version
    }

    /// Gets the registered server version.
    pub async fn get_server_version(&self) -> f64 {
        self.data.read().await.game_version
    }

    /// Sets the current game session.
    /// This function will return an Error if it's not connected to the server,
    /// but this can be ignored safely.
    pub async fn set_session(&self, session: &str) -> IoResult<()> {
        self.data.write().await.session = session.to_owned();
        self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateGameSession(
            session.to_owned(),
        ))?)
        .await
    }

    /// Gets the current set session.
    pub async fn get_session(&self) -> String {
        self.data.read().await.session.clone()
    }

    fn get_save_key(file: &str, section: &str, key: &str) -> String {
        if file.is_empty() {
            format!(
                "{}>{}",
                urlencoding::encode(section),
                urlencoding::encode(key)
            )
        } else {
            format!(
                "{}>{}>{}",
                urlencoding::encode(file),
                urlencoding::encode(section),
                urlencoding::encode(key)
            )
        }
    }

    /// Gets the currently open playerini file.
    /// The result will be empty is no files are open.
    pub async fn get_open_playerini(&self) -> String {
        self.data.read().await.player_open_save.clone()
    }

    /// Sets the currently open playerini file.
    pub async fn open_playerini(&self, file: &str) {
        self.data.write().await.player_open_save = file.to_owned();
    }

    /// Closes the currently open playerini file.
    pub async fn close_playerini(&self) {
        self.data.write().await.player_open_save.clear();
    }

    /// Checks if requested entry exists in the currently open playerini file.
    pub async fn has_playerini(&self, section: &str, key: &str) -> bool {
        let dlock = self.data.read().await;
        dlock
            .player_save
            .contains_key(&Self::get_save_key(&dlock.player_open_save, section, key))
    }

    /// Returns the value stored on the requested entry in the currently open playerini file.
    /// Will return None if the value doesn't exist.
    pub async fn get_playerini(&self, section: &str, key: &str) -> Option<Variable> {
        let dlock = self.data.read().await;
        dlock
            .player_save
            .get(&Self::get_save_key(&dlock.player_open_save, section, key))
            .cloned()
    }

    /// Sets the value for the requested entry in the currently open playerini file.
    pub async fn set_playerini(&self, section: &str, key: &str, value: Variable) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.player_open_save, section, key);
        dlock.player_save.insert(save_key.clone(), value.clone());
        dlock
            .update_playerini
            .insert(save_key, OptionalVariable::Some(value));
    }

    /// Removes the stored value for the requested entry in the currently open playerini file.
    pub async fn remove_playerini(&self, section: &str, key: &str) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.player_open_save, section, key);
        dlock.player_save.remove(&save_key);
        dlock
            .update_playerini
            .insert(save_key, OptionalVariable::None);
    }

    /// Gets the currently open gameini file.
    /// The result will be empty is no files are open.
    pub async fn get_open_gameini(&self) -> String {
        self.data.read().await.game_open_save.clone()
    }

    /// Sets the currently open gameini file.
    pub async fn open_gameini(&self, file: &str) {
        self.data.write().await.game_open_save = file.to_owned();
    }

    /// Closes the currently open gameini file.
    pub async fn close_gameini(&self) {
        self.data.write().await.game_open_save.clear();
    }

    /// Checks if requested entry exists in the currently open gameini file.
    pub async fn has_gameini(&self, section: &str, key: &str) -> bool {
        let dlock = self.data.read().await;
        dlock
            .game_save
            .contains_key(&Self::get_save_key(&dlock.game_open_save, section, key))
    }

    /// Returns the value stored on the requested entry in the currently open gameini file.
    /// Will return None if the value doesn't exist.
    pub async fn get_gameini(&self, section: &str, key: &str) -> Option<Variable> {
        let dlock = self.data.read().await;
        dlock
            .game_save
            .get(&Self::get_save_key(&dlock.game_open_save, section, key))
            .cloned()
    }

    /// Sets the value for the requested entry in the currently open gameini file.
    pub async fn set_gameini(&self, section: &str, key: &str, value: Variable) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.game_open_save, section, key);
        dlock.game_save.insert(save_key.clone(), value.clone());
        dlock
            .update_gameini
            .insert(save_key, OptionalVariable::Some(value));
    }

    /// Removes the stored value for the requested entry in the currently open gameini file.
    pub async fn remove_gameini(&self, section: &str, key: &str) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.game_open_save, section, key);
        dlock.game_save.remove(&save_key);
        dlock
            .update_gameini
            .insert(save_key, OptionalVariable::None);
    }

    /// Checks if the requested achievement exists.
    pub async fn has_achievement(&self, aid: u64) -> bool {
        self.data
            .read()
            .await
            .game_achievements
            .contains_key(&Leb(aid))
    }

    /// Returns the achievement on the requested id.
    /// Will return None if the value doesn't exist.
    pub async fn get_achievement(&self, aid: u64) -> Option<Achievement> {
        self.data
            .read()
            .await
            .game_achievements
            .get(&Leb(aid))
            .cloned()
    }

    /// Checks if the player has reached the requested
    /// achievement.
    pub async fn has_reached_achievement(&self, aid: u64) -> bool {
        let dlock = self.data.read().await;
        if dlock.player_id.is_some() {
            dlock
                .game_achievements
                .get(&Leb(aid))
                .map(|achievement| achievement.unlocked.is_some())
                .unwrap_or(false)
        } else {
            false
        }
    }

    /// Get the timestamp (Unix UTC) of when the achievement
    /// was unlocked if the player has.
    pub async fn get_reached_achievement(&self, aid: u64) -> Option<i64> {
        let dlock = self.data.read().await;
        if dlock.player_id.is_some() {
            dlock
                .game_achievements
                .get(&Leb(aid))
                .map(|achievement| achievement.unlocked)?
        } else {
            None
        }
    }

    /// If the player hasn't reached the achievement
    /// then you can reach it with this function.
    pub async fn reach_achievement(&self, aid: u64) -> IoResult<()> {
        if !self.has_reached_achievement(aid).await {
            let mut dlock = self.data.write().await;
            if dlock.player_id.is_some() {
                if let Some(achievement) = dlock.game_achievements.get_mut(&Leb(aid)) {
                    achievement.unlocked = Some(Utc::now().timestamp());
                    self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateAchievement(
                        aid,
                    ))?)
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Checks if the requested highscore exists.
    pub async fn has_highscore(&self, hid: u64) -> bool {
        self.data
            .read()
            .await
            .game_achievements
            .contains_key(&Leb(hid))
    }

    /// Returns the highscore on the requested id.
    /// Will return None if the value doesn't exist.
    pub async fn get_highscore(&self, hid: u64) -> Option<Highscore> {
        self.data
            .read()
            .await
            .game_highscores
            .get(&Leb(hid))
            .cloned()
    }

    /// Checks if the player has reached the requested
    /// highscore.
    pub async fn has_score_highscore(&self, hid: u64) -> bool {
        let dlock = self.data.read().await;
        if let Some(player_id) = dlock.player_id {
            if let Some(highscore) = dlock.game_highscores.get(&Leb(hid)) {
                highscore.scores.contains_key(&Leb(player_id))
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Returns the player's highscore on the requested
    /// highscore id.
    pub async fn get_score_highscore(&self, hid: u64) -> Option<f64> {
        let dlock = self.data.read().await;
        if let Some(player_id) = dlock.player_id {
            if let Some(highscore) = dlock.game_highscores.get(&Leb(hid)) {
                highscore.scores.get(&Leb(player_id)).cloned()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// If the player hasn't reached the highscore
    /// then you can reach it with this function.
    pub async fn set_score_highscore(&self, hid: u64, score: f64) -> IoResult<()> {
        let mut dlock = self.data.write().await;
        if let Some(player_id) = dlock.player_id {
            if let Some(highscore) = dlock.game_highscores.get_mut(&Leb(hid)) {
                if let Some(hscore) = highscore.scores.get_mut(&Leb(player_id)) {
                    if *hscore != score {
                        *hscore = score;
                        self.internal_iosend(Self::get_packet_write(
                            &WritePacket::UpdateHighscore(hid, score),
                        )?)
                        .await?;
                    }
                } else {
                    highscore.scores.insert(Leb(player_id), score);
                    self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateHighscore(
                        hid, score,
                    ))?)
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Create a new sync.
    pub async fn create_sync(&self, sync_type: SyncType, kind: i16) -> usize {
        let mut dlock = self.data.write().await;
        let slot = if let Some((index, sync)) = dlock
            .syncs
            .iter_mut()
            .enumerate()
            .find(|(_, sync)| sync.is_none())
        {
            *sync = Some(SelfSync {
                kind,
                sync_type,
                variables: HashMap::new(),
                to_sync: HashSet::new(),
            });
            index
        } else {
            dlock.syncs.push(Some(SelfSync {
                kind,
                sync_type,
                variables: HashMap::new(),
                to_sync: HashSet::new(),
            }));
            dlock.syncs.len() - 1
        };
        dlock.new_sync_queue.push(NewSyncQueue {
            slot,
            kind,
            sync_type,
        });
        slot
    }

    /// Destroy a created sync.
    pub async fn destroy_sync(&self, sync: usize) {
        let mut dlock = self.data.write().await;
        if let Some(Some(_)) = dlock.syncs.get(sync) {
            dlock.syncs_remove.push(sync);
        }
    }

    /// Set a sync variable.
    pub async fn set_variable_sync(&self, sync: usize, name: &str, value: Variable) {
        let mut dlock = self.data.write().await;
        if let Some(Some(sync)) = dlock.syncs.get_mut(sync) {
            if let Some(old_value) = sync.variables.get(name) {
                if old_value == &value {
                    return;
                }
            }
            sync.variables.insert(name.to_owned(), value);
            sync.to_sync.insert(name.to_owned());
        }
    }

    /// Remove a sync variable.
    pub async fn remove_variable_sync(&self, sync: usize, name: &str) {
        let mut dlock = self.data.write().await;
        if let Some(Some(sync)) = dlock.syncs.get_mut(sync) {
            if sync.variables.remove(name).is_some() {
                sync.to_sync.insert(name.to_owned());
            }
        }
    }

    /// Gets a sync variable.
    pub async fn get_variable_other_sync(
        &self,
        pid: u64,
        sync: usize,
        name: &str,
    ) -> Option<Variable> {
        let dlock = self.data.read().await;
        if let Some(player) = dlock.players.get(&pid) {
            if let Some(Some(sync)) = player.syncs.get(sync) {
                sync.variables.get(name).cloned()
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Checks if a sync variable exists.
    pub async fn has_variable_other_sync(&self, pid: u64, sync: usize, name: &str) -> Option<bool> {
        let dlock = self.data.read().await;
        if let Some(player) = dlock.players.get(&pid) {
            if let Some(Some(sync)) = player.syncs.get(sync) {
                Some(sync.variables.contains_key(name))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Stream to fetch all current syncs from players (excluding this client) data.
    pub async fn iter_other_syncs(&self) -> impl Stream<Item = SyncIter> {
        let data = self.data.clone();

        Box::pin(async_stream::stream! {
            for id in data.read().await.players.keys().cloned().collect::<Vec<u64>>() {
                let player_data = data.read().await.players.get(&id).cloned();
                if let Some(player) = player_data {
                    let syncs_data = player.syncs.iter().cloned().enumerate();
                    for (index, sync) in syncs_data {
                        if let Some(sync) = sync {
                            {
                                if data.read().await.room != player.room && sync.event != SyncEvent::End {
                                    continue;
                                }
                            }
                            let iter = SyncIter {
                                player_id: id,
                                player_name: player.name.clone(),
                                slot: index,
                                event: sync.event,
                                kind: sync.kind,
                                variables: sync.variables.clone(),
                            };
                            if sync.event == SyncEvent::New {
                                if let Some(player) = data.write().await.players.get_mut(&id) {
                                    if let Some(Some(sync)) = player.syncs.get_mut(index) {
                                        if sync.event == SyncEvent::New { // Make sure nothing happened while we were iterating
                                            sync.event = SyncEvent::Step;
                                        }
                                    }
                                }
                            }
                            yield iter;
                        }
                    }
                }
            }
        })
    }

    /// Checks if the player id is an admin, this also includes this client's player.
    pub async fn is_player_admin(&self, pid: u64) -> bool {
        self.data
            .read()
            .await
            .game_administrators
            .contains_key(&Leb(pid))
    }

    /// Obtains the player id administrator data, if they are one.
    /// This also includes this client's player.
    pub async fn get_player_admin(&self, pid: u64) -> Option<Administrator> {
        self.data
            .read()
            .await
            .game_administrators
            .get(&Leb(pid))
            .cloned()
    }

    /// Kicks the player id from the game, if the current player is an
    /// administrator and has permission to do so, otherwise it checks
    /// if it's trying to kick itself.
    /// Returns a bool if the action succeded or not, an Err(...)
    /// if there was an issue sending the data to the server.
    pub async fn player_kick(&self, pid: u64, reason: &str) -> IoResult<bool> {
        if self.get_player_id().await == Some(pid)
            || self
                .get_player_admin(self.get_player_id().await.unwrap_or(u64::MAX))
                .await
                .unwrap_or_default()
                .can_kick
        {
            self.internal_iosend(Self::get_packet_write(&WritePacket::AdminAction(
                AdminAction::Kick(reason.to_owned()),
                pid,
            ))?)
            .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Bans the player id from the game, if the current player is an
    /// administrator and has permission to do so, otherwise it checks
    /// if it's trying to ban itself.
    /// Returns a bool if the action succeded or not, an Err(...)
    /// if there was an issue sending the data to the server.
    pub async fn player_ban(
        &self,
        pid: u64,
        reason: &str,
        unban_time: DateTime<Utc>,
    ) -> IoResult<bool> {
        if self.get_player_id().await == Some(pid)
            || self
                .get_player_admin(self.get_player_id().await.unwrap_or(u64::MAX))
                .await
                .unwrap_or_default()
                .can_ban
        {
            self.internal_iosend(Self::get_packet_write(&WritePacket::AdminAction(
                AdminAction::Ban(reason.to_string(), unban_time.timestamp()),
                pid,
            ))?)
            .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Unbans the player id from the game, if the current player is an
    /// administrator and has permission to do so.
    /// Returns a bool if the action succeded or not, an Err(...)
    /// if there was an issue sending the data to the server.
    pub async fn player_unban(&self, pid: u64) -> IoResult<bool> {
        if self
            .get_player_admin(self.get_player_id().await.unwrap_or(u64::MAX))
            .await
            .unwrap_or_default()
            .can_unban
        {
            self.internal_iosend(Self::get_packet_write(&WritePacket::AdminAction(
                AdminAction::Unban,
                pid,
            ))?)
            .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Logs out from the currently connected account.
    /// Returns a bool if the action succeded or not, an Err(...)
    /// if there was an issue sending the data to the server.
    pub async fn logout(&self) -> IoResult<bool> {
        if self.is_loggedin().await {
            self.internal_iosend(Self::get_packet_write(&WritePacket::Logout())?)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Request a sync variable update.
    /// This allows you to fetch a syncs' variable
    /// even if the server is currently not syncing it
    /// to you.
    pub async fn request_other_sync_variable(
        &self,
        pid: u64,
        slot: usize,
        name: &str,
        callback: Option<SyncVariableServerUpdate>,
    ) -> IoResult<()> {
        let mut dlock = self.data.write().await;
        if let Some(player) = dlock.players.get(&pid) {
            if let Some(Some(_)) = player.syncs.get(slot) {
                let index = if let Some((index, csu)) = dlock
                    .callback_server_update
                    .iter_mut()
                    .enumerate()
                    .find(|(_, csu)| csu.is_none())
                {
                    *csu = Some(CallbackServerUpdate {
                        name: name.to_owned(),
                        callback: ServerUpdateCallback::SyncVariable(
                            callback,
                            pid,
                            name.to_owned(),
                            slot,
                        ),
                    });
                    index
                } else {
                    dlock
                        .callback_server_update
                        .push(Some(CallbackServerUpdate {
                            name: name.to_owned(),
                            callback: ServerUpdateCallback::SyncVariable(
                                callback,
                                pid,
                                name.to_owned(),
                                slot,
                            ),
                        }));
                    dlock.callback_server_update.len() - 1
                };
                self.internal_iosend(Self::get_packet_write(&WritePacket::RequestSyncVariable(
                    pid,
                    index as u64,
                    slot as u64,
                    name.to_owned(),
                ))?)
                .await?;
                Ok(())
            } else {
                Err(Error::new(ErrorKind::NotFound, "unable to find sync"))
            }
        } else {
            Err(Error::new(ErrorKind::NotFound, "unable to find player id"))
        }
    }

    /// Fetch a Binary Data Block.
    /// A BDB is like a binary savefile, but manually obtained/set
    /// and is async, meaning you need to wait time to get the data.
    pub async fn fetch_bdb(
        &self,
        name: &str,
        callback: Option<FetchBdbServerUpdate>,
    ) -> IoResult<()> {
        let mut dlock = self.data.write().await;
        let index = if let Some((index, csu)) = dlock
            .callback_server_update
            .iter_mut()
            .enumerate()
            .find(|(_, csu)| csu.is_none())
        {
            *csu = Some(CallbackServerUpdate {
                name: name.to_owned(),
                callback: ServerUpdateCallback::FetchBdb(callback, name.to_owned()),
            });
            index
        } else {
            dlock
                .callback_server_update
                .push(Some(CallbackServerUpdate {
                    name: name.to_owned(),
                    callback: ServerUpdateCallback::FetchBdb(callback, name.to_owned()),
                }));
            dlock.callback_server_update.len() - 1
        };
        self.internal_iosend(Self::get_packet_write(&WritePacket::RequestBdb(
            index as u64,
            name.to_owned(),
        ))?)
        .await?;
        Ok(())
    }

    /// Set the data of a Binary Data Block.
    /// A BDB is like a binary savefile, but manually obtained/set
    /// and is async, meaning you need to wait time to get the data.
    pub async fn set_bdb(&self, name: &str, data: Vec<u8>) -> IoResult<()> {
        self.internal_iosend(Self::get_packet_write(&WritePacket::SetBdb(
            name.to_owned(),
            data,
        ))?)
        .await
    }

    /// Obtain incoming friend requests.
    pub async fn get_incoming_friends(&self) -> IntSet<u64> {
        self.data.read().await.player_incoming_friends.clone()
    }

    /// Obtain outgoing friend requests.
    pub async fn get_outgoing_friends(&self) -> IntSet<u64> {
        self.data.read().await.player_outgoing_friends.clone()
    }

    /// Obtain player friends.
    pub async fn get_friends(&self) -> IntSet<u64> {
        self.data.read().await.player_friends.clone()
    }

    /// Request a friend request to the specified player.
    pub async fn send_outgoing_friend(&self, pid: u64) -> IoResult<()> {
        if self.is_loggedin().await {
            {
                let mut dlock = self.data.write().await;
                if dlock.player_friends.contains(&pid)
                    || dlock.player_outgoing_friends.contains(&pid)
                    || dlock.player_incoming_friends.contains(&pid)
                {
                    return Ok(());
                }
                dlock.player_outgoing_friends.insert(pid);
            }
            self.internal_iosend(Self::get_packet_write(
                &WritePacket::RequestChangeFriendStatus(ChangeFriendStatus::Request, pid),
            )?)
            .await?;
        }
        Ok(())
    }

    /// Remove the outgoing friend request of the specified player.
    pub async fn remove_outgoing_friend(&self, pid: u64) -> IoResult<()> {
        if self.is_loggedin().await {
            {
                let mut dlock = self.data.write().await;
                if !dlock.player_outgoing_friends.contains(&pid) {
                    return Ok(());
                }
                dlock.player_outgoing_friends.remove(&pid);
            }
            self.internal_iosend(Self::get_packet_write(
                &WritePacket::RequestChangeFriendStatus(ChangeFriendStatus::Cancel, pid),
            )?)
            .await?;
        }
        Ok(())
    }

    /// Deny the incoming friend request of the specified player.
    pub async fn deny_incoming_friend(&self, pid: u64) -> IoResult<()> {
        if self.is_loggedin().await {
            {
                let mut dlock = self.data.write().await;
                if !dlock.player_incoming_friends.contains(&pid) {
                    return Ok(());
                }
                dlock.player_incoming_friends.remove(&pid);
            }
            self.internal_iosend(Self::get_packet_write(
                &WritePacket::RequestChangeFriendStatus(ChangeFriendStatus::Deny, pid),
            )?)
            .await?;
        }
        Ok(())
    }

    /// Accept the incoming friend request of the specified player.
    pub async fn accept_incoming_friend(&self, pid: u64) -> IoResult<()> {
        if self.is_loggedin().await {
            {
                let mut dlock = self.data.write().await;
                if !dlock.player_incoming_friends.contains(&pid)
                    || dlock.player_friends.contains(&pid)
                {
                    return Ok(());
                }
                dlock.player_incoming_friends.remove(&pid);
                dlock.player_friends.insert(pid);
            }
            self.internal_iosend(Self::get_packet_write(
                &WritePacket::RequestChangeFriendStatus(ChangeFriendStatus::Accept, pid),
            )?)
            .await?;
        }
        Ok(())
    }

    /// Remove a friend of the specified player.
    pub async fn remove_friend(&self, pid: u64) -> IoResult<()> {
        if self.is_loggedin().await {
            {
                let mut dlock = self.data.write().await;
                if !dlock.player_friends.contains(&pid) {
                    return Ok(());
                }
                dlock.player_friends.remove(&pid);
            }
            self.internal_iosend(Self::get_packet_write(
                &WritePacket::RequestChangeFriendStatus(ChangeFriendStatus::Remove, pid),
            )?)
            .await?;
        }
        Ok(())
    }
}
