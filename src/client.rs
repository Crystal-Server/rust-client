use crate::types::NewSync;

use super::{
    buffer::Buffer,
    leb::Leb,
    types::{
        self, Achievement, AdminAction, Administrator, CallbackServerUpdate, ChangeFriendStatus,
        DataUpdate, FetchBdbServerUpdate, Highscore, LoginCode, LoginPassw, NewSyncQueue,
        OptionalValue, Player, PlayerQueue, PlayerRequest, PlayerVariableServerUpdate,
        RegistrationCode, SelfSync, ServerUpdateCallback, SyncEvent, SyncIter, SyncType,
        SyncUpdate, SyncVariableServerUpdate, Value, VariableUpdate,
    },
};
use age::{
    Decryptor, Encryptor,
    x25519::{Identity, Recipient},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{
    AsyncReadExt, AsyncWriteExt, SinkExt, Stream, StreamExt,
    stream::{SplitSink, SplitStream},
};
use integer_hasher::{IntMap, IntSet};
use machineid_crystal::{Encryption, HWIDComponent, IdBuilder};
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
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
#[cfg(feature = "__dev")]
use tracing::{info, warn};

/// A "bridge" between a server and the client.
///
/// First, you need to define (and connect to at any time)
/// ```rust
/// let mut cs = CrystalServer::init("your-game-id");
/// // Setup here things like game version, callbacks, etc...
/// cs.connect().await?; // Connecting requires a mutable reference
/// ```
/// Since you need to continuously call the cs.update() function,
/// you can do this on a separate async task, but you need to do so
/// after connecting to the server:
/// ```rust
/// let cs = Arc::new(cs);
/// {
///     let cs = cs.clone();
///     tokio::spawn(async {
///         loop {
///             cs.update().await?; // Update client data...
///             tokio::time::sleep(Duration::from_secs_f64(1.0 / 60.0)).await;
///         }
///     });
/// }
/// ```
/// Or in a single rust task, which is easier to handle:
/// ```rust
/// // On the end of a frame
/// cs.update().await?;
/// ```
/// <div class="warning">
/// You shouldn't update this faster than once in a frame, this may have unintended consequences
/// on the network usage of your game.
/// </div>
///
/// After making sure you're always triggering this function once in a frame (at most), you may do
/// anything else with the other functions.
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
type CallbackP2P = Box<dyn FnMut(Option<u64>, i16, Vec<Value>) + Sync + Send>;
type CallbackRegister = Box<dyn FnMut(RegistrationCode) + Sync + Send>;
type CallbackLogin = Box<dyn FnMut(LoginCode, Option<DateTime<Utc>>, Option<String>) + Sync + Send>;
type CallbackBanned = Box<dyn FnMut(String, DateTime<Utc>) + Sync + Send>;
type CallbackKicked = Box<dyn FnMut(String) + Sync + Send>;
type CallbackDisconnected = Box<dyn FnMut() + Sync + Send>;
type CallbackLoginToken = Box<dyn FnMut(String) + Sync + Send>;
type CallbackDataUpdate = Box<dyn FnMut(DataUpdate) + Sync + Send>;

#[derive(Default)]
struct StreamData {
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

    player_id: Option<u64>,
    player_name: Option<String>,
    player_save: HashMap<String, Value>,
    player_open_save: String,
    player_friends: IntSet<u64>,
    player_incoming_friends: IntSet<u64>,
    player_outgoing_friends: IntSet<u64>,

    game_save: HashMap<String, Value>,
    game_open_save: String,
    game_achievements: IntMap<Leb<u64>, Achievement>,
    game_highscores: IntMap<Leb<u64>, Highscore>,
    game_administrators: IntMap<Leb<u64>, Administrator>,
    game_version: f64,

    pub players: IntMap<u64, Player>,
    players_logout: IntSet<u64>,
    player_queue: IntMap<u64, PlayerQueue>,
    variables: HashMap<String, Value>,
    syncs: Vec<Option<SelfSync>>,
    syncs_remove: Vec<usize>,

    ping: f64,
    last_ping: Option<Instant>,

    new_sync_queue: Vec<NewSyncQueue>,
    update_variable: HashSet<String>,
    update_playerini: HashSet<String>,
    update_gameini: HashSet<String>,
    call_disconnected: bool,

    callback_server_update: IntMap<u64, Option<CallbackServerUpdate>>,
    callback_server_index: u64,

    handshake_completed: bool,

    registered_errors: Vec<ClientError>,
}

#[allow(dead_code)]
#[derive(Debug)]
enum ReaderError {
    StreamError(String),
    StreamEmpty(String),
    StreamClosed(String),
    Unknown(String),
}

#[allow(dead_code)]
#[derive(Debug)]
enum WriterError {
    StreamError(String),
    StreamClosed(String),
    Unknown(String),
}

#[allow(dead_code, clippy::enum_variant_names)]
#[derive(Debug)]
enum ClientError {
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
                    if !data.is_empty() {
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
                } else if frame.is_ping() {
                    Ok(Buffer::empty())
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
    pub async fn write_pong(&mut self) -> IoResult<()> {
        if let Some(stream) = self.stream.as_mut() {
            if stream.send(Message::Pong(Bytes::new())).await.is_err() {
                Err(Error::from(ErrorKind::BrokenPipe))
            } else {
                Ok(())
            }
        } else {
            Err(Error::from(ErrorKind::BrokenPipe))
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
        HashMap<String, Value>,
        IntSet<Leb<u64>>,
        IntSet<Leb<u64>>,
        IntSet<Leb<u64>>,
        IntMap<Leb<u64>, Achievement>,
    ),
    /// Player ID, Player Name, Player Variables, Player Syncs, Room
    PlayerLoggedIn(
        u64,
        String,
        HashMap<String, Value>,
        Vec<Option<types::Sync>>,
        String,
    ),
    /// Player ID
    PlayerLoggedOut(u64),
    /// Game Save, Game Achievements, Game Highscores, Game Administrators, Version
    SyncGameInfo(
        HashMap<String, Value>,
        IntMap<Leb<u64>, Achievement>,
        IntMap<Leb<u64>, Highscore>,
        IntMap<Leb<u64>, Administrator>,
        f64,
    ),
    /// Player ID or Server, Message ID, Data
    P2P(Option<Leb<u64>>, i16, Vec<Value>),
    /// Player ID, Variables
    UpdatePlayerVariable(u64, Vec<VariableUpdate>),
    /// Ping (ms)
    Ping(Option<f64>),
    ClearPlayers(),
    /// Variables
    GameIniWrite(Vec<VariableUpdate>),
    /// Player ID, Vec<Slot, Kind, Type, Variables>
    NewSync(u64, Vec<(u64, i16, SyncType, HashMap<String, Value>)>),
    /// Player ID, Room
    PlayerChangedRooms(u64, String),
    /// Player ID, Sync Variables
    UpdateSync(u64, Vec<SyncUpdate>),
    /// Player ID, Highscore ID, Score
    HighscoreUpdate(u64, u64, f64),
    /// Player ID, Player Syncs, Player Variables
    UpdatePlayerData(u64, Vec<Option<types::Sync>>, HashMap<String, Value>),
    /// Callback Index, Variable
    RequestPlayerVariable(u64, OptionalValue),
    // Admin Action
    AdminAction(AdminAction),
    /// Callback Index, Variable
    RequestSyncVariable(u64, OptionalValue),
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
#[doc(hidden)]
enum WritePacket {
    /// Hash, Lib Version, Device ID, Game ID, Game Version, Game Session
    InitializationHandshake([u64; 4], u64, String, String, f64, String),
    /// Username, Passw/Token, Game Token, Variables, Syncs, Room
    Login(
        String,
        LoginPassw,
        String,
        HashMap<String, Value>,
        Vec<Option<SelfSync>>,
        String,
    ),
    /// Username, Email, Passw, Repeat Passw
    Register(String, String, String, String),
    /// Player ID, Callback Index, Variable Name
    RequestPlayerVariable(PlayerRequest, u64, String),
    /// Player ID, Message ID, Payload
    P2P(PlayerRequest, i16, Vec<Value>),
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
    NewSync(Vec<(u64, i16, SyncType, HashMap<String, Value>)>),
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

    /// Try to establish a connection between the client and the server.
    pub async fn connect(&mut self) {
        #[cfg(feature = "__dev")]
        info!("Connecting to the server");
        {
            #[cfg(feature = "__dev")]
            info!("Fetching stream data");
            let mut lock = self.data.write().await;
            if let Some(thread) = &mut lock.thread {
                #[cfg(feature = "__dev")]
                info!("Aborting thread");
                if !thread.is_finished() {
                    thread.abort();
                }
            }
            lock.clear(true).await;
            lock.is_connecting = true;
            #[cfg(feature = "__dev")]
            info!("Stream data OK");
        }
        let url = if let Some(url) = self.data.read().await.last_host.clone() {
            url
        } else if cfg!(feature = "__local") {
            String::from("ws://localhost:16562")
        } else {
            String::from("ws://server.crystal-server.co:16562")
        };
        #[cfg(feature = "__dev")]
        info!("Connecting to server at {url}");
        match tokio_tungstenite::connect_async(url).await {
            Ok((ws, _)) => {
                #[cfg(feature = "__dev")]
                info!("Connection OK, initializing");
                let stream = StreamHandler::split_stream(ws).await;
                let writer = Arc::new(Mutex::new(stream.1));
                self.writer = Some(writer.clone());
                #[cfg(feature = "__dev")]
                info!("Stream setup OK");
                let thread =
                    tokio::spawn(Self::stream_handler(stream.0, writer, self.data.clone()));
                {
                    let mut lock = self.data.write().await;
                    lock.thread = Some(thread);
                    #[cfg(feature = "__dev")]
                    info!("Stream data thread OK");
                }
            }
            Err(_e) => {
                #[cfg(feature = "__dev")]
                info!("Connection error: {_e:?}");
                let mut dlock = self.data.write().await;
                dlock.clear(true).await;
                if dlock.call_disconnected {
                    if let Some(func) = dlock.func_disconnected.as_mut() {
                        func();
                    }
                    if let Some(dup) = dlock.func_data_update.as_mut() {
                        dup(DataUpdate::Disconnected());
                    }
                    dlock.call_disconnected = false;
                }
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

            #[cfg(feature = "__dev")]
            info!("Initialized stream handle task");

            loop {
                match reader.read().await {
                    Ok(mut buffer) => {
                        if buffer.is_empty()? {
                            continue;
                        }
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
                                        {
                                            write_packet!(WritePacket::Ping());
                                        }
                                        writer.lock().await.write_pong().await?;
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
                                            log(token.clone());
                                        }
                                        if let Some(dup) = &mut dlock.func_data_update {
                                            dup(DataUpdate::LoginToken(token));
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
                                    Self::iter_missing_data(&mut dlock, pid).await?;
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
                                    Self::iter_missing_data(&mut dlock, pid).await?;
                                    if let Some(player) = dlock.players.get_mut(&pid) {
                                        for upd in &upds {
                                            if let OptionalValue::Some(value) = upd.value.clone() {
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
                                    Self::iter_missing_data(&mut dlock, pid).await?;
                                    let mut exists = IntSet::default();
                                    let mut add_pq = IntSet::default();
                                    #[cfg(feature = "__dev")]
                                    info!("UpdateSync >>> {pid:?},{upds:?}");
                                    if let Some(player) = dlock.players.get_mut(&pid) {
                                        #[cfg(feature = "__dev")]
                                        info!("Got Player");
                                        for upd in &upds {
                                            #[cfg(feature = "__dev")]
                                            info!("Iterating over update {upd:?}");
                                            if let Some(Some(sync)) = player.syncs.get_mut(upd.slot)
                                            {
                                                #[cfg(feature = "__dev")]
                                                info!("Got sync {sync:?}");
                                                exists.insert(upd.slot);
                                                if let Some(vari) = &upd.variables {
                                                    #[cfg(feature = "__dev")]
                                                    info!("Got variable updates: {vari:?}");
                                                    for (vname, value) in vari {
                                                        #[cfg(feature = "__dev")]
                                                        info!(
                                                            "Itering over {vname:?} >>> {value:?}"
                                                        );
                                                        if let OptionalValue::Some(value) =
                                                            value.clone()
                                                        {
                                                            #[cfg(feature = "__dev")]
                                                            info!("Set");
                                                            sync.variables
                                                                .insert(vname.clone(), value);
                                                        } else {
                                                            #[cfg(feature = "__dev")]
                                                            info!("Removed");
                                                            sync.variables.remove(vname);
                                                        }
                                                    }
                                                } else if upd.remove_sync {
                                                    #[cfg(feature = "__dev")]
                                                    info!("Marked as sync is ending");
                                                    sync.is_ending = true;
                                                } else {
                                                    #[cfg(feature = "__dev")]
                                                    info!("No updates triggered");
                                                }
                                                #[cfg(feature = "__dev")]
                                                info!("Finished iterating over sync {sync:?}");
                                            } else {
                                                #[cfg(feature = "__dev")]
                                                info!("Added sync to PlayerQueue");
                                                add_pq.insert(pid);
                                            }
                                        }
                                    } else {
                                        #[cfg(feature = "__dev")]
                                        info!("Added to PlayerQueue");
                                        add_pq.insert(pid);
                                    }
                                    for slot in exists {
                                        if let Some(upd) =
                                            upds.iter().find(|supd| supd.slot == slot)
                                        {
                                            if let Some(dup) = &mut dlock.func_data_update {
                                                if let Some(vari) = &upd.variables {
                                                    for (vname, value) in vari {
                                                        dup(DataUpdate::UpdateSyncVariable(
                                                            pid,
                                                            slot,
                                                            vname.clone(),
                                                            value.clone(),
                                                        ));
                                                    }
                                                } else if upd.remove_sync {
                                                    dup(DataUpdate::UpdateSyncRemoval(pid, slot));
                                                }
                                            }
                                        }
                                    }
                                    for pid in add_pq {
                                        for upd in &upds {
                                            if let Some(player) = dlock.players.get(&pid) {
                                                if let Some(Some(_)) = player.syncs.get(upd.slot) {
                                                    continue;
                                                }
                                            }
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
                                        if let OptionalValue::Some(value) = upd.value.clone() {
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
                                            Self::iter_missing_data(&mut dlock, pid).await?;
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
                                    Self::iter_missing_data(&mut dlock, pid).await?;
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
                                    #[cfg(feature = "__dev")]
                                    info!("{pid:?}, {syncs:?}, {vari:?}");
                                    if dlock.players.contains_key(&pid) {
                                        #[cfg(feature = "__dev")]
                                        info!("OK");
                                        dlock.player_queue.remove(&pid);
                                        if let Some(player) = dlock.players.get_mut(&pid) {
                                            player.syncs = syncs;
                                            player.variables = vari;
                                            #[cfg(feature = "__dev")]
                                            info!("{player:?}");
                                        }
                                    }
                                    #[cfg(feature = "__dev")]
                                    info!("END");
                                }
                                ReadPacket::RequestPlayerVariable(index, vari) => {
                                    let mut dlock = data.write().await;
                                    #[cfg(feature = "__dev")]
                                    info!(
                                        "RequestPlayerVariable->{:?}->{index:?}",
                                        dlock.callback_server_update
                                    );
                                    if let Some(Some(csu)) = dlock
                                        .callback_server_update
                                        .get_mut(&index)
                                        .map(|csu| csu.take())
                                    {
                                        if let ServerUpdateCallback::PlayerVariable(callback, pid) =
                                            csu.callback
                                        {
                                            Self::iter_missing_data(&mut dlock, pid).await?;
                                            if let Some(player) = dlock.players.get_mut(&pid) {
                                                if let OptionalValue::Some(value) = vari.clone() {
                                                    player
                                                        .variables
                                                        .insert(csu.name.clone(), value);
                                                } else {
                                                    player.variables.remove(&*csu.name);
                                                }
                                            }
                                            if let Some(mut callback) = callback {
                                                callback(pid, csu.name, vari);
                                            }
                                        }
                                    }
                                }
                                ReadPacket::AdminAction(aa) => {
                                    let mut dlock = data.write().await;
                                    dlock.clear(false).await;
                                    dlock.is_loggedin = false;
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
                                    if let Some(Some(csu)) =
                                        dlock.callback_server_update.remove(&index)
                                    {
                                        if let ServerUpdateCallback::SyncVariable(
                                            callback,
                                            pid,
                                            slot,
                                        ) = csu.callback
                                        {
                                            let obtained = if let Some(player) =
                                                dlock.players.get_mut(&pid)
                                            {
                                                if let Some(Some(sync)) = player.syncs.get_mut(slot)
                                                {
                                                    if let OptionalValue::Some(value) = vari.clone()
                                                    {
                                                        sync.variables
                                                            .insert(csu.name.clone(), value);
                                                    } else {
                                                        sync.variables.remove(&*csu.name);
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
                                                        csu.name.clone(),
                                                        vari.clone(),
                                                    ));
                                                }
                                            }
                                            if let Some(mut callback) = callback {
                                                callback(pid, csu.name, vari);
                                            }
                                        }
                                    }
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
                                        if let OptionalValue::Some(value) = upd.value.clone() {
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
                                    if let Some(Some(mut csu)) = dlock
                                        .callback_server_update
                                        .get_mut(&index)
                                        .map(|csu| csu.take())
                                    {
                                        if let ServerUpdateCallback::FetchBdb(Some(callback)) =
                                            &mut csu.callback
                                        {
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
        let mut dlock;
        if let Err(_e) = task.await {
            dlock = cdata.write().await;
            #[cfg(feature = "__dev")]
            info!("Stream handler closed with error: {_e:?}");
            dlock.registered_errors.push(if _e.is_panic() {
                ClientError::HandlerPanic(format!("{:?}", _e.into_panic()))
            } else {
                ClientError::HandlerResult(Ok(())) // TODO: Change this into the actual error
            });
        } else {
            dlock = cdata.write().await;
            dlock
                .registered_errors
                .push(ClientError::HandlerResult(Ok(())));
        }
        {
            dlock.clear(true).await;
            if dlock.call_disconnected {
                if let Some(func) = dlock.func_disconnected.as_mut() {
                    func();
                }
                if let Some(dup) = dlock.func_data_update.as_mut() {
                    dup(DataUpdate::Disconnected());
                }
                dlock.call_disconnected = false;
            }
        }
        cwriter.lock().await.shutdown().await;
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

    #[inline(always)]
    async fn iter_missing_data(data: &mut StreamData, pid: u64) -> IoResult<()> {
        if let Some(pq) = data.player_queue.get_mut(&pid) {
            if let Some(player) = data.players.get_mut(&pid) {
                for (name, value) in pq.variables.drain() {
                    if let OptionalValue::Some(value) = value {
                        player.variables.insert(name, value);
                    } else {
                        player.variables.remove(&name);
                    }
                }
                for (index, osync) in player.syncs.iter_mut().enumerate() {
                    if osync.is_none() {
                        if let Some((sni, sn)) = pq
                            .new_syncs
                            .iter()
                            .enumerate()
                            .find(|(_, sn)| sn.slot == index)
                        {
                            *osync = Some(types::Sync {
                                event: SyncEvent::New,
                                kind: sn.kind,
                                sync_type: sn.sync_type,
                                variables: sn.variables.clone(),
                                is_ending: false,
                            });
                            pq.new_syncs.remove(sni);
                        }
                    }
                    if let Some(sync) = osync {
                        if let Some((sni, _)) = pq
                            .new_syncs
                            .iter()
                            .enumerate()
                            .find(|(_, sn)| sn.slot == index)
                        {
                            pq.new_syncs.remove(sni);
                        }
                        if let Some(is) = pq.syncs.remove(&index) {
                            for (name, value) in is {
                                if let OptionalValue::Some(value) = value {
                                    sync.variables.insert(name, value);
                                } else {
                                    sync.variables.remove(&name);
                                }
                            }
                        }
                        if let Some((index, _)) = pq
                            .remove_syncs
                            .iter()
                            .enumerate()
                            .find(|(rindex, _)| *rindex == index)
                        {
                            sync.is_ending = true;
                            pq.remove_syncs.remove(index);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Update the Crystal Server runtime.
    /// This ensures that things are kept up-to-date between the server
    /// and the client.
    #[inline(always)]
    pub async fn update(&self) -> IoResult<()> {
        let conn = self.is_connected().await;
        let mut dlock = self.data.write().await;
        if let Some(room_callback) = &mut dlock.func_room {
            let result = room_callback();
            if dlock.room != result && conn {
                self.internal_iosend(Self::get_packet_write(&WritePacket::UpdateRoom(
                    result.clone(),
                ))?)
                .await?;
            }
            dlock.room = result;
        }
        if conn {
            let lroom = dlock.room.clone();
            //let timestamp = Utc::now().timestamp();
            for pid in dlock.players.keys().cloned().collect::<Vec<u64>>() {
                let player_logged_out = dlock.players_logout.contains(&pid);
                let mut is_all_none = true;
                Self::iter_missing_data(&mut dlock, pid).await?;
                let player = dlock
                    .players
                    .get_mut(&pid)
                    .ok_or(Error::new(ErrorKind::NotFound, "player not found"))?;
                for sync in &mut player.syncs {
                    if let Some(msync) = sync {
                        /*#[cfg(feature = "__dev")]
                        info!("Got sync {msync:?} for pid {pid}");*/
                        is_all_none = false;
                        if msync.event == SyncEvent::End {
                            #[cfg(feature = "__dev")]
                            info!("Sync END {pid}");
                            drop(sync.take());
                            continue;
                        }
                        if msync.sync_type == SyncType::Once
                            || player.room != lroom
                            || player_logged_out
                        {
                            #[cfg(feature = "__dev")]
                            info!("Marked as Sync END {pid}");
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
            if !dlock.syncs.is_empty() || !dlock.syncs_remove.is_empty() {
                let mut upds = Vec::new();
                for (index, sync) in dlock.syncs.iter_mut().enumerate() {
                    if let Some(sync) = sync {
                        if !sync.to_sync.is_empty() {
                            #[cfg(feature = "__dev")]
                            info!(
                                "updating sync variables on slot {index}: {:?}",
                                sync.to_sync
                            );
                            let mut variupd = HashMap::new();
                            for upd in sync.to_sync.drain() {
                                let vari = sync.variables.get(&upd).cloned().into();
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
                for remove in dlock.syncs_remove.drain(..) {
                    upds.push(SyncUpdate {
                        slot: remove,
                        remove_sync: true,
                        variables: None,
                    });
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
                let variupds = dlock.update_variable.drain().collect::<Vec<String>>();
                for name in variupds {
                    let value = dlock.variables.get(&name).cloned().into();
                    data.push(VariableUpdate { name, value });
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::UpdatePlayerVariable(
                    data,
                ))?)
                .await?;
            }
            if !dlock.update_playerini.is_empty() {
                let mut data: Vec<VariableUpdate> = Vec::new();
                let variupds = dlock.update_playerini.drain().collect::<Vec<String>>();
                for name in variupds {
                    let value = dlock.player_save.get(&name).cloned().into();
                    data.push(VariableUpdate { name, value });
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::PlayerIniWrite(data))?)
                    .await?;
            }
            if !dlock.update_gameini.is_empty() {
                let mut data: Vec<VariableUpdate> = Vec::new();
                let variupds = dlock.update_gameini.drain().collect::<Vec<String>>();
                for name in variupds {
                    let value = dlock.game_save.get(&name).cloned().into();
                    data.push(VariableUpdate { name, value });
                }
                self.internal_iosend(Self::get_packet_write(&WritePacket::GameIniWrite(data))?)
                    .await?;
            }
            if let Some(ping) = dlock.last_ping {
                if ping.elapsed().as_secs_f64() >= 90.0 {
                    drop(dlock);
                    self.disconnect().await;
                    if let Some(dup) = self.data.write().await.func_data_update.as_mut() {
                        dup(DataUpdate::Disconnected());
                    }
                }
            }
        } else {
            if dlock.is_loggedin {
                dlock.is_loggedin = false;
            }
            dlock.last_ping.take();
        }
        Ok(())
    }

    /// This is where you tell the other clients in which *room* you are
    /// The function receives 0 arguments and must return a [String]
    /// ```rust
    /// static CURRENT_ROOM: LazyLock<Mutex<String>> = LazyLock::new(|| Mutex::new(String::from("kitchen")));
    ///
    /// /// ...now I am in the living room
    /// *CURRENT_ROOM.lock().unwrap() = String::from("living room");
    /// ```
    /// And you'd tell that to the server through this function:
    /// ```rust
    /// cs.callback_set_room(Box::new(|| CURRENT_ROOM.lock().unwrap().clone())).await;
    /// ```
    pub async fn callback_set_room(&self, callback: CallbackRoom) {
        self.data.write().await.func_room = Some(callback);
    }

    /// This is the callback when the client receives data from the server or another player.
    /// The function receives 3 arguments: Player ID: [Option<u64>], Message ID: [i64], Payload/Data: [Vec<Value>], and must return nothing.
    /// ```rust
    /// cs.callback_set_p2p(|player_id, message_id, payload| {
    ///     // Your code goes here to handle the event.
    ///     // For example:
    ///     const P2P_HELLOWORLD: i16 = 0;
    ///     if message_id == P2P_HELLOWORLD {
    ///         if let Some(player_id) = player_id {
    ///             println!("Hello World, {}!", cs.get_other_player(player_id).unwrap().name);
    ///         }
    ///     }
    /// });
    /// ```
    pub async fn callback_set_p2p(&self, callback: CallbackP2P) {
        self.data.write().await.func_p2p = Some(callback);
    }

    /// This is the callback when a registration event has been carried out.
    /// The function receives 1 argument: Result: [RegistrationCode], and must return nothing.
    pub async fn callback_set_register(&self, callback: CallbackRegister) {
        self.data.write().await.func_register = Some(callback);
    }

    /// This is the callback when a login event has been carried out.
    /// The function receives 1 argument: Result: [LoginCode], and must return nothing.
    pub async fn callback_set_login(&self, callback: CallbackLogin) {
        self.data.write().await.func_login = Some(callback);
    }

    /// This is the callback when the player has been banned while playing.
    /// The function receives 2 arguments: Reason: [String], Unban Time: [DateTime<Utc>], and must return nothing.
    pub async fn callback_set_banned(&self, callback: CallbackBanned) {
        self.data.write().await.func_banned = Some(callback);
    }

    /// This is the callback when the player has been kicked while playing.
    /// The function receives 2 arguments: Reason: [String], and must return nothing.
    pub async fn callback_set_kicked(&self, callback: CallbackKicked) {
        self.data.write().await.func_kicked = Some(callback);
    }

    /// This is the callback when the client has disconnected from the server.
    /// The function receives 0 arguments and must return nothing.
    pub async fn callback_set_disconnected(&self, callback: CallbackDisconnected) {
        self.data.write().await.func_disconnected = Some(callback);
    }

    /// This is the callback when the client has disconnected from the server.
    /// The function receives 0 arguments and must return nothing.
    ///
    /// The login-token is only valid once and only works for the game it was
    /// generated on.
    pub async fn callback_set_login_token(&self, callback: CallbackLoginToken) {
        self.data.write().await.func_login_token = Some(callback);
    }

    /// This is the callback when an event has been triggered by the server.
    /// This may work as a replacement for all of the other callback functions
    /// except [CrystalServer::callback_set_room].
    /// The function receives 1 argument, Data: [DataUpdate] and must return nothing.
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

    /// Check if the client is currently logged in.
    pub async fn is_loggedin(&self) -> bool {
        self.is_connected().await && {
            let dlock = self.data.read().await;
            dlock.is_loggedin && dlock.handshake_completed
        }
    }

    /// Obtain the ping between the client and the server in ms.
    pub async fn get_ping(&self) -> f64 {
        self.data.read().await.ping
    }

    /// Sets the Game Token so that the client may be able to log in.
    ///
    /// The Game Token is like a "password" for your game and that
    /// the game the player is playing on must be the same one as the
    /// one in the server.
    pub async fn set_game_token(&self, token: &str) {
        self.data.write().await.game_token = token.to_owned();
    }

    /// Disconnects the client from an active connection with the server.
    pub async fn disconnect(&self) {
        let mut lock = self.data.write().await;
        if let Some(thread) = lock.thread.take() {
            thread.abort();
        }
        lock.clear(true).await;
    }

    async fn internal_iosend(&self, data: Buffer) -> IoResult<()> {
        if let Some(writer) = &self.writer {
            if let Err(_e) = writer.lock().await.write(data).await {
                #[cfg(feature = "__dev")]
                info!("unable to send data to server with error: {_e:?}");
                let mut dlock = self.data.write().await;
                dlock.clear(true).await;
                if dlock.call_disconnected {
                    if let Some(func) = dlock.func_disconnected.as_mut() {
                        func();
                    }
                    if let Some(dup) = dlock.func_data_update.as_mut() {
                        dup(DataUpdate::Disconnected());
                    }
                    dlock.call_disconnected = false;
                }
            }
        } else {
            #[cfg(feature = "__dev")]
            info!("stream is not open for writing");
        }
        Ok(())
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

    /// Try to log into an existing account.
    ///
    /// The login result will be sent as a callback event.
    /// Use [CrystalServer::callback_set_login] and [CrystalServer::callback_set_login_token]
    /// respectively to obtain data from the login attempt.
    pub async fn login(&self, username: &str, passw: &str) -> IoResult<()> {
        self.internal_login(username, LoginPassw::Passw(passw.to_owned()))
            .await
    }

    /// Try to log into an existing account using a login-token.
    ///
    /// The login result will be sent as a callback event.
    /// Use [CrystalServer::callback_set_login] and [CrystalServer::callback_set_login_token]
    /// respectively to obtain data from the login attempt.
    pub async fn login_with_token(&self, username: &str, token: &str) -> IoResult<()> {
        self.internal_login(username, LoginPassw::Token(token.to_owned()))
            .await
    }

    /// Try to register a new account.
    ///
    /// The register result will be sent as a callback event.
    /// Use [CrystalServer::callback_set_login] to obtain data from the registration attempt.
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

    /// Obtain the current Player ID.
    /// Note that this will only return something if the player is logged in.
    pub async fn get_player_id(&self) -> Option<u64> {
        self.data.read().await.player_id
    }

    /// Obtain the current Player Name.
    /// Note that this will only return something if the player is logged in.
    pub async fn get_player_name(&self) -> Option<String> {
        self.data.read().await.player_name.clone()
    }

    /// Sets a variable with the name and value provided.
    pub async fn set_variable(&self, name: &str, value: Value) {
        let mut dlock = self.data.write().await;
        if let Some(orgvalue) = dlock.variables.get(name) {
            if value == *orgvalue {
                return;
            }
        }
        dlock.variables.insert(name.to_owned(), value);
        dlock.update_variable.insert(name.to_owned());
    }

    /// Removes a variable with the provided name.
    pub async fn remove_variable(&self, name: &str) {
        let mut dlock = self.data.write().await;
        if dlock.variables.remove(name).is_some() {
            dlock.update_variable.insert(name.to_owned());
        }
    }

    /// An iterator to obtain the other connected players data. (data such as syncs, variables, etc.)
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

    /// Gets the current amount of other players.
    pub async fn other_player_count(&self) -> usize {
        self.data.read().await.players.len()
    }

    /// Gets the data of a player from the Player ID if the player is connected.
    /// If the player is not found, it returns [None].
    pub async fn get_other_player(&self, pid: u64) -> Option<Player> {
        self.data.read().await.players.get(&pid).cloned()
    }

    /// Gets the data of a player from the Player Name if the player is connected.
    /// /// If the player is not found, it returns [None].
    pub async fn get_other_player_name(&self, name: &str) -> Option<(u64, Player)> {
        let name = name.to_lowercase();
        self.data
            .read()
            .await
            .players
            .iter()
            .find(|(_pid, player)| player.name.to_lowercase() == name)
            .map(|(pid, player)| (*pid, player.clone()))
    }

    /// Force an update of a variable, note that this will only be useful if the
    /// player is not in the same room as you.
    ///
    /// The result will be saved as normal and will be sent as a callback event.
    /// Use [CrystalServer::callback_set_data_update] or the `callback` variable to fetch it.
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
                .find(|(_, csu)| csu.is_none())
            {
                *csu = Some(CallbackServerUpdate {
                    name: name.to_owned(),
                    callback: ServerUpdateCallback::PlayerVariable(callback, pid),
                });
                *index
            } else {
                let index = dlock.callback_server_index;
                dlock.callback_server_update.insert(
                    index,
                    Some(CallbackServerUpdate {
                        name: name.to_owned(),
                        callback: ServerUpdateCallback::PlayerVariable(callback, pid),
                    }),
                );
                dlock.callback_server_index += 1;
                index
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

    /// Sends a message "peer-to-peer" to the requested target.
    /// The Message ID will allow you to quickly differentiate what message it's
    /// supposed to be.
    pub async fn p2p(
        &self,
        target: PlayerRequest,
        message_id: i16,
        payload: Vec<Value>,
    ) -> IoResult<()> {
        self.internal_iosend(Self::get_packet_write(&WritePacket::P2P(
            target, message_id, payload,
        ))?)
        .await
    }

    /// Sets the current game version.
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

    /// Opens a playerini file.
    pub async fn open_playerini(&self, file: &str) {
        self.data.write().await.player_open_save = file.to_owned();
    }

    /// Closes the currently open playerini file.
    pub async fn close_playerini(&self) {
        self.data.write().await.player_open_save.clear();
    }

    /// Checks if the requested section & key exists in the currently open playerini file.
    pub async fn has_playerini(&self, section: &str, key: &str) -> bool {
        let dlock = self.data.read().await;
        dlock
            .player_save
            .contains_key(&Self::get_save_key(&dlock.player_open_save, section, key))
    }

    /// Returns the saved value in the section & key of the currently open playerini file.
    /// If the value is not found, it returns [None].
    pub async fn get_playerini(&self, section: &str, key: &str) -> Option<Value> {
        let dlock = self.data.read().await;
        dlock
            .player_save
            .get(&Self::get_save_key(&dlock.player_open_save, section, key))
            .cloned()
    }

    /// Saves a new value for the requested section & key of the currently open playerini file.
    pub async fn set_playerini(&self, section: &str, key: &str, value: Value) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.player_open_save, section, key);
        if let Some(orgvalue) = dlock.player_save.get(&save_key) {
            if value == *orgvalue {
                return;
            }
        }
        dlock.player_save.insert(save_key.clone(), value.clone());
        dlock.update_playerini.insert(save_key);
    }

    /// Removes the saved value for the requested section & key of the currently open playerini file.
    pub async fn remove_playerini(&self, section: &str, key: &str) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.player_open_save, section, key);
        if dlock.player_save.remove(&save_key).is_some() {
            dlock.update_playerini.insert(save_key);
        }
    }

    /// Gets the currently open gameini file.
    /// The result will be empty is no files are open.
    pub async fn get_open_gameini(&self) -> String {
        self.data.read().await.game_open_save.clone()
    }

    /// Opens a gameini file.
    pub async fn open_gameini(&self, file: &str) {
        self.data.write().await.game_open_save = file.to_owned();
    }

    /// Closes the currently open gameini file.
    pub async fn close_gameini(&self) {
        self.data.write().await.game_open_save.clear();
    }

    /// Checks if the requested section & key exists in the currently open gameini file.
    pub async fn has_gameini(&self, section: &str, key: &str) -> bool {
        let dlock = self.data.read().await;
        dlock
            .game_save
            .contains_key(&Self::get_save_key(&dlock.game_open_save, section, key))
    }

    /// Returns the saved value in the section & key of the currently open gameini file.
    /// If the value is not found, it returns [None].
    pub async fn get_gameini(&self, section: &str, key: &str) -> Option<Value> {
        let dlock = self.data.read().await;
        dlock
            .game_save
            .get(&Self::get_save_key(&dlock.game_open_save, section, key))
            .cloned()
    }

    /// Saves a new value for the requested section & key of the currently open gameini file.
    pub async fn set_gameini(&self, section: &str, key: &str, value: Value) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.game_open_save, section, key);
        if let Some(orgvalue) = dlock.game_save.get(&save_key) {
            if value == *orgvalue {
                return;
            }
        }
        dlock.game_save.insert(save_key.clone(), value.clone());
        dlock.update_gameini.insert(save_key);
    }

    /// Removes the saved value for the requested section & key of the currently open gameini file.
    pub async fn remove_gameini(&self, section: &str, key: &str) {
        let mut dlock = self.data.write().await;
        let save_key = Self::get_save_key(&dlock.game_open_save, section, key);
        if dlock.game_save.remove(&save_key).is_some() {
            dlock.update_gameini.insert(save_key);
        }
    }

    /// Checks if an achievement exists.
    pub async fn has_achievement(&self, aid: u64) -> bool {
        self.data
            .read()
            .await
            .game_achievements
            .contains_key(&Leb(aid))
    }

    /// Gets an achievement data.
    /// If it doesn't exist it returns [None].
    pub async fn get_achievement(&self, aid: u64) -> Option<Achievement> {
        self.data
            .read()
            .await
            .game_achievements
            .get(&Leb(aid))
            .cloned()
    }

    /// Checks if the player has reached an achievement.
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

    /// Get the timestamp (on Unix UTC) of when the achievement
    /// was unlocked.
    /// If the player hasn't unlocked the achievement yet, it returns [None].
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

    /// Make the current player reach an achievement.
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

    /// Checks if a highscore exists.
    pub async fn has_highscore(&self, hid: u64) -> bool {
        self.data
            .read()
            .await
            .game_achievements
            .contains_key(&Leb(hid))
    }

    /// Gets a highscore data.
    /// If the highscore doesn't exist, it returns [None].
    pub async fn get_highscore(&self, hid: u64) -> Option<Highscore> {
        self.data
            .read()
            .await
            .game_highscores
            .get(&Leb(hid))
            .cloned()
    }

    /// Checks if the player has a score on a highscore.
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

    /// Gets the last recorded score from the player on this highscore.
    /// If the highscore doesn't exist, it returns [None].
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

    /// Records a new player score on the specified highscore.
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

    /// Creates a new sync.
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

    /// Destroys a created sync.
    pub async fn destroy_sync(&self, sync: usize) {
        let mut dlock = self.data.write().await;
        if let Some(Some(_)) = dlock.syncs.get(sync) {
            dlock.syncs_remove.push(sync);
        }
    }

    /// Set a sync variable with the specified name and value.
    pub async fn set_variable_sync(&self, sync: usize, name: &str, value: Value) {
        let mut dlock = self.data.write().await;
        if let Some(Some(sync)) = dlock.syncs.get_mut(sync) {
            if let Some(orgvalue) = sync.variables.get(name) {
                if value == *orgvalue {
                    return;
                }
            }
            sync.variables.insert(name.to_owned(), value);
            sync.to_sync.insert(name.to_owned());
        }
    }

    /// Remove a sync variable with the specified name.
    pub async fn remove_variable_sync(&self, sync: usize, name: &str) {
        let mut dlock = self.data.write().await;
        if let Some(Some(sync)) = dlock.syncs.get_mut(sync) {
            if sync.variables.remove(name).is_some() {
                sync.to_sync.insert(name.to_owned());
            }
        }
    }

    /// Gets a sync variable from another player with the specified Sync ID and name.
    pub async fn get_variable_other_sync(
        &self,
        pid: u64,
        sync: usize,
        name: &str,
    ) -> Option<Value> {
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

    /// Checks if a sync variable exists of another player with the specified Sync ID and name.
    /// If the player or sync is not found, it returns [None].
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

    /// An iterator to obtain all the other players' sync data. (data such as variables, current event, etc.)
    pub async fn iter_other_syncs(&self) -> impl Stream<Item = SyncIter> {
        let data = self.data.clone();

        Box::pin(async_stream::stream! {
            let pids = data.read().await.players.keys().cloned().collect::<Vec<u64>>();
            for id in pids {
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

    /// Checks if a player is an administrator.
    pub async fn is_player_admin(&self, pid: u64) -> bool {
        self.data
            .read()
            .await
            .game_administrators
            .contains_key(&Leb(pid))
    }

    /// Obtains the administrator data of a player.
    /// If the player is not an administrator, it returns [None].
    pub async fn get_player_admin(&self, pid: u64) -> Option<Administrator> {
        self.data
            .read()
            .await
            .game_administrators
            .get(&Leb(pid))
            .cloned()
    }

    /// Kicks the specified player from the game.
    /// If the player doesn't have permission to kick the player,
    /// the function will return [false] and nothing will happen.
    /// You can kick yourself with this function without any checks.
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

    /// Bans the specified player from the game.
    /// If the player doesn't have permission to ban the player,
    /// the function will return [false] and nothing will happen.
    /// You can ban yourself with this function without any checks.
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

    /// Unbans the specified player from the game.
    /// If the player doesn't have permission to unban the player,
    /// the function will return [false] and nothing will happen.
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

    /// Logs out from the account currently playing in.
    pub async fn logout(&self) -> IoResult<bool> {
        if self.is_loggedin().await {
            self.internal_iosend(Self::get_packet_write(&WritePacket::Logout())?)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Force an update of a variable from a sync, note that this will only be useful if the
    /// player is not in the same room as you.
    ///
    /// The result will be saved as normal and will be sent as a callback event.
    /// Use [CrystalServer::callback_set_data_update] or the `callback` variable to fetch it.
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
                    .find(|(_, csu)| csu.is_none())
                {
                    *csu = Some(CallbackServerUpdate {
                        name: name.to_owned(),
                        callback: ServerUpdateCallback::SyncVariable(callback, pid, slot),
                    });
                    *index
                } else {
                    let index = dlock.callback_server_index;
                    dlock.callback_server_update.insert(
                        index,
                        Some(CallbackServerUpdate {
                            name: name.to_owned(),
                            callback: ServerUpdateCallback::SyncVariable(callback, pid, slot),
                        }),
                    );
                    dlock.callback_server_index += 1;
                    index
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

    /// Get the data of a Binary Data Block.
    ///
    /// The result will be saved as normal and will be sent as a callback event.
    /// Use [CrystalServer::callback_set_data_update] or the `callback` variable to fetch it.
    pub async fn fetch_bdb(
        &self,
        name: &str,
        callback: Option<FetchBdbServerUpdate>,
    ) -> IoResult<()> {
        let mut dlock = self.data.write().await;
        let index = if let Some((index, csu)) = dlock
            .callback_server_update
            .iter_mut()
            .find(|(_, csu)| csu.is_none())
        {
            *csu = Some(CallbackServerUpdate {
                name: name.to_owned(),
                callback: ServerUpdateCallback::FetchBdb(callback),
            });
            *index
        } else {
            let index = dlock.callback_server_index;
            dlock.callback_server_update.insert(
                index,
                Some(CallbackServerUpdate {
                    name: name.to_owned(),
                    callback: ServerUpdateCallback::FetchBdb(callback),
                }),
            );
            dlock.callback_server_index += 1;
            index
        };
        self.internal_iosend(Self::get_packet_write(&WritePacket::RequestBdb(
            index as u64,
            name.to_owned(),
        ))?)
        .await?;
        Ok(())
    }

    /// Update the data of a Binary Data Block.
    pub async fn set_bdb(&self, name: &str, data: Vec<u8>) -> IoResult<()> {
        self.internal_iosend(Self::get_packet_write(&WritePacket::SetBdb(
            name.to_owned(),
            data,
        ))?)
        .await
    }

    /// Obtain the player's incoming friend requests.
    /// If the player is not logged in, it returns [None].
    pub async fn get_incoming_friends(&self) -> Option<IntSet<u64>> {
        if self.is_loggedin().await {
            Some(self.data.read().await.player_incoming_friends.clone())
        } else {
            None
        }
    }

    /// Obtain the player's outgoing friend requests.
    /// If the player is not logged in, it returns [None].
    pub async fn get_outgoing_friends(&self) -> Option<IntSet<u64>> {
        if self.is_loggedin().await {
            Some(self.data.read().await.player_outgoing_friends.clone())
        } else {
            None
        }
    }

    /// Obtain the player's current friends.
    /// If the player is not logged in, it returns [None].
    pub async fn get_friends(&self) -> Option<IntSet<u64>> {
        if self.is_loggedin().await {
            Some(self.data.read().await.player_friends.clone())
        } else {
            None
        }
    }

    /// Send a friend request to a player.
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

    /// Cancel the sent friend request of a player.
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

    /// Deny the received friend request of a player.
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

    /// Accept the received the friend request of a player.
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

    /// Remove a friend from the player's friend list.
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
