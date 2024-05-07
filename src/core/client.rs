use std::{io::Result as IoResult, sync::Arc};
use chrono::{DateTime, TimeZone, Utc};
use integer_hasher::IntMap;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use serde_json::{Number, Value};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream}, runtime::{Builder, Runtime}, sync::Mutex, task::JoinHandle};
use super::buffer::Buffer;
use tracing::info;

type TStreamReader = Arc<Mutex<StreamReader>>;
type TStreamWriter = Arc<Mutex<StreamWriter>>;
type TStreamData = Arc<Mutex<StreamData>>;

pub struct CrystalServer {
    stream: Option<(TStreamReader, TStreamWriter)>,
    data: TStreamData,
    runtime: Runtime,
    thread: JoinHandle<IoResult<()>>,
}

pub struct StreamHandler;

pub struct StreamReader {
    stream: Option<OwnedReadHalf>,
}

pub struct StreamWriter {
    stream: Option<OwnedWriteHalf>,
}

type ScriptRegister = dyn FnMut(RegistrationCode) 
                        + Sync + Send;
type ScriptLogin = dyn FnMut(LoginCode, Option<DateTime<Utc>>, Option<String>)
                        + Sync + Send;
type ScriptP2p = dyn FnMut(i16, u64, Vec<Value>)
                        + Sync + Send;

#[derive(Default)]
pub struct StreamData {
    script_register: Option<Box<ScriptRegister>>,
    script_login: Option<Box<ScriptLogin>>,
    script_p2p: Option<Box<ScriptP2p>>,
    is_loggedin: bool,
    players: IntMap<u64, Player>,
    player_id: Option<u64>,
    player_name: Option<String>,
    player_save: Value,
    game_save: Value,
    game_achievements: Value,
    game_highscores: Value,
    game_administrators: Value,
    game_version: f64,
}

#[derive(Default)]
pub struct Player {
    id: u64,
    name: String,
    room: String,
    variables: Value,
    instances: Vec<Instance>,
}

#[derive(Default)]
pub struct Instance {
    event_type: SyncEvent,
    sync_type: u8,
    kind: i16,
    variables: Value,
    local_variables: Value,
}

#[derive(FromPrimitive, ToPrimitive, Default)]
pub enum SyncEvent {
    #[default]
    New = 0,
    Step = 1,
    End = 2,
    Once = 3,
}

#[derive(FromPrimitive, ToPrimitive, Default)]
pub enum CreateSync {
    #[default]
    Once = 0,
    Semi = 1,
    Full = 2,
}

#[derive(Debug)]
pub enum ReaderError {
    StreamError,
    StreamEmpty,
    StreamClosed,
    Unknown,
}

#[derive(Debug)]
pub enum WriterError {
    StreamError,
    StreamClosed,
    Unknown,
}

#[derive(Copy, Clone, Debug, FromPrimitive, ToPrimitive, PartialEq)]
pub enum RegistrationCode {
    OK = 0,
    AccountExists = 1,
    UsedEmail = 2,
    InvalidEmail = 3,
    ShortPassword = 4,
    InvalidName = 5,
    ShortName = 6,
    DifferentPasswords = 7,
    Error = 8,
    LongName = 9,
    GlobalBan = 10,
    LongPassword = 11,
}

#[derive(Copy, Clone, Debug, FromPrimitive, ToPrimitive, PartialEq)]
pub enum LoginCode {
    OK = 0,
    NoUser = 1,
    WrongPassword = 2,
    Unauthenticated = 3,
    UsedClient = 4,
    AlreadyIn = 5,
    GameBan = 6,
    GlobalBan = 7,
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(i64)]
pub enum P2pCode {
    AllGame = -5,
    CurrentSession = -6,
    CurrentRoom = -7,
    PlayerId(u64) = -8,
}

macro_rules! xor {
    ($bytes: expr) => {{
        let mut key = [0x76, 0xf2, 0xab, 0x8b];
        let mut i = 0;
        let len = $bytes.len();
        $bytes.iter_mut().take(len.min(2048)).for_each(|x| {
            let y = key[i];
            *x ^= y;
            key[i] = key[i].wrapping_add(y).wrapping_add(0xf9 ^ (i as u8));
            i = (i + 1) % 4;
        });
    }};
}

macro_rules! prepare_buffer {
    ($data: expr) => {{
        xor!($data.data);
        for byte in ($data.data.len() as u32).to_be_bytes() {
            $data.data.insert(0, byte);
        }
    }};
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

impl StreamHandler {
    pub async fn split_stream(stream: TcpStream) -> (StreamReader, StreamWriter) {
        let split = stream.into_split();
        (StreamReader {
            stream: Some(split.0),
        },
        StreamWriter {
            stream: Some(split.1),
        })
    }
}

impl StreamReader {
    pub async fn read(&mut self, xor: bool) -> Result<Buffer, ReaderError> {
        if let Some(stream) = self.stream.as_mut() {
            unwrap_return!(stream.readable().await, Err(ReaderError::StreamError));
            let mut buf_size = [0u8; 4];
            unwrap_return!(
                stream.read_exact(&mut buf_size).await,
                Err(ReaderError::StreamError)
            );
            let size = u32::from_le_bytes(buf_size);
            info!("Packet Size: {size}");
            if size > 0 {
                let mut buf = vec![0; size as usize];
                unwrap_return!(
                    stream.read_exact(&mut buf).await,
                    Err(ReaderError::StreamError)
                );
                if xor {
                    xor!(buf);
                }
                Ok(Buffer::new_with_data(buf).await)
            } else {
                Err(ReaderError::StreamEmpty)
            }
        } else {
            Err(ReaderError::StreamClosed)
        }
    }
}

impl StreamWriter {
    pub async fn write(&mut self, data: impl Into<Buffer>) -> Result<(), WriterError> {
        if let Some(stream) = self.stream.as_mut() {
            let mut data = data.into();
            unwrap_return!(stream.writable().await, Err(WriterError::StreamError));
            prepare_buffer!(data);
            unwrap_return!(stream.write_all(&data.data).await, Err(WriterError::StreamError));
            Ok(())

        } else {
            Err(WriterError::StreamClosed)
        }
    }
}

impl CrystalServer {
    pub fn init() -> Self {
        let runtime = Builder::new_current_thread()
            .build().unwrap();
        let thread = runtime.spawn(async { Ok(()) });
        Self {
            stream: None,
            data: Arc::new(Mutex::new(StreamData::default())),
            runtime,
            thread,
        }
    }

    pub async fn connect(&mut self) {
        if !self.thread.is_finished() {
            self.thread.abort();
        }
        if let Ok(stream) = TcpStream::connect("127.0.0.1:16561").await {
            let stream = StreamHandler::split_stream(stream).await;
            let reader = Arc::new(Mutex::new(stream.0));
            let writer = Arc::new(Mutex::new(stream.1));
            self.stream = Some((reader.clone(), writer.clone()));
            let h = Self::stream_handler(reader, writer, self.data.clone());
            let t = self.runtime.spawn(h);
            self.thread = t;
        }
    }

    async fn stream_handler(reader: TStreamReader, writer: TStreamWriter, tdata: TStreamData) -> IoResult<()> {
        loop {
            match reader.lock().await.read(true).await {
                Ok(mut data) => {
                    let read_result: IoResult<()> = {
                        match data.read::<u8>().await? {
                            0 => {
                                // Registration
                                let code = RegistrationCode::from_u8(data.read::<u8>().await?).unwrap();
                                let mut lock = tdata.lock().await;
                                if let Some(sr) = lock.script_register.as_mut() {
                                    sr(code);
                                }
                            }
                            1 => {
                                // Login
                                let code = LoginCode::from_u8(data.read::<u8>().await?).unwrap();
                                let mut lock = tdata.lock().await;
                                match code {
                                    LoginCode::OK => {
                                        lock.is_loggedin = true;
                                        lock.player_id = Some(data.read::<u64>().await?);
                                        lock.player_name = Some(data.read::<String>().await?);
                                        lock.player_save = serde_json::from_str(&data.read::<String>().await?)?;
                                    }
                                    LoginCode::GameBan => {
                                        let unban_time = data.read::<u64>().await?;
                                        let reason = data.read::<String>().await?;
                                        if let Some(sl) = lock.script_login.as_mut() {
                                            sl(code, Some(Utc.timestamp_opt(unban_time as i64, 0).unwrap().to_utc()), Some(reason));
                                        }
                                    }
                                    _ => {}
                                }
                                if let Some(sl) = lock.script_login.as_mut() {
                                    if code != LoginCode::GameBan {
                                        sl(code, None, None);
                                    }
                                }
                            }
                            2 => {
                                // User logged in
                                let mut lock = tdata.lock().await;
                                let mut player = Player {
                                    ..Default::default()
                                };
                                player.id = data.read::<u64>().await?;
                                player.name = data.read::<String>().await?;
                                player.variables = serde_json::from_str(&data.read::<String>().await?)?;
                                let instances: Value = serde_json::from_str(&data.read::<String>().await?)?;
                                for instance in instances.as_array().unwrap() {
                                    let mut inst = Instance {
                                        ..Default::default()
                                    };
                                    inst.event_type = SyncEvent::from_u64(instance["type"].as_u64().unwrap_or_default()).unwrap_or_default();
                                    inst.variables = serde_json::from_str(instance["variables"].as_str().unwrap_or_default())?;
                                    inst.kind = instance["kind"].as_u64().unwrap_or_default() as _;
                                    inst.local_variables = serde_json::from_str(instance["local_variables"].as_str().unwrap_or_default())?;
                                    player.instances.push(inst);
                                }
                                player.room = data.read::<String>().await?;
                                lock.players.insert(player.id, player);
                            }
                            3 => {
                                // User logged out
                                let mut lock = tdata.lock().await;
                                lock.players.remove(&data.read::<u64>().await?);
                            }
                            4 => {
                                // Sync game info
                                let mut lock = tdata.lock().await;
                                lock.game_save = serde_json::from_str(&data.read::<String>().await?)?;
                                lock.game_achievements = serde_json::from_str(&data.read::<String>().await?)?;
                                lock.game_highscores = serde_json::from_str(&data.read::<String>().await?)?;
                                lock.game_administrators = serde_json::from_str(&data.read::<String>().await?)?;
                                lock.game_version = data.read::<f64>().await?;
                            }
                            5 => {
                                // P2p
                                let player_id = data.read::<u64>().await?;
                                let message_id = data.read::<i16>().await?;
                                let size = match data.read::<u8>().await? {
                                    1 => data.read::<u8>().await? as usize,
                                    2 => data.read::<u16>().await? as usize,
                                    3 => data.read::<u32>().await? as usize,
                                    4 => data.read::<u64>().await? as usize,
                                    _ => 0,
                                };
                                let mut p2p_data = Vec::with_capacity(size);
                                for _ in 0..size {
                                    match data.read::<u8>().await? {
                                        0 => p2p_data.push(Value::String(data.read::<String>().await?)),
                                        1 => p2p_data.push(Value::Number(Number::from_f64(data.read::<f64>().await?).unwrap_or(Number::from(0)))),
                                        2 => p2p_data.push(Value::Number(Number::from(data.read::<i64>().await?))),
                                        3 => p2p_data.push(Value::Null),
                                        4 => p2p_data.push(Value::Bool(data.read::<bool>().await?)),
                                        5 => p2p_data.push(data.read::<Value>().await?),
                                        6 => p2p_data.push(Value::Array({
                                            let tmp = data.read::<Vec<u8>>().await?;
                                            let mut opt = Vec::new();
                                            for val in tmp {
                                                opt.push(Value::Number(Number::from(val)));
                                            }
                                            opt
                                        })),
                                        _ => {},
                                    }
                                }
                                let mut lock = tdata.lock().await;
                                if let Some(sp) = lock.script_p2p.as_mut() {
                                    sp(message_id, player_id, p2p_data);
                                }
                            }
                            _ => {}
                        }
                        Ok(())
                    };
                    if read_result.is_err() {
                        return Ok(());
                    }
                }
                Err(_) => {
                    writer.lock().await.stream.take().unwrap().forget();
                    let _ = reader.lock().await.stream.take();
                    return Ok(());
                }
            }
        }
    }

    pub async fn p2p(code: P2pCode, message_id: i16, data: Option<Vec<Value>>) {
        let mut b = Buffer::new().await;
        b.write::<u8>(4).await;
        match code {
            P2pCode::AllGame => {
                b.write::<u8>(1).await;
            }
            P2pCode::CurrentSession => {
                b.write::<u8>(2).await;
            }
            P2pCode::CurrentRoom => {
                b.write::<u8>(3).await;
            }
            P2pCode::PlayerId(player_id) => {
                b.write::<u8>(0).await;
                b.write::<u64>(player_id).await;
            }
        }
    }
}