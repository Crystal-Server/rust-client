use std::{io::{Result as IoResult, Cursor}, sync::Arc};
use async_compression::tokio::write::{ZstdDecoder, ZstdEncoder};
use chrono::{DateTime, TimeZone, Utc};
use integer_hasher::IntMap;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use serde_json::{Number, Value};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream}, runtime::{Builder, Runtime}, sync::Mutex, task::JoinHandle};
use super::{buffer::Buffer, types::Variable};
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
type ScriptP2P = dyn FnMut(i16, u64, Vec<Variable>)
                        + Sync + Send;

#[derive(Default)]
pub struct StreamData {
    script_register: Option<Box<ScriptRegister>>,
    script_login: Option<Box<ScriptLogin>>,
    script_p2p: Option<Box<ScriptP2P>>,
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
pub enum P2PCode {
    AllGame = -5,
    CurrentSession = -6,
    CurrentRoom = -7,
    PlayerId(u64) = -8,
}
macro_rules! prepare_buffer {
    ($data: expr) => {{
        let s_og = $data.size().unwrap();

        let mut enc = ZstdEncoder::new(Vec::new());
        enc.write_all($data.container.get_ref()).await.unwrap();
        enc.shutdown().await.unwrap();
        let enc = enc.into_inner();
        let enc_s = enc.len() as u64;
        let mut compr = false;
        if enc_s + ((2 as u64).pow(
            if enc_s <= u8::MAX as u64 {
                0
            } else if enc_s <= u16::MAX as u64 {
                1
            } else if enc_s <= u32::MAX as u64 {
                2
            } else {
                3
            }
        )) < s_og {
            $data.container = Cursor::new(enc);
            compr = true;
        }

        let s = $data.size().unwrap();

        $data.container.get_mut().insert(0,
            if s <= u8::MAX as u64 {
                0
            } else if s <= u16::MAX as u64 {
                1
            } else if s <= u32::MAX as u64 {
                2
            } else {
                3
            } | ((compr as u8) << 7)
        );
        if s <= u8::MAX as u64 {
            for byte in (s as u8).to_be_bytes() {
                $data.container.get_mut().insert(1, byte);
            }
        } else if s <= u16::MAX as u64 {
            for byte in (s as u16).to_be_bytes() {
                $data.container.get_mut().insert(1, byte);
            }
        } else if s <= u32::MAX as u64 {
            for byte in (s as u32).to_be_bytes() {
                $data.container.get_mut().insert(1, byte);
            }
        } else {
            for byte in s.to_be_bytes() {
                $data.container.get_mut().insert(1, byte);
            }
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
    pub async fn read(&mut self) -> Result<Buffer, ReaderError> {
        if let Some(stream) = self.stream.as_mut() {
            unwrap_return!(stream.readable().await, Err(ReaderError::StreamError));
            let mut size = 0u64;
            let is_compr;
            if let Ok(n) = stream.read_u8().await {
                is_compr = (n & 0x80) != 0;
                match n & 0x7f {
                    0 => {
                        if let Ok(s) = stream.read_u8().await {
                            size = s as u64;
                        } else {
                            return Err(ReaderError::StreamError);
                        }
                    }
                    1 => {
                        if let Ok(s) = stream.read_u16().await {
                            size = s as u64;
                        } else {
                            return Err(ReaderError::StreamError);
                        }
                    }
                    2 => {
                        if let Ok(s) = stream.read_u32().await {
                            size = s as u64;
                        } else {
                            return Err(ReaderError::StreamError);
                        }
                    }
                    3 => {
                        if let Ok(s) = stream.read_u64().await {
                            size = s;
                        } else {
                            return Err(ReaderError::StreamError);
                        }
                    }
                    _ => {}
                }
                if let Ok(n) = stream.read_u8().await {
                    match n {
                        0 => {
                            if stream.read_u8().await.is_err() {
                                return Err(ReaderError::StreamError);
                            }
                        }
                        1 => {
                            if stream.read_u16().await.is_err() {
                                return Err(ReaderError::StreamError);
                            }
                        }
                        2 => {
                            if stream.read_u32().await.is_err() {
                                return Err(ReaderError::StreamError);
                            }
                        }
                        3 => {
                            if stream.read_u64().await.is_err() {
                                return Err(ReaderError::StreamError);
                            }
                        }
                        _ => {}
                    }
                } else {
                    return Err(ReaderError::StreamError);
                }
            } else {
                return Err(ReaderError::StreamError);
            }
            //info!("size: {}, compr: {}", size, is_compr);
            if size > 0 {
                let mut buf = vec![0; size as usize];
                unwrap_return!(
                    stream.read_exact(&mut buf).await,
                    Err(ReaderError::StreamError)
                );
                if is_compr {
                    let mut dec = ZstdDecoder::new(Vec::new());
                    unwrap_return!(dec.write_all(&buf).await, Err(ReaderError::StreamError));
                    unwrap_return!(dec.shutdown().await, Err(ReaderError::StreamError));
                    Ok(Buffer::new(dec.into_inner()))
                } else {
                    Ok(Buffer::new(buf))
                }
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
            prepare_buffer!(data);
            unwrap_return!(stream.writable().await, Err(WriterError::StreamError));
            unwrap_return!(stream.write_all(data.container.get_ref()).await, Err(WriterError::StreamError));
            unwrap_return!(stream.flush().await, Err(WriterError::StreamError));
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
            match reader.lock().await.read().await {
                Ok(mut data) => {
                    let read_result: IoResult<()> = {
                        match data.read_u8()? {
                            0 => {
                                // Registration
                                let code = RegistrationCode::from_u8(data.read_u8()?).unwrap();
                                let mut lock = tdata.lock().await;
                                if let Some(sr) = lock.script_register.as_mut() {
                                    sr(code);
                                }
                            }
                            1 => {
                                // Login
                                let code = LoginCode::from_u8(data.read_u8()?).unwrap();
                                let mut lock = tdata.lock().await;
                                match code {
                                    LoginCode::OK => {
                                        lock.is_loggedin = true;
                                        lock.player_id = Some(data.read_u64()?);
                                        lock.player_name = Some(data.read::<String>()?);
                                        lock.player_save = serde_json::from_str(&data.read::<String>()?)?;
                                    }
                                    LoginCode::GameBan => {
                                        let unban_time = data.read_u64()?;
                                        let reason = data.read::<String>()?;
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
                                player.id = data.read_u64()?;
                                player.name = data.read::<String>()?;
                                player.variables = serde_json::from_str(&data.read::<String>()?)?;
                                let instances: Value = serde_json::from_str(&data.read::<String>()?)?;
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
                                player.room = data.read::<String>()?;
                                lock.players.insert(player.id, player);
                            }
                            3 => {
                                // User logged out
                                let mut lock = tdata.lock().await;
                                lock.players.remove(&data.read_u64()?);
                            }
                            4 => {
                                // Sync game info
                                let mut lock = tdata.lock().await;
                                lock.game_save = serde_json::from_str(&data.read::<String>()?)?;
                                lock.game_achievements = serde_json::from_str(&data.read::<String>()?)?;
                                lock.game_highscores = serde_json::from_str(&data.read::<String>()?)?;
                                lock.game_administrators = serde_json::from_str(&data.read::<String>()?)?;
                                lock.game_version = data.read::<f64>()?;
                            }
                            5 => {
                                // P2P
                                let player_id = data.read_u64()?;
                                let message_id = data.read_i16()?;
                                let data = data.read::<Vec<Variable>>()?;
                                let mut lock = tdata.lock().await;
                                if let Some(sp) = lock.script_p2p.as_mut() {
                                    sp(message_id, player_id, data);
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

    pub async fn p2p(code: P2PCode, message_id: i16, data: Option<Vec<Value>>) -> IoResult<()> {
        let mut b = Buffer::empty();
        b.write_u8(4)?;
        match code {
            P2PCode::AllGame => {
                b.write_u8(1)?;
            }
            P2PCode::CurrentSession => {
                b.write_u8(2)?;
            }
            P2PCode::CurrentRoom => {
                b.write_u8(3)?;
            }
            P2PCode::PlayerId(player_id) => {
                b.write_u8(0)?;
                b.write_u64(player_id)?;
            }
        }

        Ok(())
    }
}