use chrono::{DateTime, Utc};
use integer_hasher::IntMap;
use num_enum::TryFromPrimitive;

use super::{
    buffer::{Buffer, Serialize},
    leb::Leb,
};
use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind, Result},
};

// TODO: Do a macro or something that builds this more easily
/// A structure that represents a dynamic value.
#[derive(Default, Debug, Clone, PartialEq)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Struct(HashMap<String, Value>),
    Buffer(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Achievement {
    pub name: String,
    pub description: String,
    pub unlocked: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct Highscore {
    pub name: String,
    pub scores: IntMap<Leb<u64>, f64>,
}

#[derive(Default, Debug, Copy, Clone, PartialEq)]
pub struct Administrator {
    pub can_kick: bool,
    pub can_ban: bool,
    pub can_unban: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Ban {
    pub unban_time: i64,
    pub reason: String,
}

/// Contains the data of a connected Player
#[derive(Debug, Clone)]
pub struct Player {
    pub name: String,
    pub room: String,
    pub syncs: Vec<Option<Sync>>,
    pub variables: HashMap<String, Value>,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct PlayerQueue {
    pub variables: HashMap<String, OptionalValue>,
    pub syncs: IntMap<usize, HashMap<String, OptionalValue>>,
    pub remove_syncs: Vec<usize>,
    pub new_syncs: Vec<NewSync>,
}

#[derive(Debug, Clone)]
pub(crate) struct NewSync {
    pub slot: usize,
    pub sync_type: SyncType,
    pub kind: i16,
    pub variables: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct NewSyncQueue {
    pub slot: usize,
    pub kind: i16,
    pub sync_type: SyncType,
}

#[derive(Debug)]
pub(crate) struct CallbackServerUpdate {
    pub name: String,
    pub callback: ServerUpdateCallback,
}

pub type PlayerVariableServerUpdate =
    Box<dyn FnMut(u64, String, OptionalValue) + core::marker::Sync + Send>;

pub type SyncVariableServerUpdate =
    Box<dyn FnMut(u64, String, OptionalValue) + core::marker::Sync + Send>;

pub type FetchBdbServerUpdate = Box<dyn FnMut(String, Option<Vec<u8>>) + core::marker::Sync + Send>;

pub(crate) enum ServerUpdateCallback {
    /// Callback, Player ID
    PlayerVariable(Option<PlayerVariableServerUpdate>, u64),
    /// Callback, Player ID, Sync Slot
    SyncVariable(Option<SyncVariableServerUpdate>, u64, usize),
    /// Callback, BDB Name
    FetchBdb(Option<FetchBdbServerUpdate>),
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum SyncEvent {
    #[default]
    New = 0,
    Step = 1,
    End = 2,
    Once = 3,
}

/// The target syncronization type
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(u8)]
pub enum SyncType {
    Once = 0,
    #[default]
    Normal = 1,
}

/// Data updates that have been triggered from the server.
#[derive(Debug, Clone, PartialEq)]
pub enum DataUpdate {
    /// Return Code
    Registration(RegistrationCode),
    /// Return Code
    Login(LoginCode),
    /// Player ID, Player Name
    LoginOk(u64, String),
    /// Return Code, Reason, Unix unban time
    LoginBan(LoginCode, String, i64),
    /// Player ID, Player Name, Room
    PlayerLoggedIn(u64, String, String),
    /// Player ID
    PlayerLoggedOut(u64),
    /// Player ID or Server, Message ID, Payload
    P2P(Option<u64>, i16, Vec<Value>),
    /// Player ID, Variable Name, Variable Value
    UpdateVariable(u64, String, OptionalValue),
    /// Player ID, Sync ID, Variable Name, Variable Value
    UpdateSyncVariable(u64, usize, String, OptionalValue),
    /// Player ID, Sync ID
    UpdateSyncRemoval(u64, usize),
    /// (Optional) File, Section, Key, Value
    UpdateGameIni(Option<String>, String, String, OptionalValue),
    /// (Optional) File, Section, Key, Value
    UpdatePlayerIni(Option<String>, String, String, OptionalValue),
    /// Version
    UpdateGameVersion(f64),
    /// Admin Action
    AdminAction(AdminAction),
    /// Player ID, Administrator Permissions
    UpdateAdministrator(u64, Option<Administrator>),
    /// Name, Value
    FetchBdb(String, Option<Vec<u8>>),
    /// Player ID
    ChangeFriendStatus(u64),
    /// Message
    ServerMessage(String),
    Reconnecting(),
    Disconnected(),
    /// Reason
    Kicked(String),
    /// Reason, Unban Unix Time
    Banned(String, DateTime<Utc>),
    /// Notification Message
    ServerNotification(String),
    /// Login Token
    LoginToken(String),
}

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

/// The return success value of a login attempt
#[derive(Default, Copy, Clone, Debug, TryFromPrimitive, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LoginCode {
    Ok = 0,
    NoUser = 1,
    WrongPassword = 2,
    Unauthenticated = 3,
    Unverified = 4,
    AlreadyIn = 5,
    GameBan = 6,
    GlobalBan = 7,
    #[default]
    Error = 8,
    MaxPlayers = 9,
}

/// The target it should request something from/to
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlayerRequestCode {
    AllGame,
    PlayerId(u64),
}

#[derive(Debug, Clone)]
pub(crate) enum LoginPassw {
    Passw(String),
    Token(String),
}

/// The target it should request something from/to
#[derive(Debug, Clone, Copy)]
pub enum PlayerRequest {
    ID(u64),
    AllGame,
    CurrentSession,
    CurrentRoom,
    Server,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AdminAction {
    Unban,
    /// Reason, Unix unban time
    Ban(String, i64),
    /// Reason
    Kick(String),
}

#[derive(Debug, Clone)]
pub(crate) struct VariableUpdate {
    pub name: String,
    pub value: OptionalValue,
}

#[derive(Debug, Clone)]
pub(crate) struct SyncUpdate {
    pub slot: usize,
    pub remove_sync: bool,
    pub variables: Option<HashMap<String, OptionalValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptionalValue {
    Some(Value),
    None,
}

/// An object that's being synced between players
#[derive(Debug, Clone, Default)]
pub struct Sync {
    pub kind: i16,
    pub sync_type: SyncType,
    pub variables: HashMap<String, Value>,
    pub event: SyncEvent,
    pub is_ending: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SelfSync {
    pub kind: i16,
    pub sync_type: SyncType,
    pub variables: HashMap<String, Value>,
    pub to_sync: HashSet<String>,
}

/// A return value from an iterator containing data about a Sync from another player
#[derive(Debug, Clone, Default)]
pub struct SyncIter {
    pub player_id: u64,
    pub player_name: String,
    pub slot: usize,
    pub event: SyncEvent,
    pub kind: i16,
    pub variables: HashMap<String, Value>,
}

#[derive(Default, Debug, Copy, Clone, TryFromPrimitive)]
#[repr(u8)]
pub(crate) enum ChangeFriendStatus {
    // Outgoing
    Request = 0,
    Cancel = 1,
    // Incoming
    Accept = 2,
    Deny = 3,
    //
    Remove = 4,
    // Misc
    Friend = 5,
    #[default]
    NotFriend = 6,
}

impl std::fmt::Debug for ServerUpdateCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::FetchBdb(_) => "FetchBdb(...)",
                Self::PlayerVariable(_, _) => "PlayerVariable(...)",
                Self::SyncVariable(_, _, _) => "SyncVariable(...)",
            }
        )
    }
}

impl Serialize for Sync {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i16(self.kind)?;
        buffer.write_u8(self.sync_type as u8)?;
        buffer.write(&self.variables)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok({
            let mut s = Self {
                kind: buffer.read_i16()?,
                sync_type: SyncType::try_from_primitive(buffer.read_u8()?).unwrap_or_default(),
                variables: buffer.read()?,
                event: SyncEvent::New,
                is_ending: false,
            };
            if s.sync_type == SyncType::Once {
                s.event = SyncEvent::Once;
            }
            s
        })
    }
}

impl Serialize for SelfSync {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i16(self.kind)?;
        buffer.write_u8(self.sync_type as u8)?;
        buffer.write(&self.variables)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            kind: buffer.read_i16()?,
            sync_type: SyncType::try_from_primitive(buffer.read_u8()?).unwrap_or_default(),
            variables: buffer.read()?,
            to_sync: HashSet::new(),
        })
    }
}

impl Serialize for SyncUpdate {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(self.slot as u64)?;
        buffer.write_bool(self.remove_sync)?;
        if let Some(data) = &self.variables {
            assert!(
                !self.remove_sync,
                "unable to sync variables when remove sync is enabled"
            );
            buffer.write(data)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let slot = buffer.read_leb_u64()? as usize;
        let remove_sync = buffer.read_bool()?;
        let variables = if remove_sync {
            None
        } else {
            Some(buffer.read()?)
        };
        Ok(Self {
            slot,
            remove_sync,
            variables,
        })
    }
}

impl Serialize for OptionalValue {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            OptionalValue::Some(vari) => {
                buffer.write(vari)?;
            }
            OptionalValue::None => {
                buffer.write_u8(0xff)?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let has_data = buffer.read_u8()? != 0xff;
        buffer.seek_relative(-1)?;
        let vari = buffer.read::<Value>()?;
        Ok(if has_data {
            OptionalValue::Some(vari)
        } else {
            OptionalValue::None
        })
    }
}

impl Serialize for VariableUpdate {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_string(&self.name)?;
        buffer.write(&self.value)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            name: buffer.read_string()?,
            value: buffer.read()?,
        })
    }
}

impl From<Option<Value>> for OptionalValue {
    #[inline(always)]
    fn from(value: Option<Value>) -> Self {
        match value {
            Some(value) => OptionalValue::Some(value),
            None => OptionalValue::None,
        }
    }
}

impl Serialize for PlayerRequest {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            PlayerRequest::ID(pid) => {
                buffer.write_u8(0)?;
                buffer.write_leb_u64(*pid)?;
            }
            PlayerRequest::AllGame => {
                buffer.write_u8(1)?;
            }
            PlayerRequest::CurrentSession => {
                buffer.write_u8(2)?;
            }
            PlayerRequest::CurrentRoom => {
                buffer.write_u8(3)?;
            }
            PlayerRequest::Server => {
                buffer.write_u8(4)?;
            }
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        match buffer.read_u8()? {
            0 => Ok(PlayerRequest::ID(buffer.read_leb_u64()?)),
            1 => Ok(PlayerRequest::AllGame),
            2 => Ok(PlayerRequest::CurrentSession),
            3 => Ok(PlayerRequest::CurrentRoom),
            4 => Ok(PlayerRequest::Server),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid PlayerRequest handle",
            )),
        }
    }
}

impl Serialize for Value {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            Self::Null => {
                buffer.write_u8(0)?;
            }
            Self::Bool(value) => {
                buffer.write_u8(1)?;
                buffer.write_bool(*value)?;
            }
            Self::Int(value) => {
                buffer.write_u8(2)?;
                buffer.write_i64(*value)?;
            }
            Self::Float(value) => {
                buffer.write_u8(3)?;
                buffer.write_f64(*value)?;
            }
            Self::String(value) => {
                buffer.write_u8(4)?;
                buffer.write_string(value)?;
            }
            Self::Array(value) => {
                buffer.write_u8(5)?;
                buffer.write(value)?;
            }
            Self::Struct(value) => {
                buffer.write_u8(6)?;
                buffer.write(value)?;
            }
            Self::Buffer(value) => {
                buffer.write_u8(7)?;
                buffer.write_bytes(value)?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(match buffer.read_u8()? {
            1 => Self::Bool(buffer.read_bool()?),
            2 => Self::Int(buffer.read_i64()?),
            3 => Self::Float(buffer.read_f64()?),
            4 => Self::String(buffer.read_string()?),
            5 => Self::Array(buffer.read()?),
            6 => Self::Struct(buffer.read()?),
            7 => Self::Buffer(buffer.read_bytes()?),

            // Both 0 and any other unidentified values
            // will be recognized as Null
            _ => Self::Null,
        })
    }
}

impl Serialize for Achievement {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_string(&self.name)?;
        buffer.write_string(&self.description)?;
        buffer.write(&self.unlocked)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            name: buffer.read_string()?,
            description: buffer.read_string()?,
            unlocked: buffer.read()?,
        })
    }
}

impl Serialize for Highscore {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_string(&self.name)?;
        buffer.write(&self.scores)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            name: buffer.read_string()?,
            scores: buffer.read()?,
        })
    }
}

impl Serialize for Administrator {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_bool(self.can_kick)?;
        buffer.write_bool(self.can_ban)?;
        buffer.write_bool(self.can_unban)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            can_kick: buffer.read_bool()?,
            can_ban: buffer.read_bool()?,
            can_unban: buffer.read_bool()?,
        })
    }
}

impl Serialize for Ban {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i64(self.unban_time)?;
        buffer.write_string(&self.reason)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            unban_time: buffer.read_i64()?,
            reason: buffer.read_string()?,
        })
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Self::Array(value)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(value: HashMap<String, Value>) -> Self {
        Self::Struct(value)
    }
}

impl From<Option<Value>> for Value {
    fn from(value: Option<Value>) -> Self {
        if let Some(value) = value {
            value
        } else {
            Self::Null
        }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::Float(value as f64)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::Int(value as i64)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Self::Int(value as i64)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&Vec<Value>> for Value {
    fn from(value: &Vec<Value>) -> Self {
        Self::Array(value.clone())
    }
}

impl From<&HashMap<String, Value>> for Value {
    fn from(value: &HashMap<String, Value>) -> Self {
        Self::Struct(value.clone())
    }
}

impl From<&Option<Value>> for Value {
    fn from(value: &Option<Value>) -> Self {
        if let Some(value) = value {
            value.clone()
        } else {
            Self::Null
        }
    }
}

impl From<&f64> for Value {
    fn from(value: &f64) -> Self {
        Self::Float(*value)
    }
}

impl From<&f32> for Value {
    fn from(value: &f32) -> Self {
        Self::Float(*value as f64)
    }
}

impl From<&i64> for Value {
    fn from(value: &i64) -> Self {
        Self::Int(*value)
    }
}

impl From<&i32> for Value {
    fn from(value: &i32) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&u32> for Value {
    fn from(value: &u32) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&i16> for Value {
    fn from(value: &i16) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&u16> for Value {
    fn from(value: &u16) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&i8> for Value {
    fn from(value: &i8) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&u8> for Value {
    fn from(value: &u8) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&bool> for Value {
    fn from(value: &bool) -> Self {
        Self::Bool(*value)
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&Value> for Value {
    fn from(value: &Value) -> Self {
        value.clone()
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<&[Value]> for Value {
    fn from(value: &[Value]) -> Self {
        Self::Array(value.to_vec())
    }
}

impl<const N: usize> From<&&[u8; N]> for Value {
    fn from(value: &&[u8; N]) -> Self {
        Self::Buffer(value.to_vec())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Buffer(value.to_vec())
    }
}

impl From<&Vec<u8>> for Value {
    fn from(value: &Vec<u8>) -> Self {
        Self::Buffer(value.clone())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Buffer(value)
    }
}
