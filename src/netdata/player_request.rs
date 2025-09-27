use std::io::{Error, ErrorKind, Result};

use crate::buffer::{Buffer, Serialize};

/// The target it should request something from/to
#[derive(Debug, Clone, Copy)]
pub enum PlayerRequest {
    /// Player ID
    ID(u64),
    AllGame,
    CurrentSession,
    CurrentRoom,
    Server,
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
