use std::io::{Error, ErrorKind, Result};

use crate::buffer::{Buffer, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, std::hash::Hash)]
pub enum BdbPermissionTarget {
    Default,
    AllAdministrators,
    GameOwner,
    PlayerId(u64),
}

impl Serialize for BdbPermissionTarget {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            Self::Default => buffer.write_u8(0)?,
            Self::AllAdministrators => buffer.write_u8(1)?,
            Self::GameOwner => buffer.write_u8(2)?,
            Self::PlayerId(pid) => {
                buffer.write_u8(3)?;
                buffer.write_leb_u64(*pid)?;
            }
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        match buffer.read_u8()? {
            0 => Ok(Self::Default),
            1 => Ok(Self::AllAdministrators),
            2 => Ok(Self::GameOwner),
            3 => Ok(Self::PlayerId(buffer.read_leb_u64()?)),
            _ => Err(Error::from(ErrorKind::InvalidData)),
        }
    }
}
