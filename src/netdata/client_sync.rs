use std::{collections::HashMap, io::Result};

use num_enum::TryFromPrimitive;

use crate::{
    buffer::{Buffer, Serialize},
    locdata::sync::{SyncEvent, SyncType},
    netdata::variable::Variable,
};

/// An object that's being synced between players
#[derive(Debug, Clone, Default)]
pub struct ClientSync {
    pub kind: i16,
    pub sync_type: SyncType,
    pub variables: HashMap<String, Variable>,
    pub event: SyncEvent,
    pub is_ending: bool,
}

impl Serialize for ClientSync {
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
