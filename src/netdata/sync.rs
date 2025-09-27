use std::{
    collections::{HashMap, HashSet},
    io::Result,
};

use num_enum::TryFromPrimitive;

use crate::{
    buffer::{Buffer, Serialize},
    locdata::sync::SyncType,
    netdata::variable::Variable,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct SelfSync {
    pub kind: i16,
    pub sync_type: SyncType,
    pub variables: HashMap<String, Variable>,
    pub to_sync: HashSet<String>,
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
