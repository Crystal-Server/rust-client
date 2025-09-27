use std::{collections::HashMap, io::Result};

use crate::{
    buffer::{Buffer, Serialize},
    netdata::optional_variable::OptionalVariable,
};

#[derive(Debug, Clone)]
pub(crate) struct SyncUpdate {
    pub slot: usize,
    pub remove_sync: bool,
    pub variables: Option<HashMap<String, OptionalVariable>>,
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
