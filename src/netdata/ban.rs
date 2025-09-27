use std::io::Result;

use crate::buffer::{Buffer, Serialize};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Ban {
    pub unban_time: i64,
    pub reason: String,
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
