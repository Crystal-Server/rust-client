use std::io::Result;

use crate::buffer::{Buffer, Serialize};

#[derive(Debug, Clone)]
pub struct Achievement {
    pub name: String,
    pub description: String,
    pub unlocked: Option<i64>,
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
