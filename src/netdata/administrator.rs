use std::io::Result;

use crate::buffer::{Buffer, Serialize};

#[derive(Default, Debug, Copy, Clone, PartialEq)]
pub struct Administrator {
    pub can_kick: bool,
    pub can_ban: bool,
    pub can_unban: bool,
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
