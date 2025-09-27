use std::io::Result;

use crate::{
    buffer::{Buffer, Serialize},
    netdata::optional_variable::OptionalVariable,
};

#[derive(Debug, Clone)]
pub(crate) struct VariableUpdate {
    pub name: String,
    pub value: OptionalVariable,
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
