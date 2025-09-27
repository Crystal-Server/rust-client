use std::io::Result;

use crate::{
    buffer::{Buffer, Serialize},
    netdata::variable::Variable,
};

#[derive(Debug, Clone, PartialEq)]
pub enum OptionalVariable {
    Some(Variable),
    None,
}

impl Serialize for OptionalVariable {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            OptionalVariable::Some(vari) => {
                buffer.write(vari)?;
            }
            OptionalVariable::None => {
                buffer.write_u8(0xff)?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let has_data = buffer.read_u8()? != 0xff;
        buffer.seek_relative(-1)?;
        let vari = buffer.read::<Variable>()?;
        Ok(if has_data {
            OptionalVariable::Some(vari)
        } else {
            OptionalVariable::None
        })
    }
}

impl From<Option<Variable>> for OptionalVariable {
    #[inline(always)]
    fn from(value: Option<Variable>) -> Self {
        match value {
            Some(value) => OptionalVariable::Some(value),
            None => OptionalVariable::None,
        }
    }
}
