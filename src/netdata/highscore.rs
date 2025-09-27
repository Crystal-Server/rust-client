use std::io::Result;

use integer_hasher::IntMap;

use crate::{
    buffer::{Buffer, Serialize},
    leb::Leb,
};

#[derive(Debug, Clone)]
pub struct Highscore {
    pub name: String,
    pub scores: IntMap<Leb<u64>, f64>,
}

impl Serialize for Highscore {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_string(&self.name)?;
        buffer.write(&self.scores)?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self {
            name: buffer.read_string()?,
            scores: buffer.read()?,
        })
    }
}
