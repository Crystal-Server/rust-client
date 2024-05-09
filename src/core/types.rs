use std::{collections::HashMap, io::Result};
use super::buffer::{Buffer, Serialize};

#[derive(Default, Debug, Clone, PartialEq)]
pub enum Variable {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Variable>),
    /// Note: all values MUST be converted to String for Hashing.
    Struct(HashMap<String, Variable>),
}

impl Serialize for Variable {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            Self::Null => { buffer.write_u8(0)?; }
            Self::Bool(value) => {
                buffer.write_u8(1)?;
                buffer.write_bool(*value)?;
            }
            Self::Int(value) => {
                buffer.write_u8(2)?;
                buffer.write_i64(*value)?;
            }
            Self::Float(value) => {
                buffer.write_u8(3)?;
                buffer.write_f64(*value)?;
            }
            Self::String(value) => {
                buffer.write_u8(4)?;
                buffer.write_string(value)?;
            }
            Self::Array(value) => {
                buffer.write_u8(5)?;
                buffer.write(value)?;
            }
            Self::Struct(value) => {
                buffer.write_u8(6)?;
                buffer.write(value)?;
            }
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(match buffer.read_u8()? {
            1 => { Self::Bool(buffer.read_bool()?) }
            2 => { Self::Int(buffer.read_i64()?) }
            3 => { Self::Float(buffer.read_f64()?) }
            4 => { Self::String(buffer.read_string()?) }
            5 => { Self::Array(buffer.read()?) }
            6 => { Self::String(buffer.read()?) }

            // Both 0 and any other unidentified values
            // will be recognized as Null
            _ => Self::Null
        })
    }
}