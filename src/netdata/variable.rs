use std::{collections::HashMap, io::Result};

use crate::buffer::{Buffer, Serialize};

/// A structure that represents a dynamic value.
#[derive(Default, Debug, Clone, PartialEq)]
pub enum Variable {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Variable>),
    Struct(HashMap<String, Variable>),
    Buffer(Vec<u8>),
}

impl Serialize for Variable {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match self {
            Self::Null => {
                buffer.write_u8(0)?;
            }
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
            Self::Buffer(value) => {
                buffer.write_u8(7)?;
                buffer.write_bytes(value)?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(match buffer.read_u8()? {
            1 => Self::Bool(buffer.read_bool()?),
            2 => Self::Int(buffer.read_i64()?),
            3 => Self::Float(buffer.read_f64()?),
            4 => Self::String(buffer.read_string()?),
            5 => Self::Array(buffer.read()?),
            6 => Self::Struct(buffer.read()?),
            7 => Self::Buffer(buffer.read_bytes()?),

            // Both 0 and any other unidentified values
            // will be recognized as Null
            _ => Self::Null,
        })
    }
}

impl From<Vec<Variable>> for Variable {
    fn from(value: Vec<Variable>) -> Self {
        Self::Array(value)
    }
}

impl From<HashMap<String, Variable>> for Variable {
    fn from(value: HashMap<String, Variable>) -> Self {
        Self::Struct(value)
    }
}

impl From<Option<Variable>> for Variable {
    fn from(value: Option<Variable>) -> Self {
        if let Some(value) = value {
            value
        } else {
            Self::Null
        }
    }
}

impl From<f64> for Variable {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for Variable {
    fn from(value: f32) -> Self {
        Self::Float(value as f64)
    }
}

impl From<i64> for Variable {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for Variable {
    fn from(value: i32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u32> for Variable {
    fn from(value: u32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<i16> for Variable {
    fn from(value: i16) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u16> for Variable {
    fn from(value: u16) -> Self {
        Self::Int(value as i64)
    }
}

impl From<i8> for Variable {
    fn from(value: i8) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u8> for Variable {
    fn from(value: u8) -> Self {
        Self::Int(value as i64)
    }
}

impl From<bool> for Variable {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<String> for Variable {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&Vec<Variable>> for Variable {
    fn from(value: &Vec<Variable>) -> Self {
        Self::Array(value.clone())
    }
}

impl From<&HashMap<String, Variable>> for Variable {
    fn from(value: &HashMap<String, Variable>) -> Self {
        Self::Struct(value.clone())
    }
}

impl From<&Option<Variable>> for Variable {
    fn from(value: &Option<Variable>) -> Self {
        if let Some(value) = value {
            value.clone()
        } else {
            Self::Null
        }
    }
}

impl From<&f64> for Variable {
    fn from(value: &f64) -> Self {
        Self::Float(*value)
    }
}

impl From<&f32> for Variable {
    fn from(value: &f32) -> Self {
        Self::Float(*value as f64)
    }
}

impl From<&i64> for Variable {
    fn from(value: &i64) -> Self {
        Self::Int(*value)
    }
}

impl From<&i32> for Variable {
    fn from(value: &i32) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&u32> for Variable {
    fn from(value: &u32) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&i16> for Variable {
    fn from(value: &i16) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&u16> for Variable {
    fn from(value: &u16) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&i8> for Variable {
    fn from(value: &i8) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&u8> for Variable {
    fn from(value: &u8) -> Self {
        Self::Int(*value as i64)
    }
}

impl From<&bool> for Variable {
    fn from(value: &bool) -> Self {
        Self::Bool(*value)
    }
}

impl From<&String> for Variable {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&Variable> for Variable {
    fn from(value: &Variable) -> Self {
        value.clone()
    }
}

impl From<&str> for Variable {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<&[Variable]> for Variable {
    fn from(value: &[Variable]) -> Self {
        Self::Array(value.to_vec())
    }
}

impl<const N: usize> From<&&[u8; N]> for Variable {
    fn from(value: &&[u8; N]) -> Self {
        Self::Buffer(value.to_vec())
    }
}

impl From<&[u8]> for Variable {
    fn from(value: &[u8]) -> Self {
        Self::Buffer(value.to_vec())
    }
}

impl From<&Vec<u8>> for Variable {
    fn from(value: &Vec<u8>) -> Self {
        Self::Buffer(value.clone())
    }
}

impl From<Vec<u8>> for Variable {
    fn from(value: Vec<u8>) -> Self {
        Self::Buffer(value)
    }
}
