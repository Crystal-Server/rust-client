use bstr::{BString, ByteSlice};
use serde_json::Value;
#[cfg(feature = "debug")]
use tracing::*;
use std::io::{Error, ErrorKind};

macro_rules! write_buf {
    ($kind: ty, $s: expr, $buf: expr) => {
        let data_size = std::mem::size_of::<$kind>();
        $s.write_verify_space(data_size as _).await;
        for index in 0..data_size {
            $s.data[$s.offset as usize + index] = $buf[index];
        }
        //$s.data.extend($buf);
    };
}
macro_rules! read_buf {
    ($kind: ty, $s: expr) => {
        {
            let data_size = std::mem::size_of::<$kind>();
            $s.read_verify_space(data_size as _).await?;
            Ok(&$s.data[$s.offset as usize..$s.offset as usize + data_size])
        }
    };
}

macro_rules! read_until_null {
    ($s: expr) => {
        {
            let start_offset = $s.offset;
            let mut data_size = 0;
            while start_offset + data_size < $s.data.len() as isize && $s.data[(start_offset + data_size) as usize] != 0 {
                data_size += 1;
            }
            Ok(&$s.data[$s.offset as usize..$s.offset as usize + data_size as usize])
        }
    };
}

macro_rules! read_bytes {
    ($s: expr) => {
        {
            let data_size = u64::from_le_bytes($s.data[$s.offset as usize..$s.offset as usize + 8].try_into().unwrap_or_default());
            $s.offset += 8;
            $s.read_verify_space(data_size as _).await?;
            Ok(&$s.data[$s.offset as usize..$s.offset as usize + data_size as usize])
        }
    };
}

pub trait BSerialize: Sized {
    async fn write(&self, buf: &mut Buffer);
    async fn read(buf: &mut Buffer) -> Result<Self, Error>;
}

pub enum BufferSeek {
    Start,
    Current,
    End,
}

pub struct Buffer {
    pub data: Vec<u8>,
    pub offset: isize,
}

impl Buffer {
    pub async fn write_verify_space(&mut self, size: isize) {
        if self.offset + size >= self.data.len() as isize {
            self.data.reserve(size as _);
        }
    }

    pub async fn read_verify_space(&self, size: isize) -> Result<(), Error>{
        if self.offset + size > self.data.len() as isize {
            #[cfg(feature = "debug")]
            error!("Trying to read {} byte(s) more than allocated while reading a {} byte-object.", (self.offset - self.data.len() as isize) + size, size);
            Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires."))
        } else {
            Ok(())
        }
    }

    /// Generates a new `Buffer`
    pub async fn new() -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
        }
    }

    /// Generates a new `Buffer` with the given data
    pub async fn new_with_data(data: Vec<u8>) -> Self {
        Self {
            data,
            offset: 0,
        }
    }

    /// Changes the `Buffer` position to a specified offset.
    pub async fn seek(&mut self, seek: BufferSeek, offset: isize) {
        match seek {
            BufferSeek::Start => {
                self.offset = offset;
            }
            BufferSeek::Current => {
                self.offset += offset;
            }
            BufferSeek::End => {
                self.offset = self.data.len() as isize + offset;
            }
        }
    }

    /// Returns the current `Buffer` offset.
    pub async fn get_seek(&self) -> isize {
        self.offset
    }

    /// Writes a value that supports `BSerialize` into the buffer utilizing the current offset.
    pub async fn write<V>(&mut self, value: V)
        where V: BSerialize,
    {
        value.write(self);
    }

    /// Reads a value that supports `BSerialize` from the buffer utilizing the current offset.
    pub async fn read<V>(&mut self) -> Result<V, Error>
        where V: BSerialize,
    {
        V::read(self).await
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        Self {
            data: value,
            offset: 0,
        }
    }
}

impl BSerialize for u8 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for u16 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for u32 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for u64 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for u128 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for i8 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for i16 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for i32 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for i64 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for i128 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for f32 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for f64 {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_buf!(Self, buf) {
            Ok(data) => Ok(Self::from_le_bytes(match data.try_into() {
                Ok(data) => data,
                Err(_) => return Err(Error::new(ErrorKind::UnexpectedEof, "The requested buffer has less byte(s) left to read than the requested type requires.")),
            })),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.to_le_bytes());
    }
}

impl BSerialize for String {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_until_null!(buf) {
            Ok(data) => Ok(BString::new(data.into()).to_string()),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, self.as_bytes());
    }
}

impl BSerialize for BString {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_until_null!(buf) {
            Ok(data) => Ok(Self::new(data.into())),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, &self.to_vec());
        buf.write::<u8>(0).await;
    }
}

impl BSerialize for Value {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_until_null!(buf) {
            Ok(data) => Ok(serde_json::from_str(match BString::new(data.into()).to_str() {
                Ok(str) => str,
                Err(_) => return Err(Error::new(ErrorKind::InvalidData, "Invalid UTF-8 data encountered while reading a value.")),
            })?),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        write_buf!(Self, buf, serde_json::to_vec(&self).unwrap_or_default());
        buf.write::<u8>(0).await;
    }
}

impl BSerialize for Vec<u8> {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match read_bytes!(buf) {
            Ok(data) => Ok(data.into()),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        buf.write::<u64>(self.len() as _).await;
        buf.write_verify_space(self.len() as _).await;
        for (index, value) in self.iter().enumerate() {
            buf.data[buf.offset as usize + index] = *value;
        }
    }
}

impl BSerialize for bool {
    async fn read(buf: &mut Buffer) -> Result<Self, Error> {
        match buf.read::<u8>().await {
            Ok(data) => Ok(data != 0),
            Err(e) => Err(e),
        }
    }

    async fn write(&self, buf: &mut Buffer) {
        buf.write::<u8>(*self as u8).await;
    }
}
