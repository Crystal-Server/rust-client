use bstr::BString;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use integer_hasher::{IntMap, IntSet};
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
};

use super::leb::{Leb, LebCodec};

#[derive(Default, Clone, Debug)]
pub struct Buffer {
    pub container: Cursor<Vec<u8>>,
}

impl Buffer {
    #[inline(always)]
    pub fn new(container: Cursor<Vec<u8>>) -> Self {
        Self { container }
    }

    #[inline(always)]
    pub fn empty() -> Self {
        Self {
            container: Cursor::new(Vec::new()),
        }
    }

    #[inline(always)]
    pub fn position(&mut self) -> Result<u64> {
        self.container.stream_position()
    }

    #[inline(always)]
    pub fn len(&mut self) -> Result<u64> {
        let cur = self.position()?;
        let len = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(cur))?;
        Ok(len)
    }

    #[inline(always)]
    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.container.seek(pos)
    }

    #[inline(always)]
    pub fn seek_relative(&mut self, offset: i64) -> Result<u64> {
        self.container.seek(SeekFrom::Current(offset))
    }

    #[inline(always)]
    pub fn read<V: Serialize>(&mut self) -> Result<V> {
        V::read(self)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn read_buf(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.container.read(buf)
    }

    #[inline(always)]
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.container.read_exact(buf)
    }

    #[inline(always)]
    pub fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.container.read_to_end(buf)
    }

    #[inline(always)]
    pub fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<()> {
        let pos = self.position()?;
        self.seek(SeekFrom::Start(0))?;
        self.read_to_end(buf)?;
        self.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    #[inline(always)]
    pub fn read_u8(&mut self) -> Result<u8> {
        self.container.read_u8()
    }

    #[inline(always)]
    pub fn read_u16(&mut self) -> Result<u16> {
        self.container.read_u16::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_u32(&mut self) -> Result<u32> {
        self.container.read_u32::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_u64(&mut self) -> Result<u64> {
        self.container.read_u64::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_leb_u64(&mut self) -> Result<u64> {
        let mut data = [0u8; 16];
        let mut current = 0;

        loop {
            let n = self.read_u8()?;
            data[current] = n;
            current += 1;
            if current > 15 {
                return Err(Error::new(
                    ErrorKind::OutOfMemory,
                    "Expected 16 bytes at most, but found no trail end byte within 16 bytes.",
                ));
            }
            if n & 0x80 == 0 {
                // No more bytes
                break;
            }
        }

        Ok(LebCodec::decode(&data))
    }

    #[inline(always)]
    pub fn read_u128(&mut self) -> Result<u128> {
        self.container.read_u128::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_i8(&mut self) -> Result<i8> {
        self.container.read_i8()
    }

    #[inline(always)]
    pub fn read_i16(&mut self) -> Result<i16> {
        self.container.read_i16::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_i32(&mut self) -> Result<i32> {
        self.container.read_i32::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_i64(&mut self) -> Result<i64> {
        self.container.read_i64::<LittleEndian>()
    }

    /// This function doesn't support encryption.
    #[inline(always)]
    pub fn read_i128(&mut self) -> Result<i128> {
        self.container.read_i128::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_f32(&mut self) -> Result<f32> {
        self.container.read_f32::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_f64(&mut self) -> Result<f64> {
        self.container.read_f64::<LittleEndian>()
    }

    #[inline(always)]
    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    #[inline(always)]
    pub fn read_string(&mut self) -> Result<String> {
        let size = self.read_leb_u64()?;
        if self.len()? - self.position()? >= size {
            let mut string = vec![0; size as usize];
            self.read_exact(&mut string)?;
            Ok(BString::new(string).to_string())
        } else {
            Err(Error::from(ErrorKind::UnexpectedEof))
        }
    }

    #[inline(always)]
    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let size = self.read_leb_u64()?;
        if self.len()? - self.position()? >= size {
            let mut bytes = vec![0; size as usize];
            self.read_exact(&mut bytes)?;
            Ok(bytes)
        } else {
            Err(Error::from(ErrorKind::UnexpectedEof))
        }
    }

    #[inline(always)]
    pub fn write<V: Serialize>(&mut self, value: &V) -> Result<()> {
        value.write(self)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_buf(&mut self, buf: &[u8]) -> Result<usize> {
        self.container.write(buf)
    }

    #[inline(always)]
    pub fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.container.write_all(buf)
    }

    #[inline(always)]
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.container.write_u8(value)
    }

    #[inline(always)]
    pub fn write_u16(&mut self, value: u16) -> Result<()> {
        self.container.write_u16::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        self.container.write_u32::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_u64(&mut self, value: u64) -> Result<()> {
        self.container.write_u64::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_leb_u64(&mut self, value: u64) -> Result<()> {
        let leb = LebCodec::encode(value);
        self.write_all(&leb.0[..leb.1])
    }

    #[inline(always)]
    pub fn write_u128(&mut self, value: u128) -> Result<()> {
        self.container.write_u128::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_i8(&mut self, value: i8) -> Result<()> {
        self.container.write_i8(value)
    }

    #[inline(always)]
    pub fn write_i16(&mut self, value: i16) -> Result<()> {
        self.container.write_i16::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_i32(&mut self, value: i32) -> Result<()> {
        self.container.write_i32::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_i64(&mut self, value: i64) -> Result<()> {
        self.container.write_i64::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_i128(&mut self, value: i128) -> Result<()> {
        self.container.write_i128::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_f32(&mut self, value: f32) -> Result<()> {
        self.container.write_f32::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_f64(&mut self, value: f64) -> Result<()> {
        self.container.write_f64::<LittleEndian>(value)
    }

    #[inline(always)]
    pub fn write_bool(&mut self, value: bool) -> Result<()> {
        self.write_u8(value as u8)
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, value: &[u8]) -> Result<()> {
        self.write_leb_u64(value.len() as u64)?;
        self.write_all(value)
    }

    #[inline(always)]
    pub fn write_string(&mut self, value: &str) -> Result<()> {
        self.write_leb_u64(value.len() as u64)?;
        self.write_all(value.as_bytes())
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

pub trait Serialize
where
    Self: Sized,
{
    fn write(&self, buffer: &mut Buffer) -> Result<()>;
    fn read(buffer: &mut Buffer) -> Result<Self>;
}

impl<T> Serialize for Vec<T>
where
    T: Serialize,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(self.len() as u64)?;

        for value in self.iter() {
            value.write(buffer)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Vec::new();

        for _ in 0..buffer.read_leb_u64()? {
            data.push(T::read(buffer)?);
        }

        Ok(data)
    }
}

impl<T> Serialize for Option<T>
where
    T: Serialize,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        if let Some(value) = &self {
            buffer.write_bool(true)?;
            value.write(buffer)?;
        } else {
            buffer.write_bool(false)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        if buffer.read_bool()? {
            Ok(Some(buffer.read()?))
        } else {
            Ok(None)
        }
    }
}

impl<T, V> Serialize for HashMap<T, V>
where
    T: Serialize + PartialEq + Eq + std::hash::Hash,
    V: Serialize,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(self.len() as u64)?;

        for (key, value) in self {
            key.write(buffer)?;
            value.write(buffer)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Self::new();

        for _ in 0..buffer.read_leb_u64()? {
            let key = T::read(buffer)?;
            data.insert(key, V::read(buffer)?);
        }

        Ok(data)
    }
}

/// This is essentially the same as the HashMap implementation.
impl<T, V> Serialize for IntMap<T, V>
where
    T: Serialize + PartialEq + Eq + std::hash::Hash + integer_hasher::IsEnabled,
    V: Serialize,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(self.len() as u64)?;

        for (key, value) in self {
            key.write(buffer)?;
            value.write(buffer)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Self::default();

        for _ in 0..buffer.read_leb_u64()? {
            let key = T::read(buffer)?;
            data.insert(key, V::read(buffer)?);
        }

        Ok(data)
    }
}

impl<T> Serialize for HashSet<T>
where
    T: Serialize + PartialEq + Eq + std::hash::Hash,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(self.len() as u64)?;

        for value in self {
            value.write(buffer)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Self::new();

        for _ in 0..buffer.read_leb_u64()? {
            data.insert(T::read(buffer)?);
        }

        Ok(data)
    }
}

impl<T> Serialize for IntSet<T>
where
    T: Serialize + PartialEq + Eq + std::hash::Hash + integer_hasher::IsEnabled,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(self.len() as u64)?;

        for value in self {
            value.write(buffer)?;
        }

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Self::default();

        for _ in 0..buffer.read_leb_u64()? {
            data.insert(T::read(buffer)?);
        }

        Ok(data)
    }
}

impl Serialize for String {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_string(self)
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_string()
    }
}

impl Serialize for u8 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u8(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_u8()
    }
}

impl Serialize for u16 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u16(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_u16()
    }
}

impl Serialize for u32 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u32(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_u32()
    }
}

impl Serialize for u64 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u64(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_u64()
    }
}

impl Serialize for Leb<u64> {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_leb_u64(**self)
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Leb(buffer.read_leb_u64()?))
    }
}

impl Serialize for u128 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u128(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_u128()
    }
}

impl Serialize for i8 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i8(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_i8()
    }
}

impl Serialize for i16 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i16(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_i16()
    }
}

impl Serialize for i32 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i32(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_i32()
    }
}

impl Serialize for i64 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i64(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_i64()
    }
}

impl Serialize for i128 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i128(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_i128()
    }
}

impl Serialize for f32 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_f32(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_f32()
    }
}

impl Serialize for f64 {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_f64(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_f64()
    }
}

impl Serialize for bool {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_bool(*self)
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        buffer.read_bool()
    }
}

impl<T> Serialize for Result<T>
where
    T: Serialize,
{
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        match &self {
            Ok(val) => {
                buffer.write_bool(true)?;
                buffer.write(val)
            }
            Err(e) => {
                buffer.write_bool(false)?;
                buffer.write(e)
            }
        }
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(if buffer.read_bool()? {
            Ok(buffer.read()?)
        } else {
            Err(buffer.read()?)
        })
    }
}

impl Serialize for Error {
    #[inline(always)]
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_i32(self.raw_os_error().unwrap_or_default())?;

        Ok(())
    }

    #[inline(always)]
    fn read(buffer: &mut Buffer) -> Result<Self> {
        Ok(Self::from_raw_os_error(buffer.read_i32()?))
    }
}

impl Serialize for () {
    #[inline(always)]
    fn write(&self, _buffer: &mut Buffer) -> Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn read(_buffer: &mut Buffer) -> Result<Self> {
        Ok(())
    }
}
