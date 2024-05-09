use std::{collections::HashMap, io::{Cursor, Read, Result, Seek, SeekFrom, Write}};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bstr::BString;
use integer_hasher::IntMap;

#[derive(Clone, Debug)]
pub struct Buffer {
    pub container: Cursor<Vec<u8>>,
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        Self {
            container: Cursor::new(value)
        }
    }
}

impl Buffer {

    /// The time to initialize may be slow since it has to
    /// initialize the data by Cloning.
    /// 
    /// A faster option may be to use Buffer::from(data)
    /// But keep in mind the data type needs to be `Vec<u8>`
    /// otherwise, you will need to use this function to
    /// initialize fast.
    /// 
    /// As an alternative, you may use this:
    /// ```rust
    /// let mut buffer = Buffer::empty();
    /// buffer.container = Cursor::new(data);
    /// ```
    pub fn new<T>(container: T) -> Self
        where T: AsRef<[u8]>,
    {
        Self {
            container: Cursor::new(Vec::from(container.as_ref())),
        }
    }
    pub fn empty() -> Self {
        Self {
            container: Cursor::new(Vec::new()),
        }
    }

    pub fn stream_position(&mut self) -> Result<u64> { self.container.stream_position() }

    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> { self.container.seek(pos) }

    pub fn seek_relative(&mut self, offset: i64) -> Result<u64> { self.container.seek(SeekFrom::Current(offset)) }

    pub fn read<T: Serialize>(&mut self) -> Result<T> { T::read(self) }

    pub fn read_buf(&mut self, buf: &mut [u8]) -> Result<usize> { self.container.read(buf) }
    
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> { self.container.read_exact(buf) }

    pub fn read_u8(&mut self) -> Result<u8> { self.container.read_u8() }

    pub fn read_u16(&mut self) -> Result<u16> { self.container.read_u16::<LittleEndian>() }

    pub fn read_u32(&mut self) -> Result<u32> { self.container.read_u32::<LittleEndian>() }

    pub fn read_u64(&mut self) -> Result<u64> { self.container.read_u64::<LittleEndian>() }

    pub fn read_u128(&mut self) -> Result<u128> { self.container.read_u128::<LittleEndian>() }

    pub fn read_i8(&mut self) -> Result<i8> { self.container.read_i8() }

    pub fn read_i16(&mut self) -> Result<i16> { self.container.read_i16::<LittleEndian>() }

    pub fn read_i32(&mut self) -> Result<i32> { self.container.read_i32::<LittleEndian>() }

    pub fn read_i64(&mut self) -> Result<i64> { self.container.read_i64::<LittleEndian>() }

    pub fn read_i128(&mut self) -> Result<i128> { self.container.read_i128::<LittleEndian>() }

    pub fn read_f32(&mut self) -> Result<f32> { self.container.read_f32::<LittleEndian>() }

    pub fn read_f64(&mut self) -> Result<f64> { self.container.read_f64::<LittleEndian>() }

    pub fn read_bool(&mut self) -> Result<bool> { Ok(self.read_u8()? != 0) }

    pub fn read_wide_bool(&mut self) -> Result<bool> { Ok(self.read_u32()? != 0) }

    pub fn read_string(&mut self) -> Result<String> {
        let mut string = Vec::new();
        let mut i = self.read_u8()?;
        while i != 0 {
            string.push(i);
            i = self.read_u8()?;
        }
        Ok(BString::new(string).to_string())
    }

    pub fn read_bytes<const S: usize>(&mut self) -> Result<[u8; S]> {
        let mut buf = [0u8; S];
        self.container.read_exact(&mut buf)?;
        Ok(buf)
    }
    
    pub fn write<T: Serialize>(&mut self, value: &T) -> Result<()> { value.write(self) }

    pub fn write_buf(&mut self, buf: &mut [u8]) -> Result<usize> { self.container.write(buf) }

    pub fn write_all(&mut self, buf: &[u8]) -> Result<()> { self.container.write_all(buf) }

    pub fn write_u8(&mut self, value: u8) -> Result<()> { self.container.write_u8(value) }

    pub fn write_u16(&mut self, value: u16) -> Result<()> { self.container.write_u16::<LittleEndian>(value) }

    pub fn write_u32(&mut self, value: u32) -> Result<()> { self.container.write_u32::<LittleEndian>(value) }

    pub fn write_u64(&mut self, value: u64) -> Result<()> { self.container.write_u64::<LittleEndian>(value) }

    pub fn write_u128(&mut self, value: u128) -> Result<()> { self.container.write_u128::<LittleEndian>(value) }

    pub fn write_i8(&mut self, value: i8) -> Result<()> { self.container.write_i8(value) }

    pub fn write_i16(&mut self, value: i16) -> Result<()> { self.container.write_i16::<LittleEndian>(value) }

    pub fn write_i32(&mut self, value: i32) -> Result<()> { self.container.write_i32::<LittleEndian>(value) }

    pub fn write_i64(&mut self, value: i64) -> Result<()> { self.container.write_i64::<LittleEndian>(value) }

    pub fn write_i128(&mut self, value: i128) -> Result<()> { self.container.write_i128::<LittleEndian>(value) }

    pub fn write_f32(&mut self, value: f32) -> Result<()> { self.container.write_f32::<LittleEndian>(value) }

    pub fn write_f64(&mut self, value: f64) -> Result<()> { self.container.write_f64::<LittleEndian>(value) }

    pub fn write_bool(&mut self, value: bool) -> Result<()> { self.container.write_u8(value as u8) }

    pub fn write_wide_bool(&mut self, value: bool) -> Result<()> { self.container.write_u32::<LittleEndian>(value as u32) }

    pub fn write_bytes(&mut self, value: &[u8]) -> Result<()> { self.container.write_all(value) }

    pub fn write_string(&mut self, value: &str) -> Result<()> {
        self.write_bytes(value.as_bytes())?;
        self.write_u8(0)
    }

    pub fn is_empty(&mut self) -> Result<bool> {
        let pos = self.stream_position()?;
        let size = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(pos))?;
        Ok(size == 0)
    }

    pub fn size(&mut self) -> Result<u64> {
        let pos = self.stream_position()?;
        let size = self.seek(SeekFrom::End(0))?;
        self.seek(SeekFrom::Start(pos))?;
        Ok(size)
    }
}

pub trait Serialize
    where Self: Sized,
{
    fn write(&self, buffer: &mut Buffer) -> Result<()>;
    fn read(buffer: &mut Buffer) -> Result<Self>;
}

impl<T> Serialize for Vec<T>
    where T: Serialize,
{
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u64(self.len() as u64)?;

        for value in self.iter() {
            value.write(buffer)?;
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Vec::new();

        for _ in 0..buffer.read_u64()? {
            data.push(T::read(buffer)?);
        }

        Ok(data)
    }
}

impl<T> Serialize for Option<T>
    where T: Serialize,
{
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        if let Some(value) = &self {
            buffer.write_bool(true)?;
            value.write(buffer)?;
        } else {
            buffer.write_bool(false)?;
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        if buffer.read_bool()? {
            Ok(Some(buffer.read()?))
        } else {
            Ok(None)
        }
    }
}

impl<T, V> Serialize for HashMap<T, V>
    where T: Serialize + PartialEq + Eq + std::hash::Hash, V: Serialize,
{
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u64(self.len() as u64)?;

        for (key, value) in self {
            key.write(buffer)?;
            value.write(buffer)?;
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Self::new();

        for _ in 0..buffer.read_u64()? {
            let key = T::read(buffer)?;
            data.insert(key, V::read(buffer)?);
        }

        Ok(data)
    }
}

/// This is essentialy the same as the HashMap implementation.
impl<T, V> Serialize for IntMap<T, V>
    where T: Serialize + PartialEq + Eq + std::hash::Hash + integer_hasher::IsEnabled, V: Serialize,
{
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u64(self.len() as u64)?;

        for (key, value) in self {
            key.write(buffer)?;
            value.write(buffer)?;
        }

        Ok(())
    }

    fn read(buffer: &mut Buffer) -> Result<Self> {
        let mut data = Self::default();

        for _ in 0..buffer.read_u64()? {
            let key = T::read(buffer)?;
            data.insert(key, V::read(buffer)?);
        }

        Ok(data)
    }
}

impl Serialize for String {
    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_string(self)
    }

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
