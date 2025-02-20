use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
};

use integer_hasher::IsEnabled;
#[cfg(feature = "__dev")]
use tracing::warn;

pub struct LebCodec;

impl LebCodec {
    #[inline(always)]
    pub fn encode(mut number: u64) -> ([u8; 16], usize) {
        let mut leb = [0u8; 16];
        let mut pos = 0;
        loop {
            let n = number & 0x7f;
            number >>= 7;
            if number == 0 {
                leb[pos] = n as u8;
                pos += 1;
                break;
            } else {
                leb[pos] = (n | 0x80) as u8;
            }
            pos += 1;
        }
        (leb, pos)
    }

    #[inline(always)]
    pub fn decode(data: &[u8]) -> u64 {
        let mut num = 0;
        let mut shift = 0;
        let mut pos = 0;
        loop {
            if pos >= data.len() {
                #[cfg(feature = "__dev")]
                warn!("Unexpected data end for LEB128 decoder: The number may be truncated or incorrect.");
                break;
            }
            num |= ((data[pos] as u64) & 0x7f).wrapping_shl(shift);
            if data[pos] & 0x80 == 0 {
                break;
            }
            shift += 7;
            pos += 1;
        }
        num
    }
}

#[derive(Clone, Copy, Eq, Debug, Default)]
pub struct Leb<T: Into<u64> + Copy + PartialEq + Eq>(pub T);

impl<T: Into<u64> + Copy + PartialEq + Eq> Deref for Leb<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Into<u64> + Copy + PartialEq + Eq> DerefMut for Leb<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Into<u64> + Copy + PartialEq + Eq> IsEnabled for Leb<T> {}

impl<T: Into<u64> + Copy + PartialEq + Eq> Hash for Leb<T> {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.into());
    }
}

impl<T: Into<u64> + Copy + PartialEq + Eq> PartialEq for Leb<T> {
    fn eq(&self, other: &Self) -> bool {
        other.0.into() == self.0.into()
    }
}

impl<T: Into<u64> + Copy + PartialEq + Eq> PartialOrd for Leb<T> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.into().cmp(&other.0.into()))
    }
}

impl<T: Into<u64> + Copy + PartialEq + Eq> Ord for Leb<T> {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.into().cmp(&other.0.into())
    }
}
