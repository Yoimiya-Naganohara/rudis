// String data structure for Rudis

use bytes::{Bytes, BytesMut};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct RedisString {
    value: Bytes,
}

impl RedisString {
    pub fn new(value: Bytes) -> Self {
        RedisString { value }
    }

    pub fn get(&self) -> Bytes {
        self.value.clone()
    }

    pub fn set(&mut self, value: Bytes) {
        self.value = value;
    }

    pub(crate) fn append(&mut self, value: Bytes) {
        let mut new_value = BytesMut::with_capacity(self.value.len() + value.len());
        new_value.extend_from_slice(&self.value);
        new_value.extend_from_slice(&value);
        self.value = new_value.freeze();
    }

    pub(crate) fn len(&self) -> usize {
        self.value.len()
    }

    /// Try to parse the string as a number (integer or float)
    /// Returns error if the bytes are not valid UTF-8 or not a valid number
    pub(crate) fn parse<F: FromStr>(&self) -> Result<F, ()> {
        // We return Result<F, ()> to simplify error handling for now,
        // as Utf8Error and ParseIntError/ParseFloatError are different types.
        // In a real app we'd want a unified error type here.
        let s = std::str::from_utf8(&self.value).map_err(|_| ())?;
        s.parse::<F>().map_err(|_| ())
    }
}
