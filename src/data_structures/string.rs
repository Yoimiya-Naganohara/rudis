// String data structure for Rudis

use std::str::FromStr;

pub struct RedisString {
    value: String,
}

impl RedisString {
    pub fn new(value: String) -> Self {
        RedisString { value }
    }

    pub fn get(&self) -> &str {
        &self.value
    }

    pub fn set(&mut self, value: String) {
        self.value = value;
    }

    pub(crate) fn push_str(&mut self, value: &str) {
        self.value.push_str(value);
    }

    pub(crate) fn len(&self) -> usize {
        self.value.len()
    }

    pub(crate) fn parse<F: std::str::FromStr>(&self) -> Result<F, <F as FromStr>::Err> {
        self.value.parse::<F>()
    }
}
