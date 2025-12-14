// String data structure for Rudis

use std::str::FromStr;
use std::sync::Arc;

// SharedSyncString: Arc<String> for zero-copy shared reads
type SharedSyncString = Arc<String>;

#[derive(Debug, Clone)]
pub struct RedisString {
    value: SharedSyncString,
}

impl RedisString {
    pub fn new(value: String) -> Self {
        RedisString { 
            value: Arc::new(value)
        }
    }

    pub fn get(&self) -> SharedSyncString {
        Arc::clone(&self.value)
    }

    pub fn set(&mut self, value: String) {
        // Simple replacement - creates new Arc
        self.value = Arc::new(value);
    }

    pub(crate) fn push_str(&mut self, value: &str) {
        // CoW: Only clone if there are other references
        if Arc::strong_count(&self.value) > 1 {
            // Other readers exist - clone before modifying
            let mut new_value = (*self.value).clone();
            new_value.push_str(value);
            self.value = Arc::new(new_value);
        } else {
            // We're the only owner - modify in place
            let mut new_value = Arc::try_unwrap(std::mem::replace(&mut self.value, Arc::new(String::new())))
                .unwrap_or_else(|arc| (*arc).clone());
            new_value.push_str(value);
            self.value = Arc::new(new_value);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.value.len()
    }

    pub(crate) fn parse<F: std::str::FromStr>(&self) -> Result<F, <F as FromStr>::Err> {
        self.value.parse::<F>()
    }
}
