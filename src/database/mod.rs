// Database module for Rudis
// In-memory data store implementation

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
pub type SharedDatabase = Arc<Mutex<Database>>;
pub struct Database {
    data: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
        }
    }
    pub fn new_shared() -> SharedDatabase {
        Arc::new(Mutex::new(Self::new()))
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn del(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }
    pub fn incr(&mut self, key: &str) -> Result<i64, String> {
        self.add_value(key, 1)
    }

    pub fn decr(&mut self, key: &str) -> Result<i64, String> {
        self.add_value(key, -1)
    }

    pub fn incr_by(&mut self, key: &str, value: &str) -> Result<i64, String> {
        self.add_value_by_str(key, value, 1)
    }

    pub fn decr_by(&mut self, key: &str, value: &str) -> Result<i64, String> {
        self.add_value_by_str(key, value, -1)
    }
    pub fn append(&mut self, key: &str, value: &str) -> usize {
        match self.data.get_mut(key) {
            Some(current_value) => {
                current_value.push_str(value);
                current_value.len()
            }
            None => {
                self.data.insert(key.to_string(), value.to_string());
                value.len()
            }
        }
    }
    // Consolidated helper for incr_by/decr_by operations
    fn add_value_by_str(&mut self, key: &str, value: &str, multiplier: i64) -> Result<i64, String> {
        match value.parse::<i64>() {
            Ok(integer) => self.add_value(key, integer * multiplier),
            Err(_) => Err("value is not an integer or out of range".to_string()),
        }
    }

    fn add_value(&mut self, key: &str, val: i64) -> Result<i64, String> {
        match self.data.get_mut(key) {
            Some(current_value) => match current_value.parse::<i64>() {
                Ok(integer) => {
                    let new_integer = integer + val;
                    *current_value = new_integer.to_string();
                    Ok(new_integer)
                }
                Err(_) => Err("value is not an integer or out of range".to_string()),
            },
            None => {
                self.data.insert(key.to_string(), val.to_string());
                Ok(val)
            }
        }
    }
}
