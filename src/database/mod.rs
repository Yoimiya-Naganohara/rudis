// Database module for Rudis
// In-memory data store implementation

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::data_structures::{RedisHash, RedisString};
pub type SharedDatabase = Arc<Mutex<Database>>;
enum RedisValue {
    String(RedisString),
    Hash(RedisHash),
}
pub struct Database {
    data: HashMap<String, RedisValue>,
}

impl StringOp for Database {
    fn get(&self, key: &str) -> Option<&str> {
        if let Some(RedisValue::String(value)) = self.data.get(key) {
            Some(value.get())
        } else {
            None
        }
    }

    fn set(&mut self, key: &str, value: String) {
        match self.data.get_mut(key) {
            Some(RedisValue::String(val)) => val.set(value),
            Some(_) => {
                // Key exists but is wrong type - overwrite it (Redis behavior)
                self.data.insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            },
            None => {
                self.data
                    .insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            }
        }
    }

    fn del(&mut self, keys:&Vec<String>) -> usize {
        keys.into_iter().filter(|key|self.data.remove(*key).is_some()).count()
    }

    fn incr(&mut self, key: &str) -> Result<i64, String> {
        self.add_value(key, 1)
    }

    fn decr(&mut self, key: &str) -> Result<i64, String> {
        self.add_value(key, -1)
    }

    fn incr_by(&mut self, key: &str, value: &str) -> Result<i64, String> {
        self.add_value_by_str(key, value, 1)
    }

    fn decr_by(&mut self, key: &str, value: &str) -> Result<i64, String> {
        self.add_value_by_str(key, value, -1)
    }

    fn append(&mut self, key: &str, value: &str) -> usize {
        if let Some(RedisValue::String(current_value)) = self.data.get_mut(key) {
            current_value.push_str(value);
            current_value.len()
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::String(RedisString::new(value.to_string())),
            );
            value.len()
        }
    }

    fn str_len(&self, key: &str) -> usize {
        if let Some(RedisValue::String(value)) = self.data.get(key) {
            value.len()
        } else {
            0
        }
    }
}

pub trait HashOp {
    fn hset(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, String>;
    fn hget(&self, hash: &str, field: &str) -> Result<Option<&String>, String>;
    fn hdel(&mut self, hash: &str, field: &str) -> bool;
    fn hget_all(&self, hash: &str) -> Result<Vec<&String>, String>;
    fn hkeys(&self, hash: &str) -> Result<Vec<&String>, String>;fn hvals(&self,hash: &str)->Result<Vec<&String>,String>
;}

pub trait StringOp {
    fn get(&self, key: &str) -> Option<&str>;
    fn set(&mut self, key: &str, value: String);
    fn del(&mut self, keys: &Vec<String>) -> usize;
    fn incr(&mut self, key: &str) -> Result<i64, String>;
    fn decr(&mut self, key: &str) -> Result<i64, String>;
    fn incr_by(&mut self, key: &str, value: &str) -> Result<i64, String>;
    fn decr_by(&mut self, key: &str, value: &str) -> Result<i64, String>;
    fn append(&mut self, key: &str, value: &str) -> usize;
    fn str_len(&self, key: &str) -> usize;
}

impl HashOp for Database {
    fn hset(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, String> {
        match self.data.get_mut(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                // Hash exists, update/add the field
                let result = existing_hash.hset(field.to_string(), value.to_string());
                Ok(result) // Returns 1 for new field, 0 for updated field
            }
            Some(_) => {
                // Key exists but is not a hash
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
            None => {
                // Key doesn't exist, create new hash
                let mut new_hash = RedisHash::new();
                new_hash.hset(field.to_string(), value.to_string());
                self.data
                    .insert(hash.to_string(), RedisValue::Hash(new_hash));
                Ok(1) // New field was added
            }
        }
    }
    
    fn hget(&self, hash: &str, field: &str) -> Result<Option<&String>, String> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.hget(field)),
            Some(_) => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
            None => Ok(None),
        }
    }
    
    fn hdel(&mut self, hash: &str, field: &str) -> bool {
        if let Some(RedisValue::Hash(existing_hash)) = self.data.get_mut(hash) {
            existing_hash.hdel(field)
        } else {
            false
        }
    }
    
    fn hget_all(&self, hash: &str) -> Result<Vec<&String>, String> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.flatten().collect::<Vec<&String>>())
            }
            Some(_) => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }
    
    fn hkeys(&self, hash: &str) -> Result<Vec<&String>, String> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.keys().collect::<Vec<&String>>())
            }
            Some(_) => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }fn hvals(&self,hash: &str)->Result<Vec<&String>,String> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.values().collect::<Vec<&String>>())
            }
            Some(_) => {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
        
    }
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

    // Consolidated helper for incr_by/decr_by operations
    fn add_value_by_str(&mut self, key: &str, value: &str, multiplier: i64) -> Result<i64, String> {
        match value.parse::<i64>() {
            Ok(integer) => self.add_value(key, integer * multiplier),
            Err(_) => Err("value is not an integer or out of range".to_string()),
        }
    }

    fn add_value(&mut self, key: &str, val: i64) -> Result<i64, String> {
        if let Some(RedisValue::String(current_value)) = self.data.get_mut(key) {
            match current_value.parse::<i64>() {
                Ok(integer) => {
                    let new_integer = integer + val;
                    *current_value = RedisString::new(new_integer.to_string());
                    Ok(new_integer)
                }
                Err(_) => Err("value is not an integer or out of range".to_string()),
            }
        } else {
            self.data.insert(
                key.to_string(),
                RedisValue::String(RedisString::new(val.to_string())),
            );
            Ok(val)
        }
    }
}
