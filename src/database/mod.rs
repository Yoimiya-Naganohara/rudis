// Database module for Rudis
// In-memory data store implementation

use crate::commands::{CommandError, Result};
use crate::data_structures::{RedisHash, RedisList, RedisSet, RedisSortedSet, RedisString};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use regex::Regex;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};

// Type definitions
pub type SharedDatabase = Arc<Database>;

#[derive(Debug)]
pub(crate) enum RedisValue {
    String(RedisString),
    Hash(RedisHash),
    List(RedisList),
    Set(RedisSet),
    SortedSet(RedisSortedSet),
}

#[derive(Debug)]
pub struct Database {
    pub(crate) data: HashMap<u8, DashMap<Bytes, RedisValue>>,
    pub(crate) data_expiration_time: HashMap<u8, DashMap<Bytes, SystemTime>>,
    pub(crate) current_db: Mutex<u8>,
}

pub mod traits;

impl Database {
    pub fn new(db_num: usize) -> Self {
        let mut data = HashMap::new();
        let mut data_expiration_time = HashMap::new();

        for i in 0..db_num {
            data.insert(i as u8, DashMap::new());
            data_expiration_time.insert(i as u8, DashMap::new());
        }
        Database {
            data,
            data_expiration_time,
            current_db: Mutex::new(0),
        }
    }
    pub fn new_shared(db_num: usize) -> SharedDatabase {
        Arc::new(Self::new(db_num))
    }
    pub fn data_type(&self, key: &Bytes) -> &str {
        match self.current_data().get(key) {
            Some(data) => match data.value() {
                RedisValue::String(_) => "string",
                RedisValue::Hash(_) => "hash",
                RedisValue::List(_) => "list",
                RedisValue::Set(_) => "set",
                RedisValue::SortedSet(_) => "zset",
            },
            None => "none",
        }
    }
    fn current_data(&self) -> &DashMap<Bytes, RedisValue> {
        let db = *self.current_db.lock();
        self.data.get(&db).unwrap()
    }

    fn current_expiration(&self) -> &DashMap<Bytes, SystemTime> {
        let db = *self.current_db.lock();
        self.data_expiration_time.get(&db).unwrap()
    }

    fn add_value(&self, key: &Bytes, val: i64) -> Result<i64> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::String(current_value) = entry.value_mut() {
                // Parse existing Bytes to i64
                let val_bytes = current_value.get();
                let s =
                    std::str::from_utf8(&val_bytes).map_err(|_| CommandError::InvalidInteger)?;
                match s.parse::<i64>() {
                    Ok(integer) => {
                        let new_integer = integer + val;
                        *current_value = RedisString::new(Bytes::from(new_integer.to_string()));
                        Ok(new_integer)
                    }
                    Err(_) => Err(CommandError::InvalidInteger),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            data.insert(
                key.clone(),
                RedisValue::String(RedisString::new(Bytes::from(val.to_string()))),
            );
            Ok(val)
        }
    }
}

pub mod keys;
pub mod strings;
pub mod hashes;
pub mod lists;
pub mod sets;
pub mod zsets;
