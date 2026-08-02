// Database module for Rudis
// In-memory data store implementation

use crate::commands::{CommandError, Result};
use crate::data_structures::{RedisHash, RedisList, RedisSet, RedisSortedSet, RedisString};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::AtomicU8;
use std::time::SystemTime;
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
    pub(crate) current_db: AtomicU8,
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
            current_db: AtomicU8::new(0),
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
        // current_db is only written by select(), which refuses indices
        // >= data.len(), so the lookup below cannot fail.
        self.data
            .get(&self.current_db.load(std::sync::atomic::Ordering::Relaxed))
            .expect("current DB index is always a valid key (select validates)")
    }

    fn current_expiration(&self) -> &DashMap<Bytes, SystemTime> {
        // Same invariant as current_data: the map is keyed by the same DB
        // indices and current_db is validated by select().
        self.data_expiration_time
            .get(&self.current_db.load(std::sync::atomic::Ordering::Relaxed))
            .expect("current DB index is always a valid key (select validates)")
    }

    fn add_value(&self, key: &Bytes, val: i64) -> Result<i64> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::String(current_value) = entry.value_mut() {
                // Parse the existing value as an integer
                match current_value.parse::<i64>() {
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

pub mod hashes;
pub mod keys;
pub mod lists;
pub mod sets;
pub mod strings;
pub mod zsets;
