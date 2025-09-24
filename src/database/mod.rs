// Database module for Rudis
// In-memory data store implementation

use crate::commands::CommandError;
use crate::data_structures::{list, set, RedisHash, RedisList, RedisSet, RedisString};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

// Type definitions
pub type SharedDatabase = Arc<Mutex<Database>>;

#[derive(Debug)]
enum RedisValue {
    String(RedisString),
    Hash(RedisHash),
    List(RedisList),
    Set(RedisSet),
}

#[derive(Debug)]
pub struct Database {
    data: HashMap<String, RedisValue>,
}

// Traits
pub trait StringOp {
    fn get(&self, key: &str) -> Option<&str>;
    fn set(&mut self, key: &str, value: String);
    fn del(&mut self, keys: &Vec<String>) -> usize;
    fn incr(&mut self, key: &str) -> Result<i64, CommandError>;
    fn decr(&mut self, key: &str) -> Result<i64, CommandError>;
    fn incr_by(&mut self, key: &str, value: &str) -> Result<i64, CommandError>;
    fn decr_by(&mut self, key: &str, value: &str) -> Result<i64, CommandError>;
    fn append(&mut self, key: &str, value: &str) -> usize;
    fn str_len(&self, key: &str) -> usize;
}

pub trait HashOp {
    fn hset(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, CommandError>;
    fn hget(&self, hash: &str, field: &str) -> Result<Option<&String>, CommandError>;
    fn hdel(&mut self, hash: &str, field: &str) -> bool;
    fn hdel_multiple(&mut self, hash: &str, fields: &[String]) -> usize;
    fn hget_all(&self, hash: &str) -> Result<Vec<&String>, CommandError>;
    fn hkeys(&self, hash: &str) -> Result<Vec<&String>, CommandError>;
    fn hvals(&self, hash: &str) -> Result<Vec<&String>, CommandError>;
    fn hlen(&self, hash: &str) -> Result<usize, CommandError>;
    fn hexists(&self, hash: &str, field: &str) -> Result<bool, CommandError>;
    fn hincrby(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, CommandError>;
    fn hincrbyfloat(&mut self, hash: &str, field: &str, value: &str) -> Result<f64, CommandError>;
}

pub trait ListOp {
    fn lpush(&mut self, key: &str, values: &[String]) -> usize;
    fn rpush(&mut self, key: &str, values: &[String]) -> usize;
    fn lpop(&mut self, key: &str) -> Option<String>;
    fn rpop(&mut self, key: &str) -> Option<String>;
    fn llen(&self, key: &str) -> usize;
    fn lindex(&self, key: &str, index: &str) -> Option<&String>;
    fn lrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<&String>, CommandError>;
    fn ltrim(&mut self, key: &str, start: &str, end: &str) -> Result<(), CommandError>;
    fn lset(&mut self, key: &str, index: &str, value: String) -> Result<(), CommandError>;
    fn linsert(
        &mut self,
        key: &str,
        ord: &str,
        pivot: &str,
        value: String,
    ) -> Result<i64, CommandError>;
}
pub trait SetOp {
    fn sadd(&mut self, key: &str, values: &[String]) -> usize;
    fn srem(&mut self, key: &str, values: &[String]) -> usize;
    fn smembers(&mut self, key: &str) -> Result<Vec<&String>, CommandError>;
    fn scard(&mut self, key: &str) -> usize;
    fn sismember(&mut self, key: &str,member:&str) -> bool;
    fn sinter(&mut self, keys: &[String]) -> Result<Vec<&String>, CommandError>;
    fn sunion(&mut self, keys: &[String]) -> Result<Vec<&String>, CommandError>;
    fn sdiff(&mut self, keys: &[String]) -> Result<Vec<&String>, CommandError>;
}

impl SetOp for Database {
    fn sadd(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::Set(set)) = self.data.get_mut(key) {
            values
                .iter()
                .filter_map(|val| Some(set.sadd(val.to_owned())))
                .count()
        } else {
            0
        }
    }

    fn srem(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::Set(set)) = self.data.get_mut(key) {
            values
                .iter()
                .filter_map(|val| Some(set.srem(val)))
                .count()
        } else {
            0
        }
    }

    fn smembers(&mut self, key: &str) -> Result<Vec<&String>, CommandError> {
        if let Some(RedisValue::Set(set)) = self.data.get_mut(key) {
            Ok(set.smembers())
        }else {
            Err(CommandError::WrongType)
        }
    }

    fn scard(&mut self, key: &str) -> usize {
        if let Some(RedisValue::Set(set)) = self.data.get_mut(key) {
            set.scard()
        }else {
            0
        }
    }

    fn sismember(&mut self, key: &str,member:&str) -> bool {
        if let Some(RedisValue::Set(set)) = self.data.get_mut(key) {
            set.sismember(member)
        }else {
            false
        }
    }

    fn sinter(&mut self, keys: &[String]) -> Result<Vec<&String>, CommandError> {
        todo!()
    }

    fn sunion(&mut self, keys: &[String]) -> Result<Vec<&String>, CommandError> {
        todo!()
    }

    fn sdiff(&mut self, keys: &[String]) -> Result<Vec<&String>, CommandError> {
        todo!()
    }
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
                self.data
                    .insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            }
            None => {
                self.data
                    .insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            }
        }
    }

    fn del(&mut self, keys: &Vec<String>) -> usize {
        keys.into_iter()
            .filter(|key| self.data.remove(*key).is_some())
            .count()
    }

    fn incr(&mut self, key: &str) -> Result<i64, CommandError> {
        self.add_value(key, 1)
    }

    fn decr(&mut self, key: &str) -> Result<i64, CommandError> {
        self.add_value(key, -1)
    }

    fn incr_by(&mut self, key: &str, value: &str) -> Result<i64, CommandError> {
        self.add_value_by_str(key, value, 1)
    }

    fn decr_by(&mut self, key: &str, value: &str) -> Result<i64, CommandError> {
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

impl HashOp for Database {
    fn hset(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, CommandError> {
        match self.data.get_mut(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                // Hash exists, update/add the field
                let result = existing_hash.hset(field.to_string(), value.to_string());
                Ok(result) // Returns 1 for new field, 0 for updated field
            }
            Some(_) => {
                // Key exists but is not a hash
                Err(CommandError::WrongType)
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

    fn hget(&self, hash: &str, field: &str) -> Result<Option<&String>, CommandError> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.hget(field)),
            Some(_) => Err(CommandError::WrongType),
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

    fn hdel_multiple(&mut self, hash: &str, fields: &[String]) -> usize {
        if let Some(RedisValue::Hash(existing_hash)) = self.data.get_mut(hash) {
            fields
                .iter()
                .filter(|field| existing_hash.hdel(field))
                .count()
        } else {
            0
        }
    }

    fn hget_all(&self, hash: &str) -> Result<Vec<&String>, CommandError> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.flatten().collect::<Vec<&String>>())
            }
            Some(_) => Err(CommandError::WrongType),
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hkeys(&self, hash: &str) -> Result<Vec<&String>, CommandError> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.keys().collect::<Vec<&String>>())
            }
            Some(_) => Err(CommandError::WrongType),
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hvals(&self, hash: &str) -> Result<Vec<&String>, CommandError> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.values().collect::<Vec<&String>>())
            }
            Some(_) => Err(CommandError::WrongType),
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hlen(&self, hash: &str) -> Result<usize, CommandError> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.len()),
            Some(_) => Err(CommandError::WrongType),
            None => Ok(0), // 0 for non-existent keys
        }
    }

    fn hexists(&self, hash: &str, field: &str) -> Result<bool, CommandError> {
        match self.data.get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.hexists(field)),
            Some(_) => Err(CommandError::WrongType),
            None => Ok(false), // false for non-existent keys
        }
    }

    fn hincrby(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, CommandError> {
        match self.data.get_mut(hash) {
            Some(RedisValue::Hash(existing_hash)) => match value.parse::<i64>() {
                Ok(integer) => match existing_hash.hincrby(field, integer) {
                    Ok(result) => Ok(result),
                    Err(_) => Err(CommandError::InvalidInteger),
                },
                Err(_) => Err(CommandError::InvalidInteger),
            },
            Some(_) => Err(CommandError::WrongType),
            None => {
                // Key doesn't exist, create new hash with field set to value
                let mut new_hash = RedisHash::new();
                match value.parse::<i64>() {
                    Ok(integer) => match new_hash.hincrby(field, integer) {
                        Ok(result) => {
                            self.data
                                .insert(hash.to_string(), RedisValue::Hash(new_hash));
                            Ok(result)
                        }
                        Err(_) => Err(CommandError::InvalidInteger),
                    },
                    Err(_) => Err(CommandError::InvalidInteger),
                }
            }
        }
    }

    fn hincrbyfloat(&mut self, hash: &str, field: &str, value: &str) -> Result<f64, CommandError> {
        match self.data.get_mut(hash) {
            Some(RedisValue::Hash(existing_hash)) => match value.parse::<f64>() {
                Ok(float_val) => match existing_hash.hincrbyfloat(field, float_val) {
                    Ok(result) => Ok(result),
                    Err(_) => Err(CommandError::InvalidFloat),
                },
                Err(_) => Err(CommandError::InvalidFloat),
            },
            Some(_) => Err(CommandError::WrongType),
            None => {
                // Key doesn't exist, create new hash with field set to value
                let mut new_hash = RedisHash::new();
                match value.parse::<f64>() {
                    Ok(float_val) => match new_hash.hincrbyfloat(field, float_val) {
                        Ok(result) => {
                            self.data
                                .insert(hash.to_string(), RedisValue::Hash(new_hash));
                            Ok(result)
                        }
                        Err(_) => Err(CommandError::InvalidFloat),
                    },
                    Err(_) => Err(CommandError::InvalidFloat),
                }
            }
        }
    }
}

impl ListOp for Database {
    fn lpush(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            values.iter().for_each(|value| list.lpush(value.to_owned()));
            values.len()
        } else {
            0
        }
    }

    fn rpush(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            values.iter().for_each(|value| list.rpush(value.to_owned()));
            values.len()
        } else {
            0
        }
    }

    fn lpop(&mut self, key: &str) -> Option<String> {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            list.lpop()
        } else {
            None
        }
    }

    fn rpop(&mut self, key: &str) -> Option<String> {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            list.rpop()
        } else {
            None
        }
    }

    fn llen(&self, key: &str) -> usize {
        if let Some(RedisValue::List(list)) = self.data.get(key) {
            list.len()
        } else {
            0
        }
    }

    fn lindex(&self, key: &str, index: &str) -> Option<&String> {
        if let Some(RedisValue::List(list)) = self.data.get(key) {
            match index.parse() {
                Ok(integer) => list.index(integer),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn lrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<&String>, CommandError> {
        if let Some(RedisValue::List(list)) = self.data.get(key) {
            if let Ok(start) = start.parse() {
                if let Ok(end) = end.parse() {
                    Ok(list.range(start, end))
                } else {
                    Err(CommandError::InvalidInteger)
                }
            } else {
                Err(CommandError::InvalidInteger)
            }
        } else {
            Ok(Vec::new())
        }
    }

    fn ltrim(&mut self, key: &str, start: &str, end: &str) -> Result<(), CommandError> {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            if let Ok(start) = start.parse() {
                if let Ok(end) = end.parse() {
                    Ok(list.trim(start, end))
                } else {
                    Err(CommandError::InvalidInteger)
                }
            } else {
                Err(CommandError::InvalidInteger)
            }
        } else {
            Ok(())
        }
    }

    fn lset(&mut self, key: &str, index: &str, value: String) -> Result<(), CommandError> {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            if let Ok(index) = index.parse() {
                Ok(list.set(index, value))
            } else {
                Err(CommandError::InvalidInteger)
            }
        } else {
            Err(CommandError::KeyNotFound)
        }
    }

    fn linsert(
        &mut self,
        key: &str,
        ord: &str,
        pivot: &str,
        value: String,
    ) -> Result<i64, CommandError> {
        if let Some(RedisValue::List(list)) = self.data.get_mut(key) {
            list.insert(ord, pivot, value)
        } else {
            Err(CommandError::WrongType)
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
    fn add_value_by_str(
        &mut self,
        key: &str,
        value: &str,
        multiplier: i64,
    ) -> Result<i64, CommandError> {
        match value.parse::<i64>() {
            Ok(integer) => self.add_value(key, integer * multiplier),
            Err(_) => Err(CommandError::InvalidInteger),
        }
    }

    fn add_value(&mut self, key: &str, val: i64) -> Result<i64, CommandError> {
        if let Some(RedisValue::String(current_value)) = self.data.get_mut(key) {
            match current_value.parse::<i64>() {
                Ok(integer) => {
                    let new_integer = integer + val;
                    *current_value = RedisString::new(new_integer.to_string());
                    Ok(new_integer)
                }
                Err(_) => Err(CommandError::InvalidInteger),
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
