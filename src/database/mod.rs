// Database module for Rudis
// In-memory data store implementation

use crate::commands::{CommandError, Result};
use crate::data_structures::{
    RedisHash, RedisList, RedisSet, RedisSortedSet, RedisString,
};
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use dashmap::DashMap;
use regex::Regex;
use parking_lot::Mutex;

// Type definitions  
pub type SharedDatabase = Arc<Database>;

#[derive(Debug)]
enum RedisValue {
    String(RedisString),
    Hash(RedisHash),
    List(RedisList),
    Set(RedisSet),
    SortedSet(RedisSortedSet),
}

#[derive(Debug)]
pub struct Database {
    data: HashMap<u8, DashMap<String, RedisValue>>,
    data_expiration_time: HashMap<u8, DashMap<String, SystemTime>>,
    current_db: Mutex<u8>,
}

// Traits
pub trait StringOp {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&self, key: &str, value: String);
    fn del(&self, keys: &Vec<String>) -> usize;
    fn incr(&self, key: &str) -> Result<i64>;
    fn decr(&self, key: &str) -> Result<i64>;
    fn incr_by(&self, key: &str, value: &str) -> Result<i64>;
    fn decr_by(&self, key: &str, value: &str) -> Result<i64>;
    fn append(&self, key: &str, value: &str) -> usize;
    fn str_len(&self, key: &str) -> usize;
}

pub trait HashOp {
    fn hset(&self, hash: &str, field: &str, value: &str) -> Result<i64>;
    fn hget(&self, hash: &str, field: &str) -> Result<Option<String>>;
    fn hdel(&self, hash: &str, field: &str) -> bool;
    fn hdel_multiple(&self, hash: &str, fields: &[String]) -> usize;
    fn hget_all(&self, hash: &str) -> Result<Vec<String>>;
    fn hkeys(&self, hash: &str) -> Result<Vec<String>>;
    fn hvals(&self, hash: &str) -> Result<Vec<String>>;
    fn hlen(&self, hash: &str) -> Result<usize>;
    fn hexists(&self, hash: &str, field: &str) -> Result<bool>;
    fn hincrby(&self, hash: &str, field: &str, value: &str) -> Result<i64>;
    fn hincrbyfloat(&self, hash: &str, field: &str, value: &str) -> Result<f64>;
}

pub trait ListOp {
    fn lpush(&self, key: &str, values: &[String]) -> usize;
    fn rpush(&self, key: &str, values: &[String]) -> usize;
    fn lpop(&self, key: &str) -> Option<String>;
    fn rpop(&self, key: &str) -> Option<String>;
    fn llen(&self, key: &str) -> usize;
    fn lindex(&self, key: &str, index: &str) -> Option<String>;
    fn lrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<String>>;
    fn ltrim(&self, key: &str, start: &str, end: &str) -> Result<()>;
    fn lset(&self, key: &str, index: &str, value: String) -> Result<()>;
    fn linsert(&self, key: &str, ord: &str, pivot: &str, value: String) -> Result<i64>;
}
pub trait SetOp {
    fn sadd(&self, key: &str, values: &[String]) -> usize;
    fn srem(&self, key: &str, values: &[String]) -> usize;
    fn smembers(&self, key: &str) -> Result<Vec<String>>;
    fn scard(&self, key: &str) -> usize;
    fn sismember(&self, key: &str, member: &str) -> bool;
    fn sinter(&self, keys: &[String]) -> Result<Vec<String>>;
    fn sunion(&self, keys: &[String]) -> Result<Vec<String>>;
    fn sdiff(&self, keys: &[String]) -> Result<Vec<String>>;
}
pub trait SortedSetOp {
    fn zadd(&self, key: &str, pair: &[(String, String)]) -> usize;
    fn zrem(&self, key: &str, values: &[String]) -> usize;
    fn zrange(&self, key: &str, start: &str, stop: &str) -> Result<Vec<String>>;
    fn zrange_by_score(&self, key: &str, min: &str, max: &str) -> Result<Vec<String>>;
    fn zcard(&self, key: &str) -> usize;
    fn zscore(&self, key: &str, member: &str) -> Option<f64>;
    fn zrank(&self, key: &str, member: &str) -> Option<usize>;
}
pub trait KeyOp {
    fn exist(&self, keys: &[String]) -> usize;
    fn expire(&self, key: &str, seconds: &str) -> Result<()>;
    fn ttl(&self, key: &str) -> i64;
    fn keys(&self, pattern: &str) -> Result<Vec<String>>;
    fn flush_all(&self) -> bool;
    fn flush_db(&self) -> bool;
    fn select(&self, db: u8);
}

impl KeyOp for Database {
    fn exist(&self, keys: &[String]) -> usize {
        keys.iter()
            .filter(|key| self.current_data().contains_key(*key))
            .count()
    }

    fn expire(&self, key: &str, seconds: &str) -> Result<()> {
        if let Ok(secs) = seconds.parse() {
            if let Some(new_time) = SystemTime::now().checked_add(Duration::from_secs(secs)) {
                let exp_map = self.current_expiration();
                exp_map.insert(key.to_owned(), new_time);
                Ok(())
            } else {
                Err(CommandError::InvalidRange)
            }
        } else {
            Err(CommandError::InvalidInteger)
        }
    }

    fn ttl(&self, key: &str) -> i64 {
        let exp_map = self.current_expiration();
        if let Some(entry) = exp_map.get(key) {
            if let Ok(duration) = entry.value().duration_since(SystemTime::now()) {
                duration.as_secs() as i64
            } else {
                -2 // expired
            }
        } else {
            -1 // no expiration
        }
    }

    fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let data = self.current_data();
        let keys: Vec<String> = data.iter().map(|entry| entry.key().clone()).collect();
        if pattern == "*" {
            Ok(keys)
        } else {
            // Simple pattern matching with regex
            let pattern = pattern.replace("*", ".*");
            match Regex::new(&pattern) {
                Ok(re) => Ok(keys.into_iter().filter(|k| re.is_match(k)).collect()),
                Err(_) => Err(CommandError::InvalidPattern),
            }
        }
    }

    fn flush_all(&self) -> bool {
        self.current_data().clear();
        self.current_expiration().clear();
        true
    }

    fn flush_db(&self) -> bool {
        for i in 0..self.data.len() {
            if let Some(db_data) = self.data.get(&(i as u8)) {
                db_data.clear();
            }
            if let Some(db_exp) = self.data_expiration_time.get(&(i as u8)) {
                db_exp.clear();
            }
        }
        true
    }

    fn select(&self, db: u8) {
        if db as usize >= self.data.len() {
            return;
        }
        *self.current_db.lock() = db;
    }
}
impl SortedSetOp for Database {
    fn zadd(&self, key: &str, pair: &[(String, String)]) -> usize {
        let data = self.current_data();
        if let Some(mut value_ref) = data.get_mut(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value_mut() {
                pair.iter()
                    .filter(|(k, v)| {
                        if let Ok(v) = v.parse() {
                            sorted_set.zadd(k.to_owned(), v);
                            true
                        } else {
                            false
                        }
                    })
                    .count()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn zrem(&self, key: &str, values: &[String]) -> usize {
        let data = self.current_data();
        if let Some(mut value_ref) = data.get_mut(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value_mut() {
                values.iter().filter(|k| sorted_set.zrem(k)).count()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn zrange(&self, key: &str, start: &str, stop: &str) -> Result<Vec<String>> {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                if let Ok(start) = start.parse() {
                    if let Ok(stop) = stop.parse() {
                        Ok(sorted_set.zrange(start, stop).into_iter().map(|s| s.clone()).collect())
                    } else {
                        Err(CommandError::InvalidFloat)
                    }
                } else {
                    Err(CommandError::InvalidFloat)
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn zrange_by_score(&self, key: &str, min: &str, max: &str) -> Result<Vec<String>> {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                if let Ok(min) = min.parse() {
                    if let Ok(max) = max.parse() {
                        Ok(sorted_set.zrange_by_score(min, max).into_iter().map(|s| s.clone()).collect())
                    } else {
                        Err(CommandError::InvalidFloat)
                    }
                } else {
                    Err(CommandError::InvalidFloat)
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn zcard(&self, key: &str) -> usize {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                sorted_set.zcard()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                sorted_set.zscore(member)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn zrank(&self, key: &str, member: &str) -> Option<usize> {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                sorted_set.zrank(member)
            } else {
                None
            }
        } else {
            None
        }
    }
}
impl SetOp for Database {
    fn sadd(&self, key: &str, values: &[String]) -> usize {
        let data = self.current_data();
        match data.get_mut(key) {
            Some(mut entry) => {
                match entry.value_mut() {
                    RedisValue::Set(set) => {
                        values.iter().filter(|val| set.sadd((*val).clone())).count()
                    }
                    _ => {
                        // Key exists but is wrong type
                        0 // Should probably return error, but for now match existing behavior
                    }
                }
            }
            None => {
                // Key doesn't exist, create new set
                let mut new_set = RedisSet::new();
                let added = values
                    .iter()
                    .filter(|val| new_set.sadd((*val).clone()))
                    .count();
                data.insert(key.to_string(), RedisValue::Set(new_set));
                added
            }
        }
    }

    fn srem(&self, key: &str, values: &[String]) -> usize {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::Set(set) = entry.value_mut() {
                values.iter().filter(|val| set.srem(val)).count()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn smembers(&self, key: &str) -> Result<Vec<String>> {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::Set(set) = entry.value() {
                Ok(set.smembers().into_iter().map(|s| s.clone()).collect())
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn scard(&self, key: &str) -> usize {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::Set(set) = entry.value() {
                set.scard()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn sismember(&self, key: &str, member: &str) -> bool {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::Set(set) = entry.value() {
                set.sismember(member)
            } else {
                false
            }
        } else {
            false
        }
    }

    fn sinter(&self, keys: &[String]) -> Result<Vec<String>> {
        let mut res = Vec::new();
        for key in keys {
            if let Some(entry) = self.current_data().get(key) {
                if let RedisValue::Set(set) = entry.value() {
                    for ele in set.smembers() {
                        res.push(ele.clone());
                    }
                } else {
                    return Err(CommandError::WrongType);
                }
            } else {
                return Err(CommandError::WrongType);
            }
        }
        Ok(res)
    }

    fn sunion(&self, keys: &[String]) -> Result<Vec<String>> {
        match self.sinter(keys) {
            Ok(res) => Ok(res
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()),
            Err(e) => Err(e),
        }
    }

    fn sdiff(&self, keys: &[String]) -> Result<Vec<String>> {
        match self.smembers(&keys[0]) {
            Ok(res) => {
                let mut res: HashSet<_> = res.into_iter().collect();
                match self.sunion(&keys[1..]) {
                    Ok(members) => {
                        for member in members {
                            res.remove(&member);
                        }
                        Ok(res.into_iter().collect())
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }
}
impl StringOp for Database {
    fn get(&self, key: &str) -> Option<String> {
        if let Some(value_ref) = self.current_data().get(key) {
            if let RedisValue::String(value) = value_ref.value() {
                Some(value.get().to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn set(&self, key: &str, value: String) {
        let data = self.current_data();
        match data.get_mut(key) {
            Some(mut value_ref) => {
                match value_ref.value_mut() {
                    RedisValue::String(val) => val.set(value),
                    _ => {
                        // Key exists but is wrong type - overwrite it (Redis behavior)
                        drop(value_ref);
                        data.insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
                    }
                }
            }
            None => {
                data.insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            }
        }
    }

    fn del(&self, keys: &Vec<String>) -> usize {
        let data = self.current_data();
        keys.into_iter()
            .filter(|key| data.remove(*key).is_some())
            .count()
    }

    fn incr(&self, key: &str) -> Result<i64> {
        self.add_value(key, 1)
    }

    fn decr(&self, key: &str) -> Result<i64> {
        self.add_value(key, -1)
    }

    fn incr_by(&self, key: &str, value: &str) -> Result<i64> {
        self.add_value_by_str(key, value, 1)
    }

    fn decr_by(&self, key: &str, value: &str) -> Result<i64> {
        self.add_value_by_str(key, value, -1)
    }

    fn append(&self, key: &str, value: &str) -> usize {
        let data = self.current_data();
        if let Some(mut value_ref) = data.get_mut(key) {
            if let RedisValue::String(current_value) = value_ref.value_mut() {
                current_value.push_str(value);
                current_value.len()
            } else {
                drop(value_ref);
                data.insert(
                    key.to_string(),
                    RedisValue::String(RedisString::new(value.to_string())),
                );
                value.len()
            }
        } else {
            data.insert(
                key.to_string(),
                RedisValue::String(RedisString::new(value.to_string())),
            );
            value.len()
        }
    }

    fn str_len(&self, key: &str) -> usize {
        if let Some(value_ref) = self.current_data().get(key) {
            if let RedisValue::String(value) = value_ref.value() {
                value.len()
            } else {
                0
            }
        } else {
            0
        }
    }
}

impl HashOp for Database {
    fn hset(&self, hash: &str, field: &str, value: &str) -> Result<i64> {
        let data = self.current_data();
        match data.get_mut(hash) {
            Some(mut entry) => {
                match entry.value_mut() {
                    RedisValue::Hash(existing_hash) => {
                        // Hash exists, update/add the field
                        Ok(existing_hash.hset(field.to_string(), value.to_string()))
                    }
                    _ => {
                        // Key exists but is not a hash
                        Err(CommandError::WrongType)
                    }
                }
            }
            None => {
                // Key doesn't exist, create new hash
                let mut new_hash = RedisHash::new();
                new_hash.hset(field.to_string(), value.to_string());
                data.insert(hash.to_string(), RedisValue::Hash(new_hash));
                Ok(1) // New field was added
            }
        }
    }

    fn hget(&self, hash: &str, field: &str) -> Result<Option<String>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash.hget(field).map(|s| s.clone())),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(None),
        }
    }

    fn hdel(&self, hash: &str, field: &str) -> bool {
        if let Some(mut entry) = self.current_data().get_mut(hash) {
            if let RedisValue::Hash(existing_hash) = entry.value_mut() {
                existing_hash.hdel(field)
            } else {
                false
            }
        } else {
            false
        }
    }

    fn hdel_multiple(&self, hash: &str, fields: &[String]) -> usize {
        if let Some(mut entry) = self.current_data().get_mut(hash) {
            if let RedisValue::Hash(existing_hash) = entry.value_mut() {
                fields
                    .iter()
                    .filter(|field| existing_hash.hdel(field))
                    .count()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn hget_all(&self, hash: &str) -> Result<Vec<String>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => {
                    Ok(existing_hash.flatten().map(|s| s.clone()).collect::<Vec<String>>())
                }
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hkeys(&self, hash: &str) -> Result<Vec<String>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => {
                    Ok(existing_hash.keys().map(|s| s.clone()).collect::<Vec<String>>())
                }
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hvals(&self, hash: &str) -> Result<Vec<String>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => {
                    Ok(existing_hash.values().map(|s| s.clone()).collect::<Vec<String>>())
                }
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hlen(&self, hash: &str) -> Result<usize> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash.len()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(0), // 0 for non-existent keys
        }
    }

    fn hexists(&self, hash: &str, field: &str) -> Result<bool> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash.hexists(field)),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(false), // false for non-existent keys
        }
    }

    fn hincrby(&self, hash: &str, field: &str, value: &str) -> Result<i64> {
        let data = self.current_data();
        match data.get_mut(hash) {
            Some(mut entry) => match entry.value_mut() {
                RedisValue::Hash(existing_hash) => match value.parse::<i64>() {
                    Ok(integer) => match existing_hash.hincrby(field, integer) {
                        Ok(result) => Ok(result),
                        Err(_) => Err(CommandError::InvalidInteger),
                    },
                    Err(_) => Err(CommandError::InvalidInteger),
                },
                _ => Err(CommandError::WrongType),
            },
            None => {
                // Key doesn't exist, create new hash with field set to value
                let mut new_hash = RedisHash::new();
                match value.parse::<i64>() {
                    Ok(integer) => match new_hash.hincrby(field, integer) {
                        Ok(result) => {
                            data.insert(hash.to_string(), RedisValue::Hash(new_hash));
                            Ok(result)
                        }
                        Err(_) => Err(CommandError::InvalidInteger),
                    },
                    Err(_) => Err(CommandError::InvalidInteger),
                }
            }
        }
    }

    fn hincrbyfloat(&self, hash: &str, field: &str, value: &str) -> Result<f64> {
        let data = self.current_data();
        match data.get_mut(hash) {
            Some(mut entry) => match entry.value_mut() {
                RedisValue::Hash(existing_hash) => match value.parse::<f64>() {
                    Ok(float_val) => match existing_hash.hincrbyfloat(field, float_val) {
                        Ok(result) => Ok(result),
                        Err(_) => Err(CommandError::InvalidFloat),
                    },
                    Err(_) => Err(CommandError::InvalidFloat),
                },
                _ => Err(CommandError::WrongType),
            },
            None => {
                // Key doesn't exist, create new hash with field set to value
                let mut new_hash = RedisHash::new();
                match value.parse::<f64>() {
                    Ok(float_val) => match new_hash.hincrbyfloat(field, float_val) {
                        Ok(result) => {
                            data.insert(hash.to_string(), RedisValue::Hash(new_hash));
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
    fn lpush(&self, key: &str, values: &[String]) -> usize {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                values.iter().for_each(|value| list.lpush(value.to_owned()));
                values.len()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn rpush(&self, key: &str, values: &[String]) -> usize {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                values.iter().for_each(|value| list.rpush(value.to_owned()));
                values.len()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn lpop(&self, key: &str) -> Option<String> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                list.lpop()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn rpop(&self, key: &str) -> Option<String> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                list.rpop()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn llen(&self, key: &str) -> usize {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::List(list) = entry.value() {
                list.len()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn lindex(&self, key: &str, index: &str) -> Option<String> {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::List(list) = entry.value() {
                match index.parse() {
                    Ok(integer) => list.index(integer).map(|s| s.clone()),
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    fn lrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<String>> {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::List(list) = entry.value() {
                if let Ok(start) = start.parse() {
                    if let Ok(end) = end.parse() {
                        Ok(list.range(start, end).into_iter().map(|s| s.clone()).collect())
                    } else {
                        Err(CommandError::InvalidInteger)
                    }
                } else {
                    Err(CommandError::InvalidInteger)
                }
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    fn ltrim(&self, key: &str, start: &str, end: &str) -> Result<()> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
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
        } else {
            Ok(())
        }
    }

    fn lset(&self, key: &str, index: &str, value: String) -> Result<()> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                if let Ok(index) = index.parse() {
                    Ok(list.set(index, value))
                } else {
                    Err(CommandError::InvalidInteger)
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::KeyNotFound)
        }
    }

    fn linsert(&self, key: &str, ord: &str, pivot: &str, value: String) -> Result<i64> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                list.insert(ord, pivot, value)
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }
}
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
    pub fn data_type(&self, key: &str) -> &str {
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
    fn current_data(&self) -> &DashMap<String, RedisValue> {
        let db = *self.current_db.lock();
        self.data.get(&db).unwrap()
    }

    fn current_expiration(&self) -> &DashMap<String, SystemTime> {
        let db = *self.current_db.lock();
        self.data_expiration_time.get(&db).unwrap()
    }

    // Consolidated helper for incr_by/decr_by operations
    fn add_value_by_str(&self, key: &str, value: &str, multiplier: i64) -> Result<i64> {
        match value.parse::<i64>() {
            Ok(integer) => self.add_value(key, integer * multiplier),
            Err(_) => Err(CommandError::InvalidInteger),
        }
    }

    fn add_value(&self, key: &str, val: i64) -> Result<i64> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::String(current_value) = entry.value_mut() {
                match current_value.parse::<i64>() {
                    Ok(integer) => {
                        let new_integer = integer + val;
                        *current_value = RedisString::new(new_integer.to_string());
                        Ok(new_integer)
                    }
                    Err(_) => Err(CommandError::InvalidInteger),
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            data.insert(
                key.to_string(),
                RedisValue::String(RedisString::new(val.to_string())),
            );
            Ok(val)
        }
    }
}
