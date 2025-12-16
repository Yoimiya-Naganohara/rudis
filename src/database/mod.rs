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

// Traits
pub trait StringOp {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&self, key: &Bytes, value: Bytes);
    fn del(&self, keys: &[Bytes]) -> usize;
    fn incr(&self, key: &Bytes) -> Result<i64>;
    fn decr(&self, key: &Bytes) -> Result<i64>;
    fn incr_by(&self, key: &Bytes, value: Bytes) -> Result<i64>;
    fn decr_by(&self, key: &Bytes, value: Bytes) -> Result<i64>;
    fn append(&self, key: &Bytes, value: Bytes) -> usize;
    fn str_len(&self, key: &Bytes) -> usize;
}

pub trait HashOp {
    fn hset(&self, hash: &Bytes, field: Bytes, value: Bytes) -> Result<i64>;
    fn hget(&self, hash: &Bytes, field: &Bytes) -> Result<Option<Bytes>>;
    fn hdel(&self, hash: &Bytes, field: &Bytes) -> bool;
    fn hdel_multiple(&self, hash: &Bytes, fields: &[Bytes]) -> usize;
    fn hget_all(&self, hash: &Bytes) -> Result<Vec<Bytes>>;
    fn hkeys(&self, hash: &Bytes) -> Result<Vec<Bytes>>;
    fn hvals(&self, hash: &Bytes) -> Result<Vec<Bytes>>;
    fn hlen(&self, hash: &Bytes) -> Result<usize>;
    fn hexists(&self, hash: &Bytes, field: &Bytes) -> Result<bool>;
    fn hincrby(&self, hash: &Bytes, field: &Bytes, value: i64) -> Result<i64>;
    fn hincrbyfloat(&self, hash: &Bytes, field: &Bytes, value: f64) -> Result<f64>;
}

pub trait ListOp {
    fn lpush(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn rpush(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn lpop(&self, key: &Bytes) -> Option<Bytes>;
    fn rpop(&self, key: &Bytes) -> Option<Bytes>;
    fn llen(&self, key: &Bytes) -> usize;
    fn lindex(&self, key: &Bytes, index: i64) -> Option<Bytes>;
    fn lrange(&self, key: &Bytes, start: i64, end: i64) -> Result<Vec<Bytes>>;
    fn ltrim(&self, key: &Bytes, start: i64, end: i64) -> Result<()>;
    fn lset(&self, key: &Bytes, index: i64, value: Bytes) -> Result<()>;
    fn linsert(&self, key: &Bytes, ord: &str, pivot: &Bytes, value: Bytes) -> Result<i64>;
}
pub trait SetOp {
    fn sadd(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn srem(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>>;
    fn scard(&self, key: &Bytes) -> usize;
    fn sismember(&self, key: &Bytes, member: &Bytes) -> bool;
    fn sinter(&self, keys: &[Bytes]) -> Result<Vec<Bytes>>;
    fn sunion(&self, keys: &[Bytes]) -> Result<Vec<Bytes>>;
    fn sdiff(&self, keys: &[Bytes]) -> Result<Vec<Bytes>>;
}
pub trait SortedSetOp {
    fn zadd(&self, key: &Bytes, pair: &[(f64, Bytes)]) -> usize;
    fn zrem(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn zrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>>;
    fn zrange_by_score(&self, key: &Bytes, min: f64, max: f64) -> Result<Vec<Bytes>>;
    fn zcard(&self, key: &Bytes) -> usize;
    fn zscore(&self, key: &Bytes, member: &Bytes) -> Option<f64>;
    fn zrank(&self, key: &Bytes, member: &Bytes) -> Option<usize>;
}
pub trait KeyOp {
    fn exist(&self, keys: &[Bytes]) -> usize;
    fn expire(&self, key: &Bytes, seconds: u64) -> Result<()>;
    fn ttl(&self, key: &Bytes) -> i64;
    fn keys(&self, pattern: &Bytes) -> Result<Vec<Bytes>>;
    fn flush_all(&self) -> bool;
    fn flush_db(&self) -> bool;
    fn select(&self, db: u8);
}

impl KeyOp for Database {
    fn exist(&self, keys: &[Bytes]) -> usize {
        keys.iter()
            .filter(|key| self.current_data().contains_key(*key))
            .count()
    }

    fn expire(&self, key: &Bytes, seconds: u64) -> Result<()> {
        if let Some(new_time) = SystemTime::now().checked_add(Duration::from_secs(seconds)) {
            let exp_map = self.current_expiration();
            exp_map.insert(key.clone(), new_time);
            Ok(())
        } else {
            Err(CommandError::InvalidRange)
        }
    }

    fn ttl(&self, key: &Bytes) -> i64 {
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

    fn keys(&self, pattern: &Bytes) -> Result<Vec<Bytes>> {
        let data = self.current_data();
        let keys: Vec<Bytes> = data.iter().map(|entry| entry.key().clone()).collect();
        // Basic glob matching for *
        // Ideally use a glob library or regex on String if we assume keys are strings.
        // Redis keys are binary, so regex is tricky if not UTF-8.
        // But typical usage assumes UTF-8 compatible patterns.
        // TODO: Use true glob matcher. For now, assume pattern is UTF-8 regex-like if not "*".
        if pattern.as_ref() == b"*" {
            Ok(keys)
        } else {
            // Fallback to converting to string for regex matching (lossy)
            let pattern_str = String::from_utf8_lossy(pattern);
            let pattern_str = pattern_str.replace("*", ".*");
            match Regex::new(&pattern_str) {
                Ok(re) => Ok(keys
                    .into_iter()
                    .filter(|k| {
                        let ks = String::from_utf8_lossy(k);
                        re.is_match(&ks)
                    })
                    .collect()),
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
    fn zadd(&self, key: &Bytes, pair: &[(f64, Bytes)]) -> usize {
        let data = self.current_data();
        if let Some(mut value_ref) = data.get_mut(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value_mut() {
                pair.iter()
                    .map(|(score, member)| {
                        sorted_set.zadd(member.clone(), *score);
                        // zadd always returns void in our struct?
                        // Redis returns added count. Our struct needs update if we want exact count.
                        // But for now, we just do it.
                        // Let's assume we can't easily track *added* vs *updated* without changing zadd signature.
                        // We'll count all.
                        1
                    })
                    .sum()
            } else {
                0
            }
        } else {
            0
        }
    }

    fn zrem(&self, key: &Bytes, values: &[Bytes]) -> usize {
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

    fn zrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                Ok(sorted_set.zrange(start, stop))
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn zrange_by_score(&self, key: &Bytes, min: f64, max: f64) -> Result<Vec<Bytes>> {
        let data = self.current_data();
        if let Some(value_ref) = data.get(key) {
            if let RedisValue::SortedSet(sorted_set) = value_ref.value() {
                Ok(sorted_set.zrange_by_score(min, max))
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn zcard(&self, key: &Bytes) -> usize {
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

    fn zscore(&self, key: &Bytes, member: &Bytes) -> Option<f64> {
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

    fn zrank(&self, key: &Bytes, member: &Bytes) -> Option<usize> {
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
    fn sadd(&self, key: &Bytes, values: &[Bytes]) -> usize {
        let data = self.current_data();
        match data.get_mut(key) {
            Some(mut entry) => {
                match entry.value_mut() {
                    RedisValue::Set(set) => {
                        values.iter().filter(|val| set.sadd((*val).clone())).count()
                    }
                    _ => {
                        // Key exists but is wrong type
                        0
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
                data.insert(key.clone(), RedisValue::Set(new_set));
                added
            }
        }
    }

    fn srem(&self, key: &Bytes, values: &[Bytes]) -> usize {
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

    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>> {
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

    fn scard(&self, key: &Bytes) -> usize {
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

    fn sismember(&self, key: &Bytes, member: &Bytes) -> bool {
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

    fn sinter(&self, keys: &[Bytes]) -> Result<Vec<Bytes>> {
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

    fn sunion(&self, keys: &[Bytes]) -> Result<Vec<Bytes>> {
        match self.sinter(keys) {
            Ok(res) => Ok(res
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()),
            Err(e) => Err(e),
        }
    }

    fn sdiff(&self, keys: &[Bytes]) -> Result<Vec<Bytes>> {
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
    fn get(&self, key: &Bytes) -> Option<Bytes> {
        if let Some(value_ref) = self.current_data().get(key) {
            if let RedisValue::String(value) = value_ref.value() {
                Some(value.get())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn set(&self, key: &Bytes, value: Bytes) {
        let data = self.current_data();
        match data.get_mut(key) {
            Some(mut value_ref) => {
                match value_ref.value_mut() {
                    RedisValue::String(val) => val.set(value),
                    _ => {
                        // Key exists but is wrong type - overwrite it (Redis behavior)
                        drop(value_ref);
                        data.insert(key.clone(), RedisValue::String(RedisString::new(value)));
                    }
                }
            }
            None => {
                data.insert(key.clone(), RedisValue::String(RedisString::new(value)));
            }
        }
    }

    fn del(&self, keys: &[Bytes]) -> usize {
        let data = self.current_data();
        keys.iter()
            .filter(|key| data.remove(*key).is_some())
            .count()
    }

    fn incr(&self, key: &Bytes) -> Result<i64> {
        self.add_value(key, 1)
    }

    fn decr(&self, key: &Bytes) -> Result<i64> {
        self.add_value(key, -1)
    }

    fn incr_by(&self, key: &Bytes, value: Bytes) -> Result<i64> {
        // Convert value (Bytes) to i64
        let s = std::str::from_utf8(&value).map_err(|_| CommandError::InvalidInteger)?;
        let val = s.parse::<i64>().map_err(|_| CommandError::InvalidInteger)?;
        self.add_value(key, val)
    }

    fn decr_by(&self, key: &Bytes, value: Bytes) -> Result<i64> {
        let s = std::str::from_utf8(&value).map_err(|_| CommandError::InvalidInteger)?;
        let val = s.parse::<i64>().map_err(|_| CommandError::InvalidInteger)?;
        self.add_value(key, -val)
    }

    fn append(&self, key: &Bytes, value: Bytes) -> usize {
        let data = self.current_data();
        if let Some(mut value_ref) = data.get_mut(key) {
            if let RedisValue::String(current_value) = value_ref.value_mut() {
                current_value.append(value);
                current_value.len()
            } else {
                drop(value_ref);
                let cloned_val = value.clone();
                data.insert(key.clone(), RedisValue::String(RedisString::new(value)));
                cloned_val.len()
            }
        } else {
            let cloned_val = value.clone();
            data.insert(key.clone(), RedisValue::String(RedisString::new(value)));
            cloned_val.len()
        }
    }

    fn str_len(&self, key: &Bytes) -> usize {
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
    fn hset(&self, hash: &Bytes, field: Bytes, value: Bytes) -> Result<i64> {
        let data = self.current_data();
        match data.get_mut(hash) {
            Some(mut entry) => {
                match entry.value_mut() {
                    RedisValue::Hash(existing_hash) => {
                        // Hash exists, update/add the field
                        Ok(existing_hash.hset(field, value))
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
                new_hash.hset(field, value);
                data.insert(hash.clone(), RedisValue::Hash(new_hash));
                Ok(1) // New field was added
            }
        }
    }

    fn hget(&self, hash: &Bytes, field: &Bytes) -> Result<Option<Bytes>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash.hget(field).map(|s| s.clone())),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(None),
        }
    }

    fn hdel(&self, hash: &Bytes, field: &Bytes) -> bool {
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

    fn hdel_multiple(&self, hash: &Bytes, fields: &[Bytes]) -> usize {
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

    fn hget_all(&self, hash: &Bytes) -> Result<Vec<Bytes>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash
                    .flatten()
                    .map(|s| s.clone())
                    .collect::<Vec<Bytes>>()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hkeys(&self, hash: &Bytes) -> Result<Vec<Bytes>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash
                    .keys()
                    .map(|s| s.clone())
                    .collect::<Vec<Bytes>>()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hvals(&self, hash: &Bytes) -> Result<Vec<Bytes>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash
                    .values()
                    .map(|s| s.clone())
                    .collect::<Vec<Bytes>>()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hlen(&self, hash: &Bytes) -> Result<usize> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash.len()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(0), // 0 for non-existent keys
        }
    }

    fn hexists(&self, hash: &Bytes, field: &Bytes) -> Result<bool> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash.hexists(field)),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(false), // false for non-existent keys
        }
    }

    fn hincrby(&self, hash: &Bytes, field: &Bytes, value: i64) -> Result<i64> {
        let data = self.current_data();
        match data.get_mut(hash) {
            Some(mut entry) => match entry.value_mut() {
                RedisValue::Hash(existing_hash) => match existing_hash.hincrby(field, value) {
                    Ok(result) => Ok(result),
                    Err(_) => Err(CommandError::InvalidInteger),
                },
                _ => Err(CommandError::WrongType),
            },
            None => {
                // Key doesn't exist, create new hash with field set to value
                let mut new_hash = RedisHash::new();
                match new_hash.hincrby(field, value) {
                    Ok(result) => {
                        data.insert(hash.clone(), RedisValue::Hash(new_hash));
                        Ok(result)
                    }
                    Err(_) => Err(CommandError::InvalidInteger),
                }
            }
        }
    }

    fn hincrbyfloat(&self, hash: &Bytes, field: &Bytes, value: f64) -> Result<f64> {
        let data = self.current_data();
        match data.get_mut(hash) {
            Some(mut entry) => match entry.value_mut() {
                RedisValue::Hash(existing_hash) => match existing_hash.hincrbyfloat(field, value) {
                    Ok(result) => Ok(result),
                    Err(_) => Err(CommandError::InvalidFloat),
                },
                _ => Err(CommandError::WrongType),
            },
            None => {
                // Key doesn't exist, create new hash with field set to value
                let mut new_hash = RedisHash::new();
                match new_hash.hincrbyfloat(field, value) {
                    Ok(result) => {
                        data.insert(hash.clone(), RedisValue::Hash(new_hash));
                        Ok(result)
                    }
                    Err(_) => Err(CommandError::InvalidFloat),
                }
            }
        }
    }
}

impl ListOp for Database {
    fn lpush(&self, key: &Bytes, values: &[Bytes]) -> usize {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                values.iter().for_each(|value| list.lpush(value.clone()));
                list.len()
            } else {
                0
            }
        } else {
            // Create new list
            let mut new_list = RedisList::new();
            values
                .iter()
                .for_each(|value| new_list.lpush(value.clone()));
            let len = new_list.len();
            data.insert(key.clone(), RedisValue::List(new_list));
            len
        }
    }

    fn rpush(&self, key: &Bytes, values: &[Bytes]) -> usize {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                values.iter().for_each(|value| list.rpush(value.clone()));
                list.len()
            } else {
                0
            }
        } else {
            let mut new_list = RedisList::new();
            values
                .iter()
                .for_each(|value| new_list.rpush(value.clone()));
            let len = new_list.len();
            data.insert(key.clone(), RedisValue::List(new_list));
            len
        }
    }

    fn lpop(&self, key: &Bytes) -> Option<Bytes> {
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

    fn rpop(&self, key: &Bytes) -> Option<Bytes> {
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

    fn llen(&self, key: &Bytes) -> usize {
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

    fn lindex(&self, key: &Bytes, index: i64) -> Option<Bytes> {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::List(list) = entry.value() {
                list.index(index).map(|s| s.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn lrange(&self, key: &Bytes, start: i64, end: i64) -> Result<Vec<Bytes>> {
        if let Some(entry) = self.current_data().get(key) {
            if let RedisValue::List(list) = entry.value() {
                Ok(list.range(start, end))
            } else {
                Ok(Vec::new())
            }
        } else {
            Ok(Vec::new())
        }
    }

    fn ltrim(&self, key: &Bytes, start: i64, end: i64) -> Result<()> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                list.trim(start, end);
                Ok(())
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key mismatch or doesn't exist - usually ok or error depending on command
            Ok(())
        }
    }

    fn lset(&self, key: &Bytes, index: i64, value: Bytes) -> Result<()> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                let len = list.len() as i64;
                if index >= len || index < -len {
                    return Err(CommandError::IndexOutOfRange);
                }
                list.set(index, value);
                Ok(())
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::KeyNotFound)
        }
    }

    fn linsert(&self, key: &Bytes, ord: &str, pivot: &Bytes, value: Bytes) -> Result<i64> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                list.insert(ord, pivot, value)
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            // Key not found = 0? Or error?
            // LINSERT on missing key does nothing and returns 0.
            Ok(0)
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
