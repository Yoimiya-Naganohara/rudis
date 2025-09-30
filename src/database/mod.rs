// Database module for Rudis
// In-memory data store implementation

use crate::commands::CommandError;
use crate::data_structures::{
    list, set, sorted_set, RedisHash, RedisList, RedisSet, RedisSortedSet, RedisString,
};
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, sync::Arc};
use regex::Regex;
use tokio::sync::Mutex;

// Type definitions
pub type SharedDatabase = Arc<Mutex<Database>>;

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
    data: HashMap<u8, HashMap<String, RedisValue>>,
    data_expiration_time: HashMap<u8, HashMap<String, SystemTime>>,
    current_db: u8,
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
    fn smembers(&self, key: &str) -> Result<Vec<&String>, CommandError>;
    fn scard(&mut self, key: &str) -> usize;
    fn sismember(&self, key: &str, member: &str) -> bool;
    fn sinter(&self, keys: &[String]) -> Result<Vec<&String>, CommandError>;
    fn sunion(&self, keys: &[String]) -> Result<Vec<&String>, CommandError>;
    fn sdiff(&self, keys: &[String]) -> Result<Vec<&String>, CommandError>;
}
pub trait SortedSetOp {
    fn zadd(&mut self, key: &str, pair: &[(String, String)]) -> usize;
    fn zrem(&mut self, key: &str, values: &[String]) -> usize;
    fn zrange(&mut self, key: &str, start: &str, stop: &str) -> Result<Vec<&String>, CommandError>;
    fn zrange_by_score(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<Vec<&String>, CommandError>;
    fn zcard(&mut self, key: &str) -> usize;
    fn zscore(&mut self, key: &str, member: &str) -> Option<f64>;
    fn zrank(&mut self, key: &str, member: &str) -> Option<usize>;
}
pub trait KeyOp {
    fn exist(&self, keys: &[String]) -> usize;
    fn expire(&mut self, key: &str, seconds: &str) -> Result<(), CommandError>;
    fn ttl(&mut self, key: &str) -> i64;
    fn keys(&self, pattern: &str) -> Result<Vec<&String>,CommandError>;
    fn flush_all(&mut self) -> bool;
    fn flush_db(&mut self) -> bool;
    fn select(&mut self, db: u8);
}

impl KeyOp for Database {
    fn exist(&self, keys: &[String]) -> usize {
        keys.iter().filter(|key| self.current_data().contains_key(*key)).count()
    }

    fn expire(&mut self, key: &str, seconds: &str) -> Result<(), CommandError> {
        if let Some(time) = self.current_expiration_mut().get_mut(key) {
            if let Ok(secs) = seconds.parse() {
                if let Some(new_time) = SystemTime::now().checked_add(Duration::from_secs(secs)) {
                    *time = new_time;
                    Ok(())
                } else {
                    Err(CommandError::InvalidRange)
                }
            } else {
                Err(CommandError::InvalidInteger)
            }
        } else {
            Err(CommandError::KeyNotFound)
        }
    }

    fn ttl(&mut self, key: &str) -> i64 {
        let exp_map = self.current_expiration();
        if let Some(time) = exp_map.get(key) {
            if let Ok(duration) = time.duration_since(SystemTime::now()) {
                duration.as_secs() as i64
            } else {
                -2 // expired
            }
        } else {
            -1 // no expiration
        }
    }

    fn keys(&self, pattern: &str) -> Result<Vec<&String>,CommandError> {
      if let Ok(regex) = Regex::new(pattern) {
        Ok(self.current_data().keys().filter(|k|regex.is_match(k)).collect())
       }else {
           Err(CommandError::SyntaxError)
       }
    }

    fn flush_all(&mut self) -> bool {
        self.current_data_mut().clear();
        self.current_expiration_mut().clear();
        true
    }

    fn flush_db(&mut self) -> bool {
       for i in 0..self.data.len() {
           self.data.get_mut(&(i as u8)).unwrap().clear();
           self.data_expiration_time.get_mut(&(i as u8)).unwrap().clear();
       }
        true
    }
    
    fn select(&mut self, db: u8) {
        if db as usize >= self.data.len() {
            return ;
        }
        self.current_db=db;
    }
}
impl SortedSetOp for Database {
    fn zadd(&mut self, key: &str, pair: &[(String, String)]) -> usize {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get_mut(key) {
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
    }

    fn zrem(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get_mut(key) {
            values.iter().filter(|k| sorted_set.zrem(k)).count()
        } else {
            0
        }
    }

    fn zrange(&mut self, key: &str, start: &str, stop: &str) -> Result<Vec<&String>, CommandError> {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get_mut(key) {
            if let Ok(start) = start.parse() {
                if let Ok(stop) = stop.parse() {
                    Ok(sorted_set.zrange(start, stop))
                } else {
                    Err(CommandError::InvalidFloat)
                }
            } else {
                Err(CommandError::InvalidFloat)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn zrange_by_score(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
    ) -> Result<Vec<&String>, CommandError> {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get(key) {
            if let Ok(min) = min.parse() {
                if let Ok(max) = max.parse() {
                    Ok(sorted_set.zrange_by_score(min, max))
                } else {
                    Err(CommandError::InvalidFloat)
                }
            } else {
                Err(CommandError::InvalidFloat)
            }
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn zcard(&mut self, key: &str) -> usize {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get(key) {
            sorted_set.zcard()
        } else {
            0
        }
    }

    fn zscore(&mut self, key: &str, member: &str) -> Option<f64> {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get(key) {
            sorted_set.zscore(member)
        } else {
            None
        }
    }

    fn zrank(&mut self, key: &str, member: &str) -> Option<usize> {
        if let Some(RedisValue::SortedSet(sorted_set)) = self.current_data_mut().get(key) {
            sorted_set.zrank(member)
        } else {
            None
        }
    }
}
impl SetOp for Database {
    fn sadd(&mut self, key: &str, values: &[String]) -> usize {
        match self.current_data_mut().get_mut(key) {
            Some(RedisValue::Set(set)) => {
                values.iter().filter(|val| set.sadd((*val).clone())).count()
            }
            Some(_) => {
                // Key exists but is wrong type
                0 // Should probably return error, but for now match existing behavior
            }
            None => {
                // Key doesn't exist, create new set
                let mut new_set = RedisSet::new();
                let added = values
                    .iter()
                    .filter(|val| new_set.sadd((*val).clone()))
                    .count();
                self.current_data_mut().insert(key.to_string(), RedisValue::Set(new_set));
                added
            }
        }
    }

    fn srem(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::Set(set)) = self.current_data_mut().get_mut(key) {
            values.iter().filter(|val| set.srem(val)).count()
        } else {
            0
        }
    }

    fn smembers(&self, key: &str) -> Result<Vec<&String>, CommandError> {
        if let Some(RedisValue::Set(set)) = self.current_data().get(key) {
            Ok(set.smembers())
        } else {
            Err(CommandError::WrongType)
        }
    }

    fn scard(&mut self, key: &str) -> usize {
        if let Some(RedisValue::Set(set)) = self.current_data_mut().get_mut(key) {
            set.scard()
        } else {
            0
        }
    }

    fn sismember(&self, key: &str, member: &str) -> bool {
        if let Some(RedisValue::Set(set)) = self.current_data().get(key) {
            set.sismember(member)
        } else {
            false
        }
    }

    fn sinter(&self, keys: &[String]) -> Result<Vec<&String>, CommandError> {
        let mut res = Vec::new();
        for key in keys {
            if let Some(RedisValue::Set(set)) = self.current_data().get(key) {
                for ele in set.smembers() {
                    res.push(ele);
                }
            } else {
                return Err(CommandError::WrongType);
            }
        }
        Ok(res)
    }

    fn sunion(&self, keys: &[String]) -> Result<Vec<&String>, CommandError> {
        match self.sinter(keys) {
            Ok(res) => Ok(res
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()),
            Err(e) => Err(e),
        }
    }

    fn sdiff(&self, keys: &[String]) -> Result<Vec<&String>, CommandError> {
        match self.smembers(&keys[0]) {
            Ok(res) => {
                let mut res: HashSet<_> = res.into_iter().collect();
                match self.sunion(&keys[1..]) {
                    Ok(members) => {
                        for member in members {
                            res.remove(member);
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
    fn get(&self, key: &str) -> Option<&str> {
        if let Some(RedisValue::String(value)) = self.current_data().get(key) {
            Some(value.get())
        } else {
            None
        }
    }

    fn set(&mut self, key: &str, value: String) {
        match self.current_data_mut().get_mut(key) {
            Some(RedisValue::String(val)) => val.set(value),
            Some(_) => {
                // Key exists but is wrong type - overwrite it (Redis behavior)
                self.current_data_mut()
                    .insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            }
            None => {
                self.current_data_mut()
                    .insert(key.to_owned(), RedisValue::String(RedisString::new(value)));
            }
        }
    }

    fn del(&mut self, keys: &Vec<String>) -> usize {
        keys.into_iter()
            .filter(|key| self.current_data_mut().remove(*key).is_some())
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
        if let Some(RedisValue::String(current_value)) = self.current_data_mut().get_mut(key) {
            current_value.push_str(value);
            current_value.len()
        } else {
            self.current_data_mut().insert(
                key.to_string(),
                RedisValue::String(RedisString::new(value.to_string())),
            );
            value.len()
        }
    }

    fn str_len(&self, key: &str) -> usize {
        if let Some(RedisValue::String(value)) = self.current_data().get(key) {
            value.len()
        } else {
            0
        }
    }
}

impl HashOp for Database {
    fn hset(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, CommandError> {
        match self.current_data_mut().get_mut(hash) {
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
                self.current_data_mut()
                    .insert(hash.to_string(), RedisValue::Hash(new_hash));
                Ok(1) // New field was added
            }
        }
    }

    fn hget(&self, hash: &str, field: &str) -> Result<Option<&String>, CommandError> {
        match self.current_data().get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.hget(field)),
            Some(_) => Err(CommandError::WrongType),
            None => Ok(None),
        }
    }

    fn hdel(&mut self, hash: &str, field: &str) -> bool {
        if let Some(RedisValue::Hash(existing_hash)) = self.current_data_mut().get_mut(hash) {
            existing_hash.hdel(field)
        } else {
            false
        }
    }

    fn hdel_multiple(&mut self, hash: &str, fields: &[String]) -> usize {
        if let Some(RedisValue::Hash(existing_hash)) = self.current_data_mut().get_mut(hash) {
            fields
                .iter()
                .filter(|field| existing_hash.hdel(field))
                .count()
        } else {
            0
        }
    }

    fn hget_all(&self, hash: &str) -> Result<Vec<&String>, CommandError> {
        match self.current_data().get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.flatten().collect::<Vec<&String>>())
            }
            Some(_) => Err(CommandError::WrongType),
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hkeys(&self, hash: &str) -> Result<Vec<&String>, CommandError> {
        match self.current_data().get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.keys().collect::<Vec<&String>>())
            }
            Some(_) => Err(CommandError::WrongType),
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hvals(&self, hash: &str) -> Result<Vec<&String>, CommandError> {
        match self.current_data().get(hash) {
            Some(RedisValue::Hash(existing_hash)) => {
                Ok(existing_hash.values().collect::<Vec<&String>>())
            }
            Some(_) => Err(CommandError::WrongType),
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hlen(&self, hash: &str) -> Result<usize, CommandError> {
        match self.current_data().get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.len()),
            Some(_) => Err(CommandError::WrongType),
            None => Ok(0), // 0 for non-existent keys
        }
    }

    fn hexists(&self, hash: &str, field: &str) -> Result<bool, CommandError> {
        match self.current_data().get(hash) {
            Some(RedisValue::Hash(existing_hash)) => Ok(existing_hash.hexists(field)),
            Some(_) => Err(CommandError::WrongType),
            None => Ok(false), // false for non-existent keys
        }
    }

    fn hincrby(&mut self, hash: &str, field: &str, value: &str) -> Result<i64, CommandError> {
        match self.current_data_mut().get_mut(hash) {
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
                            self.current_data_mut()
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
        match self.current_data_mut().get_mut(hash) {
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
                            self.current_data_mut()
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
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
            values.iter().for_each(|value| list.lpush(value.to_owned()));
            values.len()
        } else {
            0
        }
    }

    fn rpush(&mut self, key: &str, values: &[String]) -> usize {
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
            values.iter().for_each(|value| list.rpush(value.to_owned()));
            values.len()
        } else {
            0
        }
    }

    fn lpop(&mut self, key: &str) -> Option<String> {
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
            list.lpop()
        } else {
            None
        }
    }

    fn rpop(&mut self, key: &str) -> Option<String> {
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
            list.rpop()
        } else {
            None
        }
    }

    fn llen(&self, key: &str) -> usize {
        if let Some(RedisValue::List(list)) = self.current_data().get(key) {
            list.len()
        } else {
            0
        }
    }

    fn lindex(&self, key: &str, index: &str) -> Option<&String> {
        if let Some(RedisValue::List(list)) = self.current_data().get(key) {
            match index.parse() {
                Ok(integer) => list.index(integer),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    fn lrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<&String>, CommandError> {
        if let Some(RedisValue::List(list)) = self.current_data().get(key) {
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
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
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
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
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
        if let Some(RedisValue::List(list)) = self.current_data_mut().get_mut(key) {
            list.insert(ord, pivot, value)
        } else {
            Err(CommandError::WrongType)
        }
    }
}
impl Database {
    pub fn new(db_num:usize) -> Self {
        let mut data=HashMap::new();
        let mut data_expiration_time=HashMap::new();

        for i in 0..db_num {
            data.insert(i as u8, HashMap::new());
            data_expiration_time.insert(i as u8, HashMap::new());
        }
        Database {
            data,
            data_expiration_time,
            current_db: 0,
        }
    }
    pub fn new_shared(db_num:usize) -> SharedDatabase {
        Arc::new(Mutex::new(Self::new(db_num)))
    }
    fn current_data(&self) -> &HashMap<String, RedisValue> {
        self.data.get(&self.current_db).unwrap()
    }

    fn current_data_mut(&mut self) -> &mut HashMap<String, RedisValue> {
        self.data.entry(self.current_db).or_insert(HashMap::new())
    }

    fn current_expiration(&self) -> &HashMap<String, SystemTime> {
        self.data_expiration_time.get(&self.current_db).unwrap()
    }

    fn current_expiration_mut(&mut self) -> &mut HashMap<String, SystemTime> {
        self.data_expiration_time.entry(self.current_db).or_insert(HashMap::new())
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
        if let Some(RedisValue::String(current_value)) = self.current_data_mut().get_mut(key) {
            match current_value.parse::<i64>() {
                Ok(integer) => {
                    let new_integer = integer + val;
                    *current_value = RedisString::new(new_integer.to_string());
                    Ok(new_integer)
                }
                Err(_) => Err(CommandError::InvalidInteger),
            }
        } else {
            self.current_data_mut().insert(
                key.to_string(),
                RedisValue::String(RedisString::new(val.to_string())),
            );
            Ok(val)
        }
    }
}
