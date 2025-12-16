use super::{Database, RedisValue};
use crate::commands::{CommandError, Result};
use crate::data_structures::RedisHash;
use crate::database::traits::HashOp;
use bytes::Bytes;

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
