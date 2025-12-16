use super::{Database, HashOp, RedisValue};
use crate::commands::{CommandError, Result};
use crate::data_structures::RedisHash;

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
                RedisValue::Hash(existing_hash) => Ok(existing_hash
                    .flatten()
                    .map(|s| s.clone())
                    .collect::<Vec<String>>()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hkeys(&self, hash: &str) -> Result<Vec<String>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash
                    .keys()
                    .map(|s| s.clone())
                    .collect::<Vec<String>>()),
                _ => Err(CommandError::WrongType),
            },
            None => Ok(Vec::new()), // Empty array for non-existent keys
        }
    }

    fn hvals(&self, hash: &str) -> Result<Vec<String>> {
        match self.current_data().get(hash) {
            Some(entry) => match entry.value() {
                RedisValue::Hash(existing_hash) => Ok(existing_hash
                    .values()
                    .map(|s| s.clone())
                    .collect::<Vec<String>>()),
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
