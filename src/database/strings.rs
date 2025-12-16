use super::{Database, RedisValue};
use crate::commands::{CommandError, Result};
use crate::data_structures::RedisString;
use crate::database::traits::StringOp;
use bytes::Bytes;

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
