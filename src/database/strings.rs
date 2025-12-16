use super::{Database, RedisValue, StringOp};
use crate::commands::Result;
use crate::data_structures::RedisString;

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

    fn del(&self, keys: &[String]) -> usize {
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
