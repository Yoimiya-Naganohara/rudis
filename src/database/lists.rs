use super::{Database, RedisValue};
use crate::commands::{CommandError, Result};
use crate::data_structures::RedisList;
use crate::database::traits::ListOp;
use bytes::Bytes;

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
