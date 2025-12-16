use super::{Database, ListOp, RedisValue};
use crate::commands::{CommandError, Result};

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
                        Ok(list
                            .range(start, end)
                            .into_iter()
                            .map(|s| s.clone())
                            .collect())
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
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::WrongType) // Or OK if key doesn't exist? Redis returns OK.
        }
    }

    fn lset(&self, key: &str, index: &str, value: String) -> Result<()> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                if let Ok(index) = index.parse() {
                    if list.set(index, value) {
                        Ok(())
                    } else {
                        Err(CommandError::IndexOutOfRange)
                    }
                } else {
                    Err(CommandError::InvalidInteger)
                }
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Err(CommandError::NotFound)
        }
    }

    fn linsert(&self, key: &str, ord: &str, pivot: &str, value: String) -> Result<i64> {
        let data = self.current_data();
        if let Some(mut entry) = data.get_mut(key) {
            if let RedisValue::List(list) = entry.value_mut() {
                let before = match ord.to_uppercase().as_str() {
                    "BEFORE" => true,
                    "AFTER" => false,
                    _ => return Err(CommandError::SyntaxError),
                };
                Ok(list.insert(before, pivot, value) as i64)
            } else {
                Err(CommandError::WrongType)
            }
        } else {
            Ok(0)
        }
    }
}
