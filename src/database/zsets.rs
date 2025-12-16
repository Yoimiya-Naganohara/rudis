use super::{Database, RedisValue, SortedSetOp};
use crate::commands::{CommandError, Result};

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
                        Ok(sorted_set
                            .zrange(start, stop)
                            .into_iter()
                            .map(|s| s.clone())
                            .collect())
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
                        Ok(sorted_set
                            .zrange_by_score(min, max)
                            .into_iter()
                            .map(|s| s.clone())
                            .collect())
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
