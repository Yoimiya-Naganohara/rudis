use super::{Database, RedisValue};
use crate::commands::{CommandError, Result};
use crate::data_structures::RedisSortedSet;
use crate::database::traits::SortedSetOp;
use bytes::Bytes;

impl SortedSetOp for Database {
    fn zadd(&self, key: &Bytes, pair: &[(f64, Bytes)]) -> usize {
        let data = self.current_data();
        match data.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                RedisValue::SortedSet(sorted_set) => pair
                    .iter()
                    .map(|(score, member)| sorted_set.zadd(member.clone(), *score))
                    .sum(),
                _ => 0,
            },
            None => {
                // Key doesn't exist, create a new sorted set
                let mut new_set = RedisSortedSet::new();
                let added = pair
                    .iter()
                    .map(|(score, member)| new_set.zadd(member.clone(), *score))
                    .sum();
                data.insert(key.clone(), RedisValue::SortedSet(new_set));
                added
            }
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
