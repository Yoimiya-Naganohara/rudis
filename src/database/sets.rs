use super::{Database, RedisValue};
use crate::commands::{CommandError, Result};
use crate::data_structures::RedisSet;
use crate::database::traits::SetOp;
use bytes::Bytes;
use std::collections::HashSet;

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
