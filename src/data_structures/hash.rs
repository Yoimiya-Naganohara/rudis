// Hash data structure for Rudis

use bytes::Bytes;
use std::collections::{hash_map::Entry, HashMap};

#[derive(Debug)]
pub struct RedisHash {
    fields: HashMap<Bytes, Bytes>,
}

impl Default for RedisHash {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisHash {
    pub fn new() -> Self {
        RedisHash {
            fields: HashMap::new(),
        }
    }

    pub fn hset(&mut self, field: Bytes, value: Bytes) -> i64 {
        // Single lookup: insert replaces an existing value in place
        match self.fields.entry(field) {
            Entry::Occupied(mut entry) => {
                entry.insert(value);
                0
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
                1
            }
        }
    }

    pub fn hget(&self, field: &Bytes) -> Option<&Bytes> {
        self.fields.get(field)
    }

    pub fn hdel(&mut self, field: &Bytes) -> bool {
        self.fields.remove(field).is_some()
    }

    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.fields.keys()
    }
    pub fn values(&self) -> impl Iterator<Item = &Bytes> {
        self.fields.values()
    }
    pub fn flatten(&self) -> impl Iterator<Item = &Bytes> {
        self.fields.iter().flat_map(|(k, v)| [k, v])
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn hexists(&self, field: &Bytes) -> bool {
        self.fields.contains_key(field)
    }

    pub fn hincrby(
        &mut self,
        field: &Bytes,
        value: i64,
    ) -> Result<i64, crate::commands::CommandError> {
        let current_value = if let Some(existing) = self.fields.get(field) {
            let s = std::str::from_utf8(existing)
                .map_err(|_| crate::commands::CommandError::InvalidInteger)?;
            s.parse::<i64>()
                .map_err(|_| crate::commands::CommandError::InvalidInteger)?
        } else {
            0
        };

        let new_value = current_value + value;
        self.fields
            .insert(field.clone(), Bytes::from(new_value.to_string()));
        Ok(new_value)
    }

    pub fn hincrbyfloat(
        &mut self,
        field: &Bytes,
        value: f64,
    ) -> Result<f64, crate::commands::CommandError> {
        let current_value = if let Some(existing) = self.fields.get(field) {
            let s = std::str::from_utf8(existing)
                .map_err(|_| crate::commands::CommandError::InvalidFloat)?;
            s.parse::<f64>()
                .map_err(|_| crate::commands::CommandError::InvalidFloat)?
        } else {
            0.0
        };

        let new_value = current_value + value;
        self.fields
            .insert(field.clone(), Bytes::from(new_value.to_string()));
        Ok(new_value)
    }
}
