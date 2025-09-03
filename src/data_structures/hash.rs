// Hash data structure for Rudis

use std::collections::HashMap;

pub struct RedisHash {
    fields: HashMap<String, String>,
}

impl RedisHash {
    pub fn new() -> Self {
        RedisHash { fields: HashMap::new() }
    }

    pub fn hset(&mut self, field: String, value: String) {
        self.fields.insert(field, value);
    }

    pub fn hget(&self, field: &str) -> Option<&String> {
        self.fields.get(field)
    }

    pub fn hdel(&mut self, field: &str) -> bool {
        self.fields.remove(field).is_some()
    }
}
