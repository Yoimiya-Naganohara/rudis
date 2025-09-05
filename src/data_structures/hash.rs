// Hash data structure for Rudis

use std::collections::HashMap;

pub struct RedisHash {
    fields: HashMap<String, String>,
}

impl RedisHash {
    pub fn new() -> Self {
        RedisHash { fields: HashMap::new() }
    }

    pub fn hset(&mut self, field: String, value: String) -> i64 {
        let is_new = !self.fields.contains_key(&field);
        self.fields.insert(field, value);
        if is_new { 1 } else { 0 }
    }

    pub fn hget(&self, field: &str) -> Option<&String> {
        self.fields.get(field)
    }

    pub fn hdel(&mut self, field: &str) -> bool {
        self.fields.remove(field).is_some()
    }
    
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }
    pub fn values(&self)->impl Iterator<Item = &String>{
        self.fields.values()
    }
    pub fn flatten(&self) -> impl Iterator<Item = &String> {
        self.fields.iter().flat_map(|(k, v)| [k, v])
    }
}
