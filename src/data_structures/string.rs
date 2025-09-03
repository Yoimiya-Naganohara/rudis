// String data structure for Rudis

pub struct RedisString {
    value: String,
}

impl RedisString {
    pub fn new(value: String) -> Self {
        RedisString { value }
    }

    pub fn get(&self) -> &str {
        &self.value
    }

    pub fn set(&mut self, value: String) {
        self.value = value;
    }
}
