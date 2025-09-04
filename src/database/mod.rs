// Database module for Rudis
// In-memory data store implementation

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
pub type SharedDatabase = Arc<Mutex<Database>>;
pub struct Database {
    data: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
        }
    }
    pub fn new_shared() -> SharedDatabase {
        Arc::new(Mutex::new(Self::new()))
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn del(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }
}
