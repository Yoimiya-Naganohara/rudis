use super::{Database, KeyOp};
use crate::commands::{CommandError, Result};
use regex::Regex;
use std::time::{Duration, SystemTime};

impl KeyOp for Database {
    fn exist(&self, keys: &[String]) -> usize {
        keys.iter()
            .filter(|key| self.current_data().contains_key(*key))
            .count()
    }

    fn expire(&self, key: &str, seconds: &str) -> Result<()> {
        if let Ok(secs) = seconds.parse() {
            if let Some(new_time) = SystemTime::now().checked_add(Duration::from_secs(secs)) {
                let exp_map = self.current_expiration();
                exp_map.insert(key.to_owned(), new_time);
                Ok(())
            } else {
                Err(CommandError::InvalidRange)
            }
        } else {
            Err(CommandError::InvalidInteger)
        }
    }

    fn ttl(&self, key: &str) -> i64 {
        let exp_map = self.current_expiration();
        if let Some(entry) = exp_map.get(key) {
            if let Ok(duration) = entry.value().duration_since(SystemTime::now()) {
                duration.as_secs() as i64
            } else {
                -2 // expired
            }
        } else {
            -1 // no expiration
        }
    }

    fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let data = self.current_data();
        let keys: Vec<String> = data.iter().map(|entry| entry.key().clone()).collect();
        if pattern == "*" {
            Ok(keys)
        } else {
            // Simple pattern matching with regex
            let pattern = pattern.replace("*", ".*");
            match Regex::new(&pattern) {
                Ok(re) => Ok(keys.into_iter().filter(|k| re.is_match(k)).collect()),
                Err(_) => Err(CommandError::InvalidPattern),
            }
        }
    }

    fn flush_all(&self) -> bool {
        self.current_data().clear();
        self.current_expiration().clear();
        true
    }

    fn flush_db(&self) -> bool {
        for i in 0..self.data.len() {
            if let Some(db_data) = self.data.get(&(i as u8)) {
                db_data.clear();
            }
            if let Some(db_exp) = self.data_expiration_time.get(&(i as u8)) {
                db_exp.clear();
            }
        }
        true
    }

    fn select(&self, db: u8) {
        if db as usize >= self.data.len() {
            return;
        }
        *self.current_db.lock() = db;
    }
}
