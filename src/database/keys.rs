use super::{Database};
use crate::commands::{CommandError, Result};
use crate::database::traits::KeyOp;
use bytes::Bytes;
use regex::Regex;
use std::time::{Duration, SystemTime};

impl KeyOp for Database {
    fn exist(&self, keys: &[Bytes]) -> usize {
        keys.iter()
            .filter(|key| self.current_data().contains_key(*key))
            .count()
    }

    fn expire(&self, key: &Bytes, seconds: u64) -> Result<()> {
        if let Some(new_time) = SystemTime::now().checked_add(Duration::from_secs(seconds)) {
            let exp_map = self.current_expiration();
            exp_map.insert(key.clone(), new_time);
            Ok(())
        } else {
            Err(CommandError::InvalidRange)
        }
    }

    fn ttl(&self, key: &Bytes) -> i64 {
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

    fn keys(&self, pattern: &Bytes) -> Result<Vec<Bytes>> {
        let data = self.current_data();
        let keys: Vec<Bytes> = data.iter().map(|entry| entry.key().clone()).collect();
        // Basic glob matching for *
        // Ideally use a glob library or regex on String if we assume keys are strings.
        // Redis keys are binary, so regex is tricky if not UTF-8.
        // But typical usage assumes UTF-8 compatible patterns.
        // TODO: Use true glob matcher. For now, assume pattern is UTF-8 regex-like if not "*".
        if pattern.as_ref() == b"*" {
            Ok(keys)
        } else {
            // Fallback to converting to string for regex matching (lossy)
            let pattern_str = String::from_utf8_lossy(pattern);
            let pattern_str = pattern_str.replace("*", ".*");
            match Regex::new(&pattern_str) {
                Ok(re) => Ok(keys
                    .into_iter()
                    .filter(|k| {
                        let ks = String::from_utf8_lossy(k);
                        re.is_match(&ks)
                    })
                    .collect()),
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
