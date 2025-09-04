// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{database::SharedDatabase, networking::resp::RespValue};

#[derive(Debug)]
pub enum Command {
    Ping(Option<String>),
    Get(String),
    Set(String, String),
    Del(Vec<String>),
    Incr(String),
    Decr(String),
    IncrBy(String, String),
    DecrBy(String, String),
    Append(String, String),
}

impl Command {
    // Helper function to extract BulkString value
    fn extract_bulk_string(resp_value: &RespValue) -> Option<String> {
        match resp_value {
            RespValue::BulkString(Some(s)) => Some(s.clone()),
            _ => None,
        }
    }

    // Helper function to extract multiple BulkString values
    fn extract_bulk_strings(elements: &[RespValue]) -> Option<Vec<String>> {
        elements.iter().map(Self::extract_bulk_string).collect()
    }

    // Helper function for commands with single key
    fn parse_single_key_command(elements: &[RespValue], expected_len: usize) -> Option<String> {
        if elements.len() == expected_len {
            Self::extract_bulk_string(&elements[1])
        } else {
            None
        }
    }

    // Helper function for commands with key and value
    fn parse_key_value_command(
        elements: &[RespValue],
        expected_len: usize,
    ) -> Option<(String, String)> {
        if elements.len() == expected_len {
            let key = Self::extract_bulk_string(&elements[1])?;
            let value = Self::extract_bulk_string(&elements[2])?;
            Some((key, value))
        } else {
            None
        }
    }

    pub fn parse(resp_value: &RespValue) -> Option<Self> {
        match resp_value {
            RespValue::Array(elements) if !elements.is_empty() => {
                let command_name = Self::extract_bulk_string(&elements[0])?.to_uppercase();

                match command_name.as_str() {
                    "PING" => match elements.len() {
                        1 => Some(Command::Ping(None)),
                        2 => Some(Command::Ping(Self::extract_bulk_string(&elements[1]))),
                        _ => None,
                    },
                    "GET" => Self::parse_single_key_command(elements, 2).map(Command::Get),
                    "SET" => {
                        Self::parse_key_value_command(elements, 3).map(|(k, v)| Command::Set(k, v))
                    }
                    "DEL" => {
                        if elements.len() >= 2 {
                            Self::extract_bulk_strings(&elements[1..]).map(Command::Del)
                        } else {
                            None
                        }
                    }
                    "INCR" => Self::parse_single_key_command(elements, 2).map(Command::Incr),
                    "DECR" => Self::parse_single_key_command(elements, 2).map(Command::Decr),
                    "INCRBY" => Self::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::IncrBy(k, v)),
                    "DECRBY" => Self::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::DecrBy(k, v)),
                    "APPEND" => Self::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::Append(k, v)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    // Helper functions for response formatting
    fn format_integer(value: i64) -> String {
        format!(":{}\r\n", value)
    }

    fn format_error(error: String) -> String {
        format!("-ERR {}\r\n", error)
    }

    fn format_bulk_string(value: &str) -> String {
        format!("${}\r\n{}\r\n", value.len(), value)
    }

    fn format_null() -> String {
        "$-1\r\n".to_string()
    }

    fn format_simple_string(value: &str) -> String {
        format!("+{}\r\n", value)
    }

    pub async fn execute(&self, db: &SharedDatabase) -> String {
        let mut db_guard = db.lock().await;
        match self {
            Self::Ping(None) => Self::format_simple_string("PONG"),
            Self::Ping(Some(msg)) => Self::format_simple_string(msg),
            Self::Get(key) => match db_guard.get(key) {
                Some(value) => Self::format_bulk_string(value),
                None => Self::format_null(),
            },
            Self::Set(key, value) => {
                db_guard.set(key.clone(), value.clone());
                Self::format_simple_string("OK")
            }
            Self::Del(keys) => {
                let deleted_count = keys.iter().filter(|key| db_guard.del(key)).count() as i64;
                Self::format_integer(deleted_count)
            }
            Self::Incr(key) => match db_guard.incr(key) {
                Ok(value) => Self::format_integer(value),
                Err(e) => Self::format_error(e),
            },
            Self::Decr(key) => match db_guard.decr(key) {
                Ok(value) => Self::format_integer(value),
                Err(e) => Self::format_error(e),
            },
            Self::IncrBy(key, value) => match db_guard.incr_by(key, value) {
                Ok(value) => Self::format_integer(value),
                Err(e) => Self::format_error(e),
            },
            Self::DecrBy(key, value) => match db_guard.decr_by(key, value) {
                Ok(value) => Self::format_integer(value),
                Err(e) => Self::format_error(e),
            },
            Self::Append(key, value) => Self::format_integer(db_guard.append(key, value) as i64),
        }
    }
}
