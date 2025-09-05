// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{database::SharedDatabase, networking::resp::RespValue};

#[derive(Debug)]
pub enum Command {
    // Connection Commands
    Ping(Option<String>),
    // String Commands
    Get(String),
    Set(String, String),
    Del(Vec<String>),
    Incr(String),
    Decr(String),
    IncrBy(String, String),
    DecrBy(String, String),
    Append(String, String),
    Strlen(String),
    MGet(Vec<String>),
    MSet(Vec<(String, String)>),
    // Hash Commands
    HSet(String, String, String),
    HGet(String, String),
    HDel(Vec<(String, String)>),
    HGetAll(String),
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

    // Helper function for commands with key, field, and value
    fn parse_key_field_value_command(
        elements: &[RespValue],
        expected_len: usize,
    ) -> Option<(String, String, String)> {
        if elements.len() == expected_len {
            let key = Self::extract_bulk_string(&elements[1])?;
            let field = Self::extract_bulk_string(&elements[2])?;
            let value = Self::extract_bulk_string(&elements[3])?;
            Some((key, field, value))
        } else {
            None
        }
    }

    // Helper function for commands with multiple keys
    fn parse_keys_command(elements: &[RespValue], min_required_len: usize) -> Option<Vec<String>> {
        if elements.len() >= min_required_len {
            Self::extract_bulk_strings(&elements[1..])
        } else {
            None
        }
    }

    // Helper function for commands with multiple key-value pairs
    fn parse_keys_values_command(
        elements: &[RespValue],
        min_required_len: usize,
    ) -> Option<Vec<(String, String)>> {
        if elements.len() >= min_required_len && elements.len() % 2 == 1 {
            Self::extract_key_value_strings(&elements[1..])
        } else {
            None
        }
    }

    // Helper function to extract key-value pairs from bulk strings
    fn extract_key_value_strings(elements: &[RespValue]) -> Option<Vec<(String, String)>> {
        elements
            .chunks(2)
            .into_iter()
            .map(|value| {
                if value.len() == 2 {
                    if let (RespValue::BulkString(Some(key)), RespValue::BulkString(Some(val))) =
                        (&value[0], &value[1])
                    {
                        Some((key.clone(), val.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect::<Option<Vec<_>>>()
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
                    "DEL" => Self::parse_keys_command(elements, 2).map(Command::Del),
                    "INCR" => Self::parse_single_key_command(elements, 2).map(Command::Incr),
                    "DECR" => Self::parse_single_key_command(elements, 2).map(Command::Decr),
                    "INCRBY" => Self::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::IncrBy(k, v)),
                    "DECRBY" => Self::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::DecrBy(k, v)),
                    "APPEND" => Self::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::Append(k, v)),
                    "STRLEN" => {
                        Self::parse_single_key_command(elements, 2).map(|key| Command::Strlen(key))
                    }
                    "MGET" => Self::parse_keys_command(elements, 2).map(Command::MGet),
                    "MSET" => Self::parse_keys_values_command(elements, 3).map(Command::MSet),
                    "HSET" => Self::parse_key_field_value_command(elements, 4)
                        .map(|(k, f, v)| Command::HSet(k, f, v)),
                    "HGET" => {
                        Self::parse_key_value_command(elements, 3).map(|(k, f)| Command::HGet(k, f))
                    }
                    "HDEL" => Self::parse_keys_values_command(elements, 3).map(Command::HDel),
                    "HGETALL" => Self::parse_single_key_command(elements, 2).map(Command::HGetAll),
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
    fn format_array(elements: Vec<String>) -> String {
        let mut result = format!("*{}\r\n", elements.len());
        for element in elements {
            result.push_str(&element);
        }
        result
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
            Self::Strlen(key) => Self::format_integer(db_guard.str_len(key) as i64),
            Self::MGet(keys) => Self::format_array(
                keys.iter()
                    .map(|key| match db_guard.get(key) {
                        Some(value) => format!("${}\r\n{}\r\n", value.len(), value),
                        None => "$-1\r\n".to_string(),
                    })
                    .collect::<Vec<String>>(),
            ),
            Self::MSet(key_values) => {
                key_values
                    .iter()
                    .for_each(|(key, value)| db_guard.set(key.to_string(), value.to_string()));
                Self::format_simple_string("OK")
            }
            Self::HSet(hash, field, value) => match db_guard.hset(hash, field, value) {
                Ok(result) => Self::format_integer(result),
                Err(e) => Self::format_error(e),
            },
            Self::HGet(hash, field) => match db_guard.hget(hash, field) {
                Ok(Some(result)) => Self::format_bulk_string(&result),
                Ok(None) => Self::format_null(),
                Err(e) => Self::format_error(e),
            },
            Self::HDel(key_values) => Self::format_integer(
                key_values
                    .iter()
                    .filter(|(h, f)| db_guard.hdel(h, f))
                    .count() as i64,
            ),
            Self::HGetAll(key) => match db_guard.hget_all(key) {
                Ok(value) => Self::format_array(
                    value
                        .into_iter()
                        .map(|v| Self::format_bulk_string(&v))
                        .collect::<Vec<String>>(),
                ),
                Err(e) => Self::format_error(e),
            },
        }
    }
}
