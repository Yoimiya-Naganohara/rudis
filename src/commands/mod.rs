// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{database::{SharedDatabase, StringOp, HashOp}, networking::resp::RespValue};

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
    HKeys(String),HVals(String)
}
mod command_helper {
    use crate::networking::resp::RespValue;

    // Helper function to extract BulkString value
    pub fn extract_bulk_string(resp_value: &RespValue) -> Option<String> {
        match resp_value {
            RespValue::BulkString(Some(s)) => Some(s.clone()),
            _ => None,
        }
    }

    // Helper function to extract multiple BulkString values
    pub fn extract_bulk_strings(elements: &[RespValue]) -> Option<Vec<String>> {
        elements.iter().map(extract_bulk_string).collect()
    }

    // Helper function for commands with single key
    pub fn parse_single_key_command(elements: &[RespValue], expected_len: usize) -> Option<String> {
        if elements.len() == expected_len {
            extract_bulk_string(&elements[1])
        } else {
            None
        }
    }

    // Helper function for commands with key and value
    pub fn parse_key_value_command(
        elements: &[RespValue],
        expected_len: usize,
    ) -> Option<(String, String)> {
        if elements.len() == expected_len {
            let key = extract_bulk_string(&elements[1])?;
            let value = extract_bulk_string(&elements[2])?;
            Some((key, value))
        } else {
            None
        }
    }

    // Helper function for commands with key, field, and value
    pub fn parse_key_field_value_command(
        elements: &[RespValue],
        expected_len: usize,
    ) -> Option<(String, String, String)> {
        if elements.len() == expected_len {
            let key = extract_bulk_string(&elements[1])?;
            let field = extract_bulk_string(&elements[2])?;
            let value = extract_bulk_string(&elements[3])?;
            Some((key, field, value))
        } else {
            None
        }
    }

    // Helper function for commands with multiple keys
    pub fn parse_keys_command(elements: &[RespValue], min_required_len: usize) -> Option<Vec<String>> {
        if elements.len() >= min_required_len {
            extract_bulk_strings(&elements[1..])
        } else {
            None
        }
    }

    // Helper function for commands with multiple key-value pairs
    pub fn parse_keys_values_command(
        elements: &[RespValue],
        min_required_len: usize,
    ) -> Option<Vec<(String, String)>> {
        if elements.len() >= min_required_len && elements.len() % 2 == 1 {
            extract_key_value_strings(&elements[1..])
        } else {
            None
        }
    }

    // Helper function to extract key-value pairs from bulk strings
    pub fn extract_key_value_strings(elements: &[RespValue]) -> Option<Vec<(String, String)>> {
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

    // Helper functions for response formatting
    pub fn format_integer(value: i64) -> String {
        format!(":{}\r\n", value)
    }

    pub fn format_array(elements: Vec<String>) -> String {
        let mut result = format!("*{}\r\n", elements.len());
        for element in elements {
            result.push_str(&element);
        }
        result
    }

    pub fn format_error(error: String) -> String {
        format!("-ERR {}\r\n", error)
    }

    pub fn format_bulk_string(value: &str) -> String {
        format!("${}\r\n{}\r\n", value.len(), value)
    }

    pub fn format_null() -> String {
        "$-1\r\n".to_string()
    }

    pub fn format_simple_string(value: &str) -> String {
        format!("+{}\r\n", value)
    }

    pub fn format_hash_response(value: Vec<&String>) -> String {
        let mut result = format!("*{}\r\n", value.len());
        for item in value {
            result.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
        }
        result
    }
}

impl Command {
   
    pub fn parse(resp_value: &RespValue) -> Option<Self> {
        match resp_value {
            RespValue::Array(elements) if !elements.is_empty() => {
                let command_name = command_helper::extract_bulk_string(&elements[0])?.to_uppercase();

                match command_name.as_str() {
                    "PING" => match elements.len() {
                        1 => Some(Command::Ping(None)),
                        2 => Some(Command::Ping(command_helper::extract_bulk_string(&elements[1]))),
                        _ => None,
                    },
                    "GET" => command_helper::parse_single_key_command(elements, 2).map(Command::Get),
                    "SET" => {
                        command_helper::parse_key_value_command(elements, 3).map(|(k, v)| Command::Set(k, v))
                    }
                    "DEL" => command_helper::parse_keys_command(elements, 2).map(Command::Del),
                    "INCR" => command_helper::parse_single_key_command(elements, 2).map(Command::Incr),
                    "DECR" => command_helper::parse_single_key_command(elements, 2).map(Command::Decr),
                    "INCRBY" => command_helper::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::IncrBy(k, v)),
                    "DECRBY" => command_helper::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::DecrBy(k, v)),
                    "APPEND" => command_helper::parse_key_value_command(elements, 3)
                        .map(|(k, v)| Command::Append(k, v)),
                    "STRLEN" => {
                        command_helper::parse_single_key_command(elements, 2).map(|key| Command::Strlen(key))
                    }
                    "MGET" => command_helper::parse_keys_command(elements, 2).map(Command::MGet),
                    "MSET" => command_helper::parse_keys_values_command(elements, 3).map(Command::MSet),
                    "HSET" => command_helper::parse_key_field_value_command(elements, 4)
                        .map(|(k, f, v)| Command::HSet(k, f, v)),
                    "HGET" => {
                        command_helper::parse_key_value_command(elements, 3).map(|(k, f)| Command::HGet(k, f))
                    }
                    "HDEL" => command_helper::parse_keys_values_command(elements, 3).map(Command::HDel),
                    "HGETALL" => command_helper::parse_single_key_command(elements, 2).map(Command::HGetAll),
                    "HKEYS"=>command_helper::parse_single_key_command(elements, 2).map(Command::HKeys),
                    "HVALS"=>command_helper::parse_single_key_command(elements, 2).map(Command::HVals),
                    _ => None,
                }
            }
            _ => None,
        }
    }

  

    pub async fn execute(&self, db: &SharedDatabase) -> String {
        let mut db_guard = db.lock().await;
        match self {
            Self::Ping(None) => command_helper::format_simple_string("PONG"),
            Self::Ping(Some(msg)) => command_helper::format_simple_string(msg),
            Self::Get(key) => match db_guard.get(key) {
                Some(value) => command_helper::format_bulk_string(value),
                None => command_helper::format_null(),
            },
            Self::Set(key, value) => {
                db_guard.set(key, value.clone());
                command_helper::format_simple_string("OK")
            }
            Self::Del(keys) => {
                
                command_helper::format_integer(db_guard.del(keys)as i64)
            }
            Self::Incr(key) => match db_guard.incr(key) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::Decr(key) => match db_guard.decr(key) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::IncrBy(key, value) => match db_guard.incr_by(key, value) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::DecrBy(key, value) => match db_guard.decr_by(key, value) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::Append(key, value) => command_helper::format_integer(db_guard.append(key, value) as i64),
            Self::Strlen(key) => command_helper::format_integer(db_guard.str_len(key) as i64),
            Self::MGet(keys) => command_helper::format_array(
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
                    .for_each(|(key, value)| db_guard.set(key, value.clone()));
                command_helper::format_simple_string("OK")
            }
            Self::HSet(hash, field, value) => match db_guard.hset(hash, field, value) {
                Ok(result) => command_helper::format_integer(result),
                Err(e) => command_helper::format_error(e),
            },
            Self::HGet(hash, field) => match db_guard.hget(hash, field) {
                Ok(Some(result)) => command_helper::format_bulk_string(&result),
                Ok(None) => command_helper::format_null(),
                Err(e) => command_helper::format_error(e),
            },
            Self::HDel(key_values) => command_helper::format_integer(
                key_values
                
                    .iter()
                    .filter(|(h, f)| db_guard.hdel(h, f))
                    .count() as i64,
            ),
            Self::HGetAll(key) => match db_guard.hget_all(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },Self::HKeys(key)=>match db_guard.hkeys(key) {
                Ok(value) => {command_helper::format_hash_response(value)},
                Err(e ) => {command_helper::format_error(e)},
            }Self::HVals(key)=>match db_guard.hvals(key) {
                Ok(value) => {command_helper::format_hash_response(value)},
                Err(e ) => {command_helper::format_error(e)},
            }
        }
    }
}
