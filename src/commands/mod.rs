// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{
    commands::command_helper::{format_array, format_bulk_string, format_error, format_integer, format_null, format_simple_string},
    database::{HashOp, ListOp, SharedDatabase, StringOp},
    networking::resp::RespValue,
};
mod errors;
pub use errors::*;
#[derive(Debug)]
pub enum Command {
    // Connection Commands
    Ping(Option<String>), // PING [message] - Test connection, optionally echo message

    // String Commands
    Get(String),                 // GET key - Get value of key
    Set(String, String),         // SET key value - Set key to hold string value
    Del(Vec<String>),            // DEL key [key ...] - Delete one or more keys
    Incr(String),                // INCR key - Increment integer value of key by 1
    Decr(String),                // DECR key - Decrement integer value of key by 1
    IncrBy(String, String), // INCRBY key increment - Increment integer value of key by increment
    DecrBy(String, String), // DECRBY key decrement - Decrement integer value of key by decrement
    Append(String, String), // APPEND key value - Append value to key
    Strlen(String),         // STRLEN key - Get length of string stored in key
    MGet(Vec<String>),      // MGET key [key ...] - Get values of multiple keys
    MSet(Vec<(String, String)>), // MSET key value [key value ...] - Set multiple keys to multiple values

    // Hash Commands
    HSet(String, String, String), // HSET key field value - Set field in hash stored at key to value
    HGet(String, String),         // HGET key field - Get value of field in hash stored at key
    HDel(String, Vec<String>),    // HDEL key field [field ...] - Delete one or more hash fields
    HGetAll(String),              // HGETALL key - Get all fields and values in hash
    HKeys(String),                // HKEYS key - Get all field names in hash
    HVals(String),                // HVALS key - Get all values in hash
    HLen(String),                 // HLEN key - Get number of fields in hash
    HExists(String, String),      // HEXISTS key field - Check if field exists in hash
    HIncrBy(String, String, String), // HINCRBY key field increment - Increment integer value of hash field
    HIncrByFloat(String, String, String), // HINCRBYFLOAT key field increment - Increment float value of hash field

    // List Commands
    LPush(String, Vec<String>), // LPUSH key element [element ...] - Insert elements at head of list
    RPush(String, Vec<String>), // RPUSH key element [element ...] - Insert elements at tail of list
    LPop(String),               // LPOP key - Remove and return first element of list
    RPop(String),               // RPOP key - Remove and return last element of list
    LLen(String),               // LLEN key - Get length of list
    LIndex(String, String),     // LINDEX key index - Get element at index in list
    LRange(String, String, String), // LRANGE key start stop - Get range of elements from list
    LTrim(String, String, String), // LTRIM key start stop - Trim list to specified range
    LSet(String, String, String), // LSET key index element - Set element at index in list
    LInsert(String, String, String, String), // LINSERT key BEFORE|AFTER pivot element - Insert element before/after pivot
}
pub mod command_helper {
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
    pub fn parse_keys_command(
        elements: &[RespValue],
        min_required_len: usize,
    ) -> Option<Vec<String>> {
        if elements.len() >= min_required_len {
            extract_bulk_strings(&elements[1..])
        } else {
            None
        }
    }

    // Helper function for commands with key and multiple fields
    pub fn parse_key_fields_command(
        elements: &[RespValue],
        min_required_len: usize,
    ) -> Option<(String, Vec<String>)> {
        if elements.len() >= min_required_len {
            let key = extract_bulk_string(&elements[1])?;
            let fields = extract_bulk_strings(&elements[2..])?;
            Some((key, fields))
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
    pub fn parse_key_ord_pivot_value_command(
        elements: &[RespValue],
        expected_len: usize,
    ) -> Option<(String, String, String, String)> {
        if elements.len() == expected_len {
            Some((
                extract_bulk_string(&elements[1])?, // key
                extract_bulk_string(&elements[2])?, // BEFORE/AFTER
                extract_bulk_string(&elements[3])?, // pivot
                extract_bulk_string(&elements[4])?, // element
            ))
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

    pub fn format_error(error: impl std::fmt::Display) -> String {
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

macro_rules! parse_command {
    // Single key commands
    (single_key, $elements:expr, $variant:ident) => {
        command_helper::parse_single_key_command($elements, 2).map(Command::$variant)
    };

    // Key-value commands
    (key_value, $elements:expr, $variant:ident) => {
        command_helper::parse_key_value_command($elements, 3).map(|(k, v)| Command::$variant(k, v))
    };

    // Key-field-value commands
    (key_field_value, $elements:expr, $variant:ident) => {
        command_helper::parse_key_field_value_command($elements, 4)
            .map(|(k, f, v)| Command::$variant(k, f, v))
    };

    // Multiple keys commands
    (keys, $elements:expr, $variant:ident) => {
        command_helper::parse_keys_command($elements, 2).map(Command::$variant)
    };

    // Key-fields commands
    (key_fields, $elements:expr, $variant:ident) => {
        command_helper::parse_key_fields_command($elements, 3).map(|(k, f)| Command::$variant(k, f))
    };

    // Key-value pairs commands
    (key_value_pairs, $elements:expr, $variant:ident) => {
        command_helper::parse_keys_values_command($elements, 3).map(Command::$variant)
    };

    (key_ord_pivot_value,$elements:expr,$variant:ident) => {
        command_helper::parse_key_ord_pivot_value_command($elements, 5)
            .map(|(k, o, p, v)| Command::$variant(k, o, p, v))
    };
    // Special PING command
    (ping, $elements:expr) => {
        match $elements.len() {
            1 => Some(Command::Ping(None)),
            2 => Some(Command::Ping(command_helper::extract_bulk_string(
                &$elements[1],
            ))),
            _ => None,
        }
    };
}

impl Command {
    pub fn parse(resp_value: &RespValue) -> Option<Self> {
        match resp_value {
            RespValue::Array(elements) if !elements.is_empty() => {
                let command_name =
                    command_helper::extract_bulk_string(&elements[0])?.to_uppercase();

                match command_name.as_str() {
                    "PING" => parse_command!(ping, elements),
                    "GET" => parse_command!(single_key, elements, Get),
                    "SET" => parse_command!(key_value, elements, Set),
                    "DEL" => parse_command!(keys, elements, Del),
                    "INCR" => parse_command!(single_key, elements, Incr),
                    "DECR" => parse_command!(single_key, elements, Decr),
                    "INCRBY" => parse_command!(key_value, elements, IncrBy),
                    "DECRBY" => parse_command!(key_value, elements, DecrBy),
                    "APPEND" => parse_command!(key_value, elements, Append),
                    "STRLEN" => parse_command!(single_key, elements, Strlen),
                    "MGET" => parse_command!(keys, elements, MGet),
                    "MSET" => parse_command!(key_value_pairs, elements, MSet),
                    "HSET" => parse_command!(key_field_value, elements, HSet),
                    "HGET" => parse_command!(key_value, elements, HGet),
                    "HDEL" => parse_command!(key_fields, elements, HDel),
                    "HGETALL" => parse_command!(single_key, elements, HGetAll),
                    "HKEYS" => parse_command!(single_key, elements, HKeys),
                    "HVALS" => parse_command!(single_key, elements, HVals),
                    "HLEN" => parse_command!(single_key, elements, HLen),
                    "HEXISTS" => parse_command!(key_value, elements, HExists),
                    "HINCRBY" => parse_command!(key_field_value, elements, HIncrBy),
                    "HINCRBYFLOAT" => parse_command!(key_field_value, elements, HIncrByFloat),
                    "LPUSH" => parse_command!(key_fields, elements, LPush),
                    "RPUSH" => parse_command!(key_fields, elements, RPush),
                    "LPOP" => parse_command!(single_key, elements, LPop),
                    "RPOP" => parse_command!(single_key, elements, RPop),
                    "LLEN" => parse_command!(single_key, elements, LLen),
                    "LINDEX" => parse_command!(key_value, elements, LIndex),
                    "LRANGE" => parse_command!(key_field_value, elements, LRange),
                    "LTRIM" => parse_command!(key_field_value, elements, LTrim),
                    "LSET" => parse_command!(key_field_value, elements, LSet),
                    "LINSERT" => parse_command!(key_ord_pivot_value, elements, LInsert),
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
            //String
            Self::Get(key) => match db_guard.get(key) {
                Some(value) => command_helper::format_bulk_string(value),
                None => command_helper::format_null(),
            },
            Self::Set(key, value) => {
                db_guard.set(key, value.clone());
                command_helper::format_simple_string("OK")
            }
            Self::Del(keys) => command_helper::format_integer(db_guard.del(keys) as i64),
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
            Self::Append(key, value) => {
                command_helper::format_integer(db_guard.append(key, value) as i64)
            }
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
            // Hash
            Self::HSet(hash, field, value) => match db_guard.hset(hash, field, value) {
                Ok(result) => command_helper::format_integer(result),
                Err(e) => command_helper::format_error(e),
            },
            Self::HGet(hash, field) => match db_guard.hget(hash, field) {
                Ok(Some(result)) => command_helper::format_bulk_string(result),
                Ok(None) => command_helper::format_null(),
                Err(e) => command_helper::format_error(e),
            },
            Self::HDel(hash, fields) => {
                command_helper::format_integer(db_guard.hdel_multiple(hash, &fields) as i64)
            }
            Self::HGetAll(key) => match db_guard.hget_all(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::HKeys(key) => match db_guard.hkeys(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::HVals(key) => match db_guard.hvals(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },
            Self::HLen(key) => match db_guard.hlen(key) {
                Ok(value) => command_helper::format_integer(value as i64),
                Err(e) => command_helper::format_error(e),
            },
            Self::HExists(hash, field) => match db_guard.hexists(hash, field) {
                Ok(value) => command_helper::format_integer(if value { 1 } else { 0 }),
                Err(e) => command_helper::format_error(e),
            },
            Self::HIncrBy(hash, field, value) => match db_guard.hincrby(hash, field, &value) {
                Ok(result) => command_helper::format_integer(result),
                Err(e) => command_helper::format_error(e),
            },
            Self::HIncrByFloat(hash, field, value) => {
                match db_guard.hincrbyfloat(hash, field, &value) {
                    Ok(result) => command_helper::format_bulk_string(&result.to_string()),
                    Err(e) => command_helper::format_error(e),
                }
            }
            Self::LPush(key, value) => {
                db_guard.lpush(key, value);
                format_simple_string("OK")
            }
            Self::RPush(key, value) => {
                db_guard.rpush(key, value);
                format_simple_string("OK")
            }
            Self::LPop(key) => match db_guard.lpop(key) {
                Some(result) => format_simple_string(&result),
                None => format_null(),
            },
            Self::RPop(key) => match db_guard.rpop(key) {
                Some(result) => format_simple_string(&result),
                None => format_null(),
            },
            Self::LLen(key)=>format_integer(db_guard.llen(key)as i64),
            Self::LIndex(key, index)=>match db_guard.lindex(key, index) {
                Some(val) => {format_simple_string(val)},
                None => {format_null()},
            },
            Self::LRange(key, start, end)=>match db_guard.lrange(key, start, end) {
                Ok(val) => {format_array(val.iter().map(|v|format_bulk_string(v)).collect())},
                Err(e) => {format_error(e)},
            },
            Self::LTrim(key, start, end)=>match db_guard.ltrim(key, start, end) {
                Ok(val) => {format_simple_string("OK")},
                Err(e) => {format_error(e)},
            }
            Self::LSet(key, index, value)=>match db_guard.lset(key, index, value.to_string()) {
                Ok(val) => {format_simple_string("OK")},
                Err(e) => {format_error(e)},
            }Self::LInsert(key, ord, pivot, value)=>match db_guard.linsert(key, ord, pivot, value.to_string()) {
                Ok(val) => {format_integer(val)},
                Err(e) => {format_error(e)},
            }
        }
    }
}
