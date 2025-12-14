// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{
    commands::command_helper::{
        format_array, format_bulk_string, format_error, format_integer, format_null,
        format_simple_string,
    },
    database::{HashOp, KeyOp, ListOp, SetOp, SharedDatabase, SortedSetOp, StringOp},
    networking::resp::RespValue,
};
mod errors;
pub use errors::*;
#[derive(Debug, PartialEq)]
pub enum Command {
    // Connection Commands
    Ping(Option<String>), // PING [message] - Test connection, optionally echo message
    Quit,
    // String Commands
    Get(String),                             // GET key - Get value of key
    Set(String, String, Option<SetOptions>), // SET key value [NX|XX] [EX|PX|KEEPTTL] - Set key to hold string value
    Del(Vec<String>),                        // DEL key [key ...] - Delete one or more keys
    Incr(String),                            // INCR key - Increment integer value of key by 1
    Decr(String),                            // DECR key - Decrement integer value of key by 1
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

    // Set Commands
    SAdd(String, Vec<String>), // SADD key member [member ...] - Add members to set
    SRem(String, Vec<String>), // SREM key member [member ...] - Remove members from set
    SMembers(String),          // SMEMBERS key - Get all members in set
    SCard(String),             // SCARD key - Get number of members in set
    SIsMember(String, String), // SISMEMBER key member - Check if member exists in set
    SInter(Vec<String>),       // SINTER key [key ...] - Intersect multiple sets
    SUnion(Vec<String>),       // SUNION key [key ...] - Union multiple sets
    SDiff(Vec<String>),        // SDIFF key [key ...] - Subtract multiple sets

    // Sorted Set Commands
    ZAdd(String, Vec<(String, String)>), // ZADD key score member [score member ...] - Add members to sorted set
    ZRem(String, Vec<String>), // ZREM key member [member ...] - Remove members from sorted set
    ZRange(String, String, String), // ZRANGE key start stop - Get range of members in sorted set
    ZRangeByScore(String, String, String), // ZRANGEBYSCORE key min max - Get members by score range
    ZCard(String),             // ZCARD key - Get number of members in sorted set
    ZScore(String, String),    // ZSCORE key member - Get score of member in sorted set
    ZRank(String, String),     // ZRANK key member - Get rank of member in sorted set

    // Key Commands
    Exists(Vec<String>),    // EXISTS key [key ...] - Check if keys exist
    Expire(String, String), // EXPIRE key seconds - Set key expiration time
    Ttl(String),            // TTL key - Get remaining time to live of key
    Type(String),           // TYPE key - Get type of key
    Keys(String),           // KEYS pattern - Find keys matching pattern
    FlushAll,               // FLUSHALL - Remove all keys from all databases
    FlushDB,                // FLUSHDB - Remove all keys from current database

    // Connection/Server Commands
    Echo(String),         // ECHO message - Echo the given string
    Auth(String),         // AUTH password - Authenticate to server
    Select(String),       // SELECT index - Change selected database
    Info(Option<String>), // INFO [section] - Get server information

    // Additional String Commands
    SetNX(String, String), // SETNX key value - Set key only if it doesn't exist
    SetEX(String, String, String), // SETEX key seconds value - Set key with expiration
    GetSet(String, String), // GETSET key value - Set key and return old value
}
#[derive(Debug, PartialEq)]
pub struct SetOptions {
    pub nx: bool,
    pub xx: bool,
    pub ex: Option<u64>, // seconds
    pub px: Option<u64>, // milliseconds
    pub keepttl: bool,
}
pub mod command_helper;

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
    (key_pair_values,$elements:expr,$variant:ident) => {
        command_helper::parse_key_pair_values_command($elements, 4)
            .map(|(k, v)| Command::$variant(k, v))
    };
    (key_ord_pivot_value,$elements:expr,$variant:ident) => {
        command_helper::parse_key_ord_pivot_value_command($elements, 5)
            .map(|(k, o, p, v)| Command::$variant(k, o, p, v))
    };
    (key_value_options,$elements:expr,$variant:ident) => {
        command_helper::parse_key_value_options_command($elements, 3)
            .map(|(k, v, o)| Command::$variant(k, v, o))
    };
    (none,$elements:expr,$variant:ident) => {
        match $elements.len() {
            1 => Some(Command::$variant),
            _ => None,
        }
    };
    // Special PING command
    (option, $elements:expr,$variant:ident) => {
        match $elements.len() {
            1 => Some(Command::$variant(None)),
            2 => Some(Command::$variant(command_helper::extract_bulk_string(
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
                    "PING" => parse_command!(option, elements, Ping),
                    "QUIT" => parse_command!(none, elements, Quit),
                    "GET" => parse_command!(single_key, elements, Get),
                    "SET" => {
                        parse_command!(key_value_options, elements, Set)
                    }
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
                    "SADD" => parse_command!(key_fields, elements, SAdd),
                    "SREM" => parse_command!(key_fields, elements, SRem),
                    "SMEMBERS" => parse_command!(single_key, elements, SMembers),
                    "SCARD" => parse_command!(single_key, elements, SCard),
                    "SISMEMBER" => parse_command!(key_value, elements, SIsMember),
                    "SINTER" => parse_command!(keys, elements, SInter),
                    "SUNION" => parse_command!(keys, elements, SUnion),
                    "SDIFF" => parse_command!(keys, elements, SDiff),
                    "ZADD" => parse_command!(key_pair_values, elements, ZAdd),
                    "ZREM" => parse_command!(key_fields, elements, ZRem),
                    "ZRANGE" => parse_command!(key_field_value, elements, ZRange),
                    "ZRANGEBYSCORE" => parse_command!(key_field_value, elements, ZRangeByScore),
                    "ZCARD" => parse_command!(single_key, elements, ZCard),
                    "ZSCORE" => parse_command!(key_value, elements, ZScore),
                    "ZRANK" => parse_command!(key_value, elements, ZRank),
                    "EXISTS" => parse_command!(keys, elements, Exists),
                    "EXPIRE" => parse_command!(key_value, elements, Expire),
                    "TTL" => parse_command!(single_key, elements, Ttl),
                    "TYPE" => parse_command!(single_key, elements, Type),
                    "KEYS" => parse_command!(single_key, elements, Keys),
                    "FLUSHALL" => parse_command!(none, elements, FlushAll),
                    "FLUSHDB" => parse_command!(none, elements, FlushDB),
                    "ECHO" => parse_command!(single_key, elements, Echo),
                    "AUTH" => parse_command!(single_key, elements, Auth),
                    "SELECT" => parse_command!(single_key, elements, Select),
                    "INFO" => parse_command!(option, elements, Info),
                    "SETNX" => parse_command!(key_value, elements, SetNX),
                    "SETEX" => parse_command!(key_field_value, elements, SetEX),
                    "GETSET" => parse_command!(key_value, elements, GetSet),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub async fn execute(&self, db: &SharedDatabase) -> String {
        match self {
            Command::Ping(None) => command_helper::format_simple_string("PONG"),
            Command::Ping(Some(msg)) => command_helper::format_simple_string(msg),
            Command::Quit => "#QUIT".to_owned(),
            Command::Get(key) => match db.get(key) {
                Some(value) => command_helper::format_bulk_string(&value),
                None => command_helper::format_null(),
            },
            Command::Set(key, value, options) => {
                let mut should_set = true;
                if let Some(opts) = &options {
                    let exists = db.get(&key).is_some();
                    if opts.nx && exists {
                        should_set = false;
                    }
                    if opts.xx && !exists {
                        should_set = false;
                    }
                }
                if should_set {
                    db.set(&key, value.clone());
                    if let Some(opts) = &options {
                        if let Some(ex) = opts.ex {
                            let _ = db.expire(&key, &ex.to_string());
                        } else if let Some(px) = opts.px {
                            let secs = (px as f64 / 1000.0).ceil() as u64;
                            let _ = db.expire(&key, &secs.to_string());
                        }
                        // KEEPTTL does nothing, TTL is kept
                    }
                    command_helper::format_simple_string("OK")
                } else {
                    command_helper::format_null()
                }
            }
            Command::Del(keys) => command_helper::format_integer(db.del(keys) as i64),
            Command::Incr(key) => match db.incr(key) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::Decr(key) => match db.decr(key) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::IncrBy(key, value) => match db.incr_by(key, value) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::DecrBy(key, value) => match db.decr_by(key, value) {
                Ok(value) => command_helper::format_integer(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::Append(key, value) => {
                command_helper::format_integer(db.append(key, value) as i64)
            }
            Command::Strlen(key) => command_helper::format_integer(db.str_len(key) as i64),
            Command::MGet(keys) => command_helper::format_array(
                keys.iter()
                    .map(|key| match db.get(key) {
                        Some(value) => format!("${}\r\n{}\r\n", value.len(), value),
                        None => "$-1\r\n".to_string(),
                    })
                    .collect::<Vec<String>>(),
            ),
            Command::MSet(key_values) => {
                key_values
                    .iter()
                    .for_each(|(key, value)| db.set(key, value.clone()));
                command_helper::format_simple_string("OK")
            }
            Command::HSet(hash, field, value) => match db.hset(hash, field, value) {
                Ok(result) => command_helper::format_integer(result),
                Err(e) => command_helper::format_error(e),
            },
            Command::HGet(hash, field) => match db.hget(hash, field) {
                Ok(Some(result)) => command_helper::format_bulk_string(&result),
                Ok(None) => command_helper::format_null(),
                Err(e) => command_helper::format_error(e),
            },
            Command::HDel(hash, fields) => {
                command_helper::format_integer(db.hdel_multiple(hash, &fields) as i64)
            }
            Command::HGetAll(key) => match db.hget_all(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::HKeys(key) => match db.hkeys(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::HVals(key) => match db.hvals(key) {
                Ok(value) => command_helper::format_hash_response(value),
                Err(e) => command_helper::format_error(e),
            },
            Command::HLen(key) => match db.hlen(key) {
                Ok(value) => command_helper::format_integer(value as i64),
                Err(e) => command_helper::format_error(e),
            },
            Command::HExists(hash, field) => match db.hexists(hash, field) {
                Ok(value) => command_helper::format_integer(if value { 1 } else { 0 }),
                Err(e) => command_helper::format_error(e),
            },
            Command::HIncrBy(hash, field, value) => match db.hincrby(hash, field, &value) {
                Ok(result) => command_helper::format_integer(result),
                Err(e) => command_helper::format_error(e),
            },
            Command::HIncrByFloat(hash, field, value) => {
                match db.hincrbyfloat(hash, field, &value) {
                    Ok(result) => command_helper::format_bulk_string(&result.to_string()),
                    Err(e) => command_helper::format_error(e),
                }
            }
            Command::LPush(key, value) => {
                db.lpush(key, value);
                format_simple_string("OK")
            }
            Command::RPush(key, value) => {
                db.rpush(key, value);
                format_simple_string("OK")
            }
            Command::LPop(key) => match db.lpop(key) {
                Some(result) => format_simple_string(&result),
                None => format_null(),
            },
            Command::RPop(key) => match db.rpop(key) {
                Some(result) => format_simple_string(&result),
                None => format_null(),
            },
            Command::LLen(key) => format_integer(db.llen(key) as i64),
            Command::LIndex(key, index) => match db.lindex(key, index) {
                Some(val) => format_simple_string(&val),
                None => format_null(),
            },
            Command::LRange(key, start, end) => match db.lrange(key, start, end) {
                Ok(val) => format_array(val.iter().map(|v| format_bulk_string(v)).collect()),
                Err(e) => format_error(e),
            },
            Command::LTrim(key, start, end) => match db.ltrim(key, start, end) {
                Ok(val) => format_simple_string("OK"),
                Err(e) => format_error(e),
            },
            Command::LSet(key, index, value) => match db.lset(key, index, value.to_string()) {
                Ok(val) => format_simple_string("OK"),
                Err(e) => format_error(e),
            },
            Command::LInsert(key, ord, pivot, value) => {
                match db.linsert(key, ord, pivot, value.to_string()) {
                    Ok(val) => format_integer(val),
                    Err(e) => format_error(e),
                }
            }
            Command::SAdd(key, values) => format_integer(db.sadd(key, values) as i64),
            Command::SRem(key, values) => format_integer(db.srem(key, values) as i64),
            Command::SMembers(key) => match db.smembers(key) {
                Ok(value) => {
                    format_array(value.iter().map(|v| format_bulk_string(v)).collect())
                }
                Err(e) => format_error(e),
            },
            Command::SCard(key) => format_integer(db.scard(key) as i64),
            Command::SIsMember(key, member) => format_integer(db.sismember(key, member) as i64),
            Command::SInter(items) => match db.sinter(items) {
                Ok(res) => format_array(res.iter().map(|v| format_bulk_string(v)).collect()),
                Err(e) => format_error(e),
            },
            Command::SUnion(items) => match db.sunion(items) {
                Ok(res) => format_array(res.iter().map(|v| format_bulk_string(v)).collect()),
                Err(e) => format_error(e),
            },
            Command::SDiff(items) => match db.sdiff(items) {
                Ok(res) => format_array(res.iter().map(|v| format_bulk_string(v)).collect()),
                Err(e) => format_error(e),
            },
            Command::ZAdd(key, pairs) => {
                let added = db.zadd(key, pairs);
                command_helper::format_integer(added as i64)
            }
            Command::ZRem(key, members) => {
                let removed = db.zrem(key, members);
                command_helper::format_integer(removed as i64)
            }
            Command::ZRange(key, start, stop) => match db.zrange(key, start, stop) {
                Ok(members) => command_helper::format_array(
                    members
                        .iter()
                        .map(|m| command_helper::format_bulk_string(m))
                        .collect(),
                ),
                Err(e) => command_helper::format_error(e),
            },
            Command::ZRangeByScore(key, min, max) => match db.zrange_by_score(key, min, max) {
                Ok(members) => command_helper::format_array(
                    members
                        .iter()
                        .map(|m| command_helper::format_bulk_string(m))
                        .collect(),
                ),
                Err(e) => command_helper::format_error(e),
            },
            Command::ZCard(key) => command_helper::format_integer(db.zcard(key) as i64),
            Command::ZScore(key, member) => match db.zscore(key, member) {
                Some(score) => command_helper::format_bulk_string(&score.to_string()),
                None => command_helper::format_null(),
            },
            Command::ZRank(key, member) => match db.zrank(key, member) {
                Some(rank) => command_helper::format_integer(rank as i64),
                None => command_helper::format_null(),
            },
            Command::Exists(keys) => command_helper::format_integer(db.exist(keys) as i64),
            Command::Expire(key, seconds) => match db.expire(key, seconds) {
                Ok(()) => command_helper::format_simple_string("OK"),
                Err(e) => command_helper::format_error(e),
            },
            Command::Ttl(key) => command_helper::format_integer(db.ttl(key)),
            Command::Type(key) => {
                // TODO: implement type checking
                command_helper::format_simple_string(db.data_type(key)) // placeholder
            }
            Command::Keys(pattern) => match db.keys(pattern) {
                Ok(keys) => command_helper::format_array(
                    keys.iter()
                        .map(|k| command_helper::format_bulk_string(k))
                        .collect(),
                ),
                Err(e) => command_helper::format_error(e),
            },
            Command::FlushAll => {
                db.flush_all();
                command_helper::format_simple_string("OK")
            }
            Command::FlushDB => {
                db.flush_db();
                command_helper::format_simple_string("OK")
            }
            Command::Echo(msg) => command_helper::format_bulk_string(msg),
            Command::Auth(_) => command_helper::format_simple_string("OK"),
            Command::Select(db_index) => match db_index.parse::<u8>() {
                Ok(db_num) if db_num <= 15 => {
                    db.select(db_num);
                    command_helper::format_simple_string("OK")
                }
                _ => command_helper::format_error("ERR invalid DB index"),
            },
            Command::Info(_) => {
                command_helper::format_bulk_string("# Server\r\nredis_version:6.0.0\r\n")
            }
            Command::SetNX(key, value) => {
                if db.get(key).is_none() {
                    db.set(key, value.clone());
                    command_helper::format_integer(1)
                } else {
                    command_helper::format_integer(0)
                }
            }
            Command::SetEX(key, seconds, value) => {
                db.set(key, value.clone());
                match db.expire(key, seconds) {
                    Ok(()) => command_helper::format_simple_string("OK"),
                    Err(e) => command_helper::format_error(e),
                }
            }
            Command::GetSet(key, value) => {
                let old = db.get(key).map(|s| s.to_string());
                db.set(key, value.clone());
                match old {
                    Some(val) => command_helper::format_bulk_string(&val),
                    None => command_helper::format_null(),
                }
            }
        }
    }
}
