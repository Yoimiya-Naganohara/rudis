// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{
    commands::command_helper::{
        format_array_bytes, format_bulk_string, format_error, format_hash_response, format_integer,
        format_null, format_simple_string,
    },
    database::SharedDatabase,
    networking::resp::RespValue,
};
use bytes::Bytes;

pub mod connection;
pub mod errors;
pub mod hashes;
pub mod keys;
pub mod lists;
pub mod sets;
pub mod strings;
pub mod zsets;

pub use errors::*;

#[derive(Debug, PartialEq)]
pub enum Command {
    // Connection Commands
    Ping(Option<Bytes>), // PING [message] - Test connection, optionally echo message
    Quit,
    // String Commands
    Get(Bytes),                            // GET key - Get value of key
    Set(Bytes, Bytes, Option<SetOptions>), // SET key value [NX|XX] [EX|PX|KEEPTTL] - Set key to hold string value
    Del(Vec<Bytes>),                       // DEL key [key ...] - Delete one or more keys
    Incr(Bytes),                           // INCR key - Increment integer value of key by 1
    Decr(Bytes),                           // DECR key - Decrement integer value of key by 1
    IncrBy(Bytes, Bytes), // INCRBY key increment - Increment integer value of key by increment
    DecrBy(Bytes, Bytes), // DECRBY key decrement - Decrement integer value of key by decrement
    Append(Bytes, Bytes), // APPEND key value - Append value to key
    Strlen(Bytes),        // STRLEN key - Get length of string stored in key
    MGet(Vec<Bytes>),     // MGET key [key ...] - Get values of multiple keys
    MSet(Vec<(Bytes, Bytes)>), // MSET key value [key value ...] - Set multiple keys to multiple values

    // Hash Commands
    HSet(Bytes, Bytes, Bytes), // HSET key field value - Set field in hash stored at key to value
    HGet(Bytes, Bytes),        // HGET key field - Get value of field in hash stored at key
    HDel(Bytes, Vec<Bytes>),   // HDEL key field [field ...] - Delete one or more hash fields
    HGetAll(Bytes),            // HGETALL key - Get all fields and values in hash
    HKeys(Bytes),              // HKEYS key - Get all field names in hash
    HVals(Bytes),              // HVALS key - Get all values in hash
    HLen(Bytes),               // HLEN key - Get number of fields in hash
    HExists(Bytes, Bytes),     // HEXISTS key field - Check if field exists in hash
    HIncrBy(Bytes, Bytes, Bytes), // HINCRBY key field increment - Increment integer value of hash field
    HIncrByFloat(Bytes, Bytes, Bytes), // HINCRBYFLOAT key field increment - Increment float value of hash field

    // List Commands
    LPush(Bytes, Vec<Bytes>), // LPUSH key element [element ...] - Insert elements at head of list
    RPush(Bytes, Vec<Bytes>), // RPUSH key element [element ...] - Insert elements at tail of list
    LPop(Bytes),              // LPOP key - Remove and return first element of list
    RPop(Bytes),              // RPOP key - Remove and return last element of list
    LLen(Bytes),              // LLEN key - Get length of list
    LIndex(Bytes, Bytes),     // LINDEX key index - Get element at index in list
    LRange(Bytes, Bytes, Bytes), // LRANGE key start stop - Get range of elements from list
    LTrim(Bytes, Bytes, Bytes), // LTRIM key start stop - Trim list to specified range
    LSet(Bytes, Bytes, Bytes), // LSET key index element - Set element at index in list
    LInsert(Bytes, Bytes, Bytes, Bytes), // LINSERT key BEFORE|AFTER pivot element - Insert element before/after pivot

    // Set Commands
    SAdd(Bytes, Vec<Bytes>), // SADD key member [member ...] - Add members to set
    SRem(Bytes, Vec<Bytes>), // SREM key member [member ...] - Remove members from set
    SMembers(Bytes),         // SMEMBERS key - Get all members in set
    SCard(Bytes),            // SCARD key - Get number of members in set
    SIsMember(Bytes, Bytes), // SISMEMBER key member - Check if member exists in set
    SInter(Vec<Bytes>),      // SINTER key [key ...] - Intersect multiple sets
    SUnion(Vec<Bytes>),      // SUNION key [key ...] - Union multiple sets
    SDiff(Vec<Bytes>),       // SDIFF key [key ...] - Subtract multiple sets

    // Sorted Set Commands
    ZAdd(Bytes, Vec<(Bytes, Bytes)>), // ZADD key score member [score member ...] - Add members to sorted set
    ZRem(Bytes, Vec<Bytes>), // ZREM key member [member ...] - Remove members from sorted set
    ZRange(Bytes, Bytes, Bytes), // ZRANGE key start stop - Get range of members in sorted set
    ZRangeByScore(Bytes, Bytes, Bytes), // ZRANGEBYSCORE key min max - Get members by score range
    ZCard(Bytes),            // ZCARD key - Get number of members in sorted set
    ZScore(Bytes, Bytes),    // ZSCORE key member - Get score of member in sorted set
    ZRank(Bytes, Bytes),     // ZRANK key member - Get rank of member in sorted set

    // Key Commands
    Exists(Vec<Bytes>),   // EXISTS key [key ...] - Check if keys exist
    Expire(Bytes, Bytes), // EXPIRE key seconds - Set key expiration time
    Ttl(Bytes),           // TTL key - Get remaining time to live of key
    Type(Bytes),          // TYPE key - Get type of key
    Keys(Bytes),          // KEYS pattern - Find keys matching pattern
    FlushAll,             // FLUSHALL - Remove all keys from all databases
    FlushDB,              // FLUSHDB - Remove all keys from current database

    // Connection/Server Commands
    Echo(Bytes),         // ECHO message - Echo the given string
    Auth(Bytes),         // AUTH password - Authenticate to server
    Select(Bytes),       // SELECT index - Change selected database
    Info(Option<Bytes>), // INFO [section] - Get server information

    // Additional String Commands
    SetNX(Bytes, Bytes), // SETNX key value - Set key only if it doesn't exist
    SetEX(Bytes, Bytes, Bytes), // SETEX key seconds value - Set key with expiration
    GetSet(Bytes, Bytes), // GETSET key value - Set key and return old value
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
                let command_name_bytes = command_helper::extract_bulk_string(&elements[0])?;
                // Convert command name to uppercase string for matching (commands are ASCII usually)
                let command_name = String::from_utf8_lossy(&command_name_bytes).to_uppercase();

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
                    "LLen" => parse_command!(single_key, elements, LLen),
                    "LINDEX" => parse_command!(key_value, elements, LIndex),
                    "LRANGE" => parse_command!(key_field_value, elements, LRange),
                    "LTRIM" => parse_command!(key_field_value, elements, LTrim),
                    "LSET" => parse_command!(key_field_value, elements, LSet),
                    "LINSERT" => parse_command!(key_ord_pivot_value, elements, LInsert),
                    "SADD" => parse_command!(key_fields, elements, SAdd),
                    "SREM" => parse_command!(key_fields, elements, SRem),
                    "SMEMBERS" => parse_command!(single_key, elements, SMembers),
                    "SCard" => parse_command!(single_key, elements, SCard),
                    "SISMEMBER" => parse_command!(key_value, elements, SIsMember),
                    "SINTER" => parse_command!(keys, elements, SInter),
                    "SUNION" => parse_command!(keys, elements, SUnion),
                    "SDiff" => parse_command!(keys, elements, SDiff),
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

    pub async fn execute(self, db: &SharedDatabase) -> Bytes {
        match self {
            Command::Ping(msg) => connection::ping(msg),
            Command::Quit => connection::quit(),
            Command::Get(key) => strings::get(db, key),
            Command::Set(key, value, options) => strings::set(db, key, value, options),
            Command::Del(keys) => strings::del(db, keys),
            Command::Incr(key) => strings::incr(db, key),
            Command::Decr(key) => strings::decr(db, key),
            Command::IncrBy(key, value) => strings::incr_by(db, key, value),
            Command::DecrBy(key, value) => strings::decr_by(db, key, value),
            Command::Append(key, value) => strings::append(db, key, value),
            Command::Strlen(key) => strings::strlen(db, key),
            Command::MGet(keys) => strings::mget(db, keys),
            Command::MSet(key_values) => strings::mset(db, key_values),
            Command::HSet(hash, field, value) => hashes::hset(db, hash, field, value),
            Command::HGet(hash, field) => hashes::hget(db, hash, field),
            Command::HDel(hash, fields) => hashes::hdel(db, hash, fields),
            Command::HGetAll(key) => hashes::hgetall(db, key),
            Command::HKeys(key) => hashes::hkeys(db, key),
            Command::HVals(key) => hashes::hvals(db, key),
            Command::HLen(key) => hashes::hlen(db, key),
            Command::HExists(hash, field) => hashes::hexists(db, hash, field),
            Command::HIncrBy(hash, field, value) => hashes::hincrby(db, hash, field, value),
            Command::HIncrByFloat(hash, field, value) => {
                hashes::hincrbyfloat(db, hash, field, value)
            }
            Command::LPush(key, value) => lists::lpush(db, key, value),
            Command::RPush(key, value) => lists::rpush(db, key, value),
            Command::LPop(key) => lists::lpop(db, key),
            Command::RPop(key) => lists::rpop(db, key),
            Command::LLen(key) => lists::llen(db, key),
            Command::LIndex(key, index) => lists::lindex(db, key, index),
            Command::LRange(key, start, end) => lists::lrange(db, key, start, end),
            Command::LTrim(key, start, end) => lists::ltrim(db, key, start, end),
            Command::LSet(key, index, value) => lists::lset(db, key, index, value),
            Command::LInsert(key, ord, pivot, value) => lists::linsert(db, key, ord, pivot, value),
            Command::SAdd(key, values) => sets::sadd(db, key, values),
            Command::SRem(key, values) => sets::srem(db, key, values),
            Command::SMembers(key) => sets::smembers(db, key),
            Command::SCard(key) => sets::scard(db, key),
            Command::SIsMember(key, member) => sets::sismember(db, key, member),
            Command::SInter(items) => sets::sinter(db, items),
            Command::SUnion(items) => sets::sunion(db, items),
            Command::SDiff(items) => sets::sdiff(db, items),
            Command::ZAdd(key, pairs) => zsets::zadd(db, key, pairs),
            Command::ZRem(key, members) => zsets::zrem(db, key, members),
            Command::ZRange(key, start, stop) => zsets::zrange(db, key, start, stop),
            Command::ZRangeByScore(key, min, max) => zsets::zrangebyscore(db, key, min, max),
            Command::ZCard(key) => zsets::zcard(db, key),
            Command::ZScore(key, member) => zsets::zscore(db, key, member),
            Command::ZRank(key, member) => zsets::zrank(db, key, member),
            Command::Exists(keys) => keys::exists(db, keys),
            Command::Expire(key, seconds) => keys::expire(db, key, seconds),
            Command::Ttl(key) => keys::ttl(db, key),
            Command::Type(key) => keys::type_(db, key),
            Command::Keys(pattern) => keys::keys(db, pattern),
            Command::FlushAll => keys::flushall(db),
            Command::FlushDB => keys::flushdb(db),
            Command::Echo(msg) => connection::echo(msg),
            Command::Auth(msg) => connection::auth(msg),
            Command::Select(db_index) => connection::select(db, db_index),
            Command::Info(section) => connection::info(section),
            Command::SetNX(key, value) => strings::setnx(db, key, value),
            Command::SetEX(key, seconds, value) => strings::setex(db, key, seconds, value),
            Command::GetSet(key, value) => strings::getset(db, key, value),
        }
    }
}
