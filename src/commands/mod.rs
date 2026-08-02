// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{database::SharedDatabase, networking::resp::RespValue};
use bytes::{Bytes, BytesMut};
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::LazyLock,
};

pub mod connection;
pub mod errors;
pub mod hashes;
pub mod keys;
pub mod lists;
pub mod sets;
pub mod strings;
pub mod zsets;

pub use errors::*;

#[derive(Debug, PartialEq, Clone)]
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
    HSet(Bytes, Vec<(Bytes, Bytes)>), // HSET key field value [field value ...] - Set fields in hash stored at key to values
    HGet(Bytes, Bytes),               // HGET key field - Get value of field in hash stored at key
    HDel(Bytes, Vec<Bytes>),          // HDEL key field [field ...] - Delete one or more hash fields
    HGetAll(Bytes),                   // HGETALL key - Get all fields and values in hash
    HKeys(Bytes),                     // HKEYS key - Get all field names in hash
    HVals(Bytes),                     // HVALS key - Get all values in hash
    HLen(Bytes),                      // HLEN key - Get number of fields in hash
    HExists(Bytes, Bytes),            // HEXISTS key field - Check if field exists in hash
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
#[derive(Debug, PartialEq, Clone)]
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

// FNV-1a hasher: fast and deterministic, with no per-lookup randomness cost.
// Good enough for a fixed command table with short keys (collisions are rare
// and, at worst, degrade to a few memcmps across 60-ish entries).
struct FnvHasher(u64);

impl Default for FnvHasher {
    fn default() -> Self {
        FnvHasher(0xcbf2_9ce4_8422_2325) // FNV-1a offset basis
    }
}

impl Hasher for FnvHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut state = self.0;
        for &b in bytes {
            state ^= u64::from(b);
            state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = state;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

// Builds a (name, parse_fn) entry for COMMAND_TABLE, forwarding the arity
// pattern (single_key, key_value, ...) straight into parse_command!.
// Builds a (name, parse_fn) entry for COMMAND_TABLE, forwarding the arity
// pattern (single_key, key_value, ...) straight into parse_command!.
// The explicit `as fn(...)` cast forces the closure-to-fn-pointer coercion
// so all entries share one uniform type in the array literal.
macro_rules! cmd {
    ($name:literal, $kind:ident, $variant:ident) => {
        (
            $name.as_slice(),
            (|e: &[RespValue]| parse_command!($kind, e, $variant)) as CommandParser,
        )
    };
}

// Longest command name (ZRANGEBYSCORE) is 13 bytes; 32 leaves headroom.
const MAX_COMMAND_NAME_LEN: usize = 32;

// Command dispatch table: maps lowercase command names to their parser.
// Built once; every request folds the name to ASCII lowercase in a stack
// buffer (no heap allocation) and does a single hash lookup.
type CommandParser = fn(&[RespValue]) -> Option<Command>;
type CommandTable = HashMap<&'static [u8], CommandParser, BuildHasherDefault<FnvHasher>>;

static COMMAND_TABLE: LazyLock<CommandTable> = LazyLock::new(|| {
    let mut table: CommandTable = HashMap::with_hasher(BuildHasherDefault::default());

    table.extend([
        // Connection Commands
        cmd!(b"ping", option, Ping),
        cmd!(b"quit", none, Quit),
        // String Commands
        cmd!(b"get", single_key, Get),
        cmd!(b"set", key_value_options, Set),
        cmd!(b"del", keys, Del),
        cmd!(b"incr", single_key, Incr),
        cmd!(b"decr", single_key, Decr),
        cmd!(b"incrby", key_value, IncrBy),
        cmd!(b"decrby", key_value, DecrBy),
        cmd!(b"append", key_value, Append),
        cmd!(b"strlen", single_key, Strlen),
        cmd!(b"mget", keys, MGet),
        cmd!(b"mset", key_value_pairs, MSet),
        // Hash Commands
        cmd!(b"hset", key_pair_values, HSet),
        cmd!(b"hget", key_value, HGet),
        cmd!(b"hdel", key_fields, HDel),
        cmd!(b"hgetall", single_key, HGetAll),
        cmd!(b"hkeys", single_key, HKeys),
        cmd!(b"hvals", single_key, HVals),
        cmd!(b"hlen", single_key, HLen),
        cmd!(b"hexists", key_value, HExists),
        cmd!(b"hincrby", key_field_value, HIncrBy),
        cmd!(b"hincrbyfloat", key_field_value, HIncrByFloat),
        // List Commands
        cmd!(b"lpush", key_fields, LPush),
        cmd!(b"rpush", key_fields, RPush),
        cmd!(b"lpop", single_key, LPop),
        cmd!(b"rpop", single_key, RPop),
        cmd!(b"llen", single_key, LLen),
        cmd!(b"lindex", key_value, LIndex),
        cmd!(b"lrange", key_field_value, LRange),
        cmd!(b"ltrim", key_field_value, LTrim),
        cmd!(b"lset", key_field_value, LSet),
        cmd!(b"linsert", key_ord_pivot_value, LInsert),
        // Set Commands
        cmd!(b"sadd", key_fields, SAdd),
        cmd!(b"srem", key_fields, SRem),
        cmd!(b"smembers", single_key, SMembers),
        cmd!(b"scard", single_key, SCard),
        cmd!(b"sismember", key_value, SIsMember),
        cmd!(b"sinter", keys, SInter),
        cmd!(b"sunion", keys, SUnion),
        cmd!(b"sdiff", keys, SDiff),
        // Sorted Set Commands
        cmd!(b"zadd", key_pair_values, ZAdd),
        cmd!(b"zrem", key_fields, ZRem),
        cmd!(b"zrange", key_field_value, ZRange),
        cmd!(b"zrangebyscore", key_field_value, ZRangeByScore),
        cmd!(b"zcard", single_key, ZCard),
        cmd!(b"zscore", key_value, ZScore),
        cmd!(b"zrank", key_value, ZRank),
        // Key Commands
        cmd!(b"exists", keys, Exists),
        cmd!(b"expire", key_value, Expire),
        cmd!(b"ttl", single_key, Ttl),
        cmd!(b"type", single_key, Type),
        cmd!(b"keys", single_key, Keys),
        cmd!(b"flushall", none, FlushAll),
        cmd!(b"flushdb", none, FlushDB),
        // Connection/Server Commands
        cmd!(b"echo", single_key, Echo),
        cmd!(b"auth", single_key, Auth),
        cmd!(b"select", single_key, Select),
        cmd!(b"info", option, Info),
        // Additional String Commands
        cmd!(b"setnx", key_value, SetNX),
        cmd!(b"setex", key_field_value, SetEX),
        cmd!(b"getset", key_value, GetSet),
    ]);
    table
});

impl Command {
    pub fn parse(resp_value: &RespValue) -> Option<Self> {
        let RespValue::Array(elements) = resp_value else {
            return None;
        };
        if elements.is_empty() {
            return None;
        }
        // Command name as a byte slice: no clone, no allocation
        let name_bytes = command_helper::bulk_string_bytes(&elements[0])?;
        if name_bytes.is_empty() || name_bytes.len() > MAX_COMMAND_NAME_LEN {
            return None;
        }
        // Fold to ASCII lowercase in a stack buffer, then do one hash lookup.
        // Commands are case-insensitive ASCII, so this matches any casing
        // without allocating or running Unicode-aware uppercasing.
        let mut lower = [0u8; MAX_COMMAND_NAME_LEN];
        for (i, &b) in name_bytes.iter().enumerate() {
            lower[i] = b.to_ascii_lowercase();
        }
        let parse_fn = COMMAND_TABLE.get(&lower[..name_bytes.len()])?;
        parse_fn(elements)
    }

    pub async fn execute(self, db: &SharedDatabase, out: &mut BytesMut) {
        match self {
            Command::Ping(msg) => connection::ping(msg, out),
            Command::Quit => connection::quit(),
            Command::Get(key) => strings::get(db, key, out),
            Command::Set(key, value, options) => strings::set(db, key, value, options, out),
            Command::Del(keys) => strings::del(db, keys, out),
            Command::Incr(key) => strings::incr(db, key, out),
            Command::Decr(key) => strings::decr(db, key, out),
            Command::IncrBy(key, value) => strings::incr_by(db, key, value, out),
            Command::DecrBy(key, value) => strings::decr_by(db, key, value, out),
            Command::Append(key, value) => strings::append(db, key, value, out),
            Command::Strlen(key) => strings::strlen(db, key, out),
            Command::MGet(keys) => strings::mget(db, keys, out),
            Command::MSet(key_values) => strings::mset(db, key_values, out),
            Command::HSet(hash, pairs) => hashes::hset(db, hash, pairs, out),
            Command::HGet(hash, field) => hashes::hget(db, hash, field, out),
            Command::HDel(hash, fields) => hashes::hdel(db, hash, fields, out),
            Command::HGetAll(key) => hashes::hgetall(db, key, out),
            Command::HKeys(key) => hashes::hkeys(db, key, out),
            Command::HVals(key) => hashes::hvals(db, key, out),
            Command::HLen(key) => hashes::hlen(db, key, out),
            Command::HExists(hash, field) => hashes::hexists(db, hash, field, out),
            Command::HIncrBy(hash, field, value) => hashes::hincrby(db, hash, field, value, out),
            Command::HIncrByFloat(hash, field, value) => {
                hashes::hincrbyfloat(db, hash, field, value, out)
            }
            Command::LPush(key, value) => lists::lpush(db, key, value, out),
            Command::RPush(key, value) => lists::rpush(db, key, value, out),
            Command::LPop(key) => lists::lpop(db, key, out),
            Command::RPop(key) => lists::rpop(db, key, out),
            Command::LLen(key) => lists::llen(db, key, out),
            Command::LIndex(key, index) => lists::lindex(db, key, index, out),
            Command::LRange(key, start, end) => lists::lrange(db, key, start, end, out),
            Command::LTrim(key, start, end) => lists::ltrim(db, key, start, end, out),
            Command::LSet(key, index, value) => lists::lset(db, key, index, value, out),
            Command::LInsert(key, ord, pivot, value) => {
                lists::linsert(db, key, ord, pivot, value, out)
            }
            Command::SAdd(key, values) => sets::sadd(db, key, values, out),
            Command::SRem(key, values) => sets::srem(db, key, values, out),
            Command::SMembers(key) => sets::smembers(db, key, out),
            Command::SCard(key) => sets::scard(db, key, out),
            Command::SIsMember(key, member) => sets::sismember(db, key, member, out),
            Command::SInter(items) => sets::sinter(db, items, out),
            Command::SUnion(items) => sets::sunion(db, items, out),
            Command::SDiff(items) => sets::sdiff(db, items, out),
            Command::ZAdd(key, pairs) => zsets::zadd(db, key, pairs, out),
            Command::ZRem(key, members) => zsets::zrem(db, key, members, out),
            Command::ZRange(key, start, stop) => zsets::zrange(db, key, start, stop, out),
            Command::ZRangeByScore(key, min, max) => zsets::zrangebyscore(db, key, min, max, out),
            Command::ZCard(key) => zsets::zcard(db, key, out),
            Command::ZScore(key, member) => zsets::zscore(db, key, member, out),
            Command::ZRank(key, member) => zsets::zrank(db, key, member, out),
            Command::Exists(keys) => keys::exists(db, keys, out),
            Command::Expire(key, seconds) => keys::expire(db, key, seconds, out),
            Command::Ttl(key) => keys::ttl(db, key, out),
            Command::Type(key) => keys::type_(db, key, out),
            Command::Keys(pattern) => keys::keys(db, pattern, out),
            Command::FlushAll => keys::flushall(db, out),
            Command::FlushDB => keys::flushdb(db, out),
            Command::Echo(msg) => connection::echo(msg, out),
            Command::Auth(msg) => connection::auth(msg, out),
            Command::Select(db_index) => connection::select(db, db_index, out),
            Command::Info(section) => connection::info(section, out),
            Command::SetNX(key, value) => strings::setnx(db, key, value, out),
            Command::SetEX(key, seconds, value) => strings::setex(db, key, seconds, value, out),
            Command::GetSet(key, value) => strings::getset(db, key, value, out),
        }
    }
}
