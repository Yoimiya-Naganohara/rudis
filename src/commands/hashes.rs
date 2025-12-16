use crate::commands::command_helper::{
    format_bulk_string, format_error, format_hash_response, format_integer, format_null,
};
use crate::database::{HashOp, SharedDatabase};
use bytes::Bytes;

pub fn hset(db: &SharedDatabase, hash: Bytes, field: Bytes, value: Bytes) -> Bytes {
    match db.hset(&hash, field, value) {
        Ok(result) => format_integer(result),
        Err(e) => format_error(e),
    }
}

pub fn hget(db: &SharedDatabase, hash: Bytes, field: Bytes) -> Bytes {
    match db.hget(&hash, &field) {
        Ok(Some(result)) => format_bulk_string(&result),
        Ok(None) => format_null(),
        Err(e) => format_error(e),
    }
}

pub fn hdel(db: &SharedDatabase, hash: Bytes, fields: Vec<Bytes>) -> Bytes {
    format_integer(db.hdel_multiple(&hash, &fields) as i64)
}

pub fn hgetall(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.hget_all(&key) {
        Ok(value) => format_hash_response(value),
        Err(e) => format_error(e),
    }
}

pub fn hkeys(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.hkeys(&key) {
        Ok(value) => format_hash_response(value),
        Err(e) => format_error(e),
    }
}

pub fn hvals(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.hvals(&key) {
        Ok(value) => format_hash_response(value),
        Err(e) => format_error(e),
    }
}

pub fn hlen(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.hlen(&key) {
        Ok(value) => format_integer(value as i64),
        Err(e) => format_error(e),
    }
}

pub fn hexists(db: &SharedDatabase, hash: Bytes, field: Bytes) -> Bytes {
    match db.hexists(&hash, &field) {
        Ok(value) => format_integer(if value { 1 } else { 0 }),
        Err(e) => format_error(e),
    }
}

pub fn hincrby(db: &SharedDatabase, hash: Bytes, field: Bytes, value: Bytes) -> Bytes {
    // Parsing should happen here or in db?
    // Database::hincrby expects value: i64.
    // So we must parse Bytes -> i64 here.
    let val_str = match std::str::from_utf8(&value) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };
    match val_str.parse::<i64>() {
        Ok(val) => match db.hincrby(&hash, &field, val) {
            Ok(result) => format_integer(result),
            Err(e) => format_error(e),
        },
        Err(_) => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn hincrbyfloat(db: &SharedDatabase, hash: Bytes, field: Bytes, value: Bytes) -> Bytes {
    let val_str = match std::str::from_utf8(&value) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidFloat),
    };
    match val_str.parse::<f64>() {
        Ok(val) => match db.hincrbyfloat(&hash, &field, val) {
            Ok(result) => format_bulk_string(&Bytes::from(result.to_string())),
            Err(e) => format_error(e),
        },
        Err(_) => format_error(crate::commands::CommandError::InvalidFloat),
    }
}
