use crate::commands::command_helper::{
    format_bulk_string, format_error, format_hash_response, format_integer, format_null,
};
use crate::database::traits::HashOp;
use crate::database::SharedDatabase;
use bytes::{Bytes, BytesMut};

pub fn hset(db: &SharedDatabase, hash: Bytes, pairs: Vec<(Bytes, Bytes)>, out: &mut BytesMut) {
    // HSET returns the number of fields that were *added* (not updated),
    // so sum the per-field results across all pairs.
    let mut added = 0i64;
    for (field, value) in pairs {
        match db.hset(&hash, field, value) {
            Ok(result) => added += result,
            Err(e) => {
                format_error(out, e);
                return;
            }
        }
    }
    format_integer(out, added)
}

pub fn hget(db: &SharedDatabase, hash: Bytes, field: Bytes, out: &mut BytesMut) {
    match db.hget(&hash, &field) {
        Ok(Some(result)) => format_bulk_string(out, &result),
        Ok(None) => format_null(out),
        Err(e) => format_error(out, e),
    }
}

pub fn hdel(db: &SharedDatabase, hash: Bytes, fields: Vec<Bytes>, out: &mut BytesMut) {
    format_integer(out, db.hdel_multiple(&hash, &fields) as i64)
}

pub fn hgetall(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.hget_all(&key) {
        Ok(value) => format_hash_response(out, value),
        Err(e) => format_error(out, e),
    }
}

pub fn hkeys(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.hkeys(&key) {
        Ok(value) => format_hash_response(out, value),
        Err(e) => format_error(out, e),
    }
}

pub fn hvals(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.hvals(&key) {
        Ok(value) => format_hash_response(out, value),
        Err(e) => format_error(out, e),
    }
}

pub fn hlen(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.hlen(&key) {
        Ok(value) => format_integer(out, value as i64),
        Err(e) => format_error(out, e),
    }
}

pub fn hexists(db: &SharedDatabase, hash: Bytes, field: Bytes, out: &mut BytesMut) {
    match db.hexists(&hash, &field) {
        Ok(value) => format_integer(out, if value { 1 } else { 0 }),
        Err(e) => format_error(out, e),
    }
}

pub fn hincrby(db: &SharedDatabase, hash: Bytes, field: Bytes, value: Bytes, out: &mut BytesMut) {
    // Parsing should happen here or in db?
    // Database::hincrby expects value: i64.
    // So we must parse Bytes -> i64 here.
    let val_str = match std::str::from_utf8(&value) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };
    match val_str.parse::<i64>() {
        Ok(val) => match db.hincrby(&hash, &field, val) {
            Ok(result) => format_integer(out, result),
            Err(e) => format_error(out, e),
        },
        Err(_) => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn hincrbyfloat(
    db: &SharedDatabase,
    hash: Bytes,
    field: Bytes,
    value: Bytes,
    out: &mut BytesMut,
) {
    let val_str = match std::str::from_utf8(&value) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidFloat);
            return;
        }
    };
    match val_str.parse::<f64>() {
        Ok(val) => match db.hincrbyfloat(&hash, &field, val) {
            Ok(result) => format_bulk_string(out, &Bytes::from(result.to_string())),
            Err(e) => format_error(out, e),
        },
        Err(_) => format_error(out, crate::commands::CommandError::InvalidFloat),
    }
}
