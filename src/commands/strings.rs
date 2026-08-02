use crate::{
    commands::{
        command_helper::{
            format_bulk_string, format_error, format_integer, format_null, format_simple_string,
        },
        SetOptions,
    },
    database::traits::{KeyOp, StringOp},
    database::SharedDatabase,
};
use bytes::{Bytes, BytesMut};
use std::fmt::Write;

pub fn get(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.get(&key) {
        Some(value) => format_bulk_string(out, &value),
        None => format_null(out),
    }
}

pub fn set(
    db: &SharedDatabase,
    key: Bytes,
    value: Bytes,
    options: Option<SetOptions>,
    out: &mut BytesMut,
) {
    // Check options
    if let Some(opts) = options {
        // Handle NX: set only if not exists
        if opts.nx && db.get(&key).is_some() {
            format_null(out);
            return;
        }
        // Handle XX: set only if exists
        if opts.xx && db.get(&key).is_none() {
            format_null(out);
            return;
        }

        // Value must be set before expiration
        // But wait, if we set then fail expiration?
        // Ideally we'd have a set_ex in db.
        // For now:
        db.set(&key, value);

        // Handle expiration
        if let Some(ex) = opts.ex {
            let _ = db.expire(&key, ex);
        } else if let Some(px) = opts.px {
            let _ = db.expire(&key, px / 1000);
        }
    } else {
        db.set(&key, value);
    }

    format_simple_string(out, "OK")
}

pub fn del(db: &SharedDatabase, keys: Vec<Bytes>, out: &mut BytesMut) {
    let count = db.del(&keys);
    format_integer(out, count as i64)
}

pub fn incr(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.incr(&key) {
        Ok(val) => format_integer(out, val),
        Err(_) => format_error(out, "ERR value is not an integer or out of range"),
    }
}

pub fn decr(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.decr(&key) {
        Ok(val) => format_integer(out, val),
        Err(_) => format_error(out, "ERR value is not an integer or out of range"),
    }
}

pub fn incr_by(db: &SharedDatabase, key: Bytes, value: Bytes, out: &mut BytesMut) {
    match db.incr_by(&key, value) {
        Ok(val) => format_integer(out, val),
        Err(_) => format_error(out, "ERR value is not an integer or out of range"),
    }
}

pub fn decr_by(db: &SharedDatabase, key: Bytes, value: Bytes, out: &mut BytesMut) {
    match db.decr_by(&key, value) {
        Ok(val) => format_integer(out, val),
        Err(_) => format_error(out, "ERR value is not an integer or out of range"),
    }
}

pub fn append(db: &SharedDatabase, key: Bytes, value: Bytes, out: &mut BytesMut) {
    let len = db.append(&key, value);
    format_integer(out, len as i64)
}

pub fn strlen(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    let len = db.str_len(&key);
    format_integer(out, len as i64)
}

pub fn mget(db: &SharedDatabase, keys: Vec<Bytes>, out: &mut BytesMut) {
    let _ = write!(out, "*{}\r\n", keys.len());
    for key in keys {
        match db.get(&key) {
            Some(val) => format_bulk_string(out, &val),
            None => format_null(out),
        }
    }
}

pub fn mset(db: &SharedDatabase, pairs: Vec<(Bytes, Bytes)>, out: &mut BytesMut) {
    for (key, value) in pairs {
        db.set(&key, value);
    }
    format_simple_string(out, "OK")
}

pub fn setnx(db: &SharedDatabase, key: Bytes, value: Bytes, out: &mut BytesMut) {
    if db.get(&key).is_some() {
        format_integer(out, 0)
    } else {
        db.set(&key, value);
        format_integer(out, 1)
    }
}

pub fn setex(db: &SharedDatabase, key: Bytes, seconds: Bytes, value: Bytes, out: &mut BytesMut) {
    let seconds_str = String::from_utf8_lossy(&seconds);
    match seconds_str.parse::<u64>() {
        Ok(s) => {
            db.set(&key, value);
            let _ = db.expire(&key, s);
            format_simple_string(out, "OK")
        }
        Err(_) => format_error(out, "ERR value is not an integer or out of range"),
    }
}

pub fn getset(db: &SharedDatabase, key: Bytes, value: Bytes, out: &mut BytesMut) {
    match db.get(&key) {
        Some(old_val) => {
            db.set(&key, value);
            format_bulk_string(out, &old_val)
        }
        None => {
            db.set(&key, value);
            format_null(out)
        }
    }
}
