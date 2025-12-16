use crate::{
    commands::{
        command_helper::{
            format_array_bytes, format_bulk_string, format_error, format_integer, format_null,
            format_simple_string,
        },
        SetOptions,
    },
    database::traits::{KeyOp, StringOp},
    database::SharedDatabase,
};
use bytes::Bytes;

pub fn get(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.get(&key) {
        Some(value) => format_bulk_string(&value),
        None => format_null(),
    }
}

pub fn set(db: &SharedDatabase, key: Bytes, value: Bytes, options: Option<SetOptions>) -> Bytes {
    // Check options
    if let Some(opts) = options {
        // Handle NX: set only if not exists
        if opts.nx {
            if db.get(&key).is_some() {
                return format_null();
            }
        }
        // Handle XX: set only if exists
        if opts.xx {
            if db.get(&key).is_none() {
                return format_null();
            }
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

    format_simple_string("OK")
}

pub fn del(db: &SharedDatabase, keys: Vec<Bytes>) -> Bytes {
    let count = db.del(&keys);
    format_integer(count as i64)
}

pub fn incr(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.incr(&key) {
        Ok(val) => format_integer(val),
        Err(_) => format_error("ERR value is not an integer or out of range"),
    }
}

pub fn decr(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.decr(&key) {
        Ok(val) => format_integer(val),
        Err(_) => format_error("ERR value is not an integer or out of range"),
    }
}

pub fn incr_by(db: &SharedDatabase, key: Bytes, value: Bytes) -> Bytes {
    match db.incr_by(&key, value) {
        Ok(val) => format_integer(val),
        Err(_) => format_error("ERR value is not an integer or out of range"),
    }
}

pub fn decr_by(db: &SharedDatabase, key: Bytes, value: Bytes) -> Bytes {
    match db.decr_by(&key, value) {
        Ok(val) => format_integer(val),
        Err(_) => format_error("ERR value is not an integer or out of range"),
    }
}

pub fn append(db: &SharedDatabase, key: Bytes, value: Bytes) -> Bytes {
    let len = db.append(&key, value);
    format_integer(len as i64)
}

pub fn strlen(db: &SharedDatabase, key: Bytes) -> Bytes {
    let len = db.str_len(&key);
    format_integer(len as i64)
}

pub fn mget(db: &SharedDatabase, keys: Vec<Bytes>) -> Bytes {
    let mut response = Vec::new();
    for key in keys {
        match db.get(&key) {
            Some(val) => response.push(format_bulk_string(&val)),
            None => response.push(format_null()),
        }
    }
    format_array_bytes(response)
}

pub fn mset(db: &SharedDatabase, pairs: Vec<(Bytes, Bytes)>) -> Bytes {
    for (key, value) in pairs {
        db.set(&key, value);
    }
    format_simple_string("OK")
}

pub fn setnx(db: &SharedDatabase, key: Bytes, value: Bytes) -> Bytes {
    if db.get(&key).is_some() {
        format_integer(0)
    } else {
        db.set(&key, value);
        format_integer(1)
    }
}

pub fn setex(db: &SharedDatabase, key: Bytes, seconds: Bytes, value: Bytes) -> Bytes {
    let seconds_str = String::from_utf8_lossy(&seconds);
    match seconds_str.parse::<u64>() {
        Ok(s) => {
            db.set(&key, value);
            let _ = db.expire(&key, s);
            format_simple_string("OK")
        }
        Err(_) => format_error("ERR value is not an integer or out of range"),
    }
}

pub fn getset(db: &SharedDatabase, key: Bytes, value: Bytes) -> Bytes {
    match db.get(&key) {
        Some(old_val) => {
            db.set(&key, value);
            format_bulk_string(&old_val)
        }
        None => {
            db.set(&key, value);
            format_null()
        }
    }
}
