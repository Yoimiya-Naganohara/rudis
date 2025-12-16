use crate::commands::command_helper::{
    format_array_bytes, format_bulk_string, format_error, format_integer, format_simple_string,
};
use crate::database::{KeyOp, SharedDatabase};
use bytes::Bytes;

pub fn exists(db: &SharedDatabase, keys: Vec<Bytes>) -> Bytes {
    format_integer(db.exist(&keys) as i64)
}

pub fn expire(db: &SharedDatabase, key: Bytes, seconds: Bytes) -> Bytes {
    // Parse seconds from Bytes
    let secs_str = match std::str::from_utf8(&seconds) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };

    match secs_str.parse::<u64>() {
        Ok(s) => match db.expire(&key, s) {
            Ok(()) => format_simple_string("OK"),
            Err(e) => format_error(e),
        },
        Err(_) => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn ttl(db: &SharedDatabase, key: Bytes) -> Bytes {
    format_integer(db.ttl(&key))
}

pub fn type_(db: &SharedDatabase, key: Bytes) -> Bytes {
    // db.data_type now accepts &Bytes
    format_simple_string(&db.data_type(&key))
}

pub fn keys(db: &SharedDatabase, pattern: Bytes) -> Bytes {
    match db.keys(&pattern) {
        Ok(keys) => format_array_bytes(keys.into_iter().map(|k| format_bulk_string(&k)).collect()),
        Err(e) => format_error(e),
    }
}

pub fn flushall(db: &SharedDatabase) -> Bytes {
    db.flush_all();
    format_simple_string("OK")
}

pub fn flushdb(db: &SharedDatabase) -> Bytes {
    db.flush_db();
    format_simple_string("OK")
}
