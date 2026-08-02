use crate::commands::command_helper::{
    format_array_bytes, format_error, format_integer, format_simple_string,
};
use crate::database::traits::KeyOp;
use crate::database::SharedDatabase;
use bytes::{Bytes, BytesMut};

pub fn exists(db: &SharedDatabase, keys: Vec<Bytes>, out: &mut BytesMut) {
    format_integer(out, db.exist(&keys) as i64)
}

pub fn expire(db: &SharedDatabase, key: Bytes, seconds: Bytes, out: &mut BytesMut) {
    // Parse seconds from Bytes
    let secs_str = match std::str::from_utf8(&seconds) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };

    match secs_str.parse::<u64>() {
        Ok(s) => match db.expire(&key, s) {
            Ok(()) => format_simple_string(out, "OK"),
            Err(e) => format_error(out, e),
        },
        Err(_) => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn ttl(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    format_integer(out, db.ttl(&key))
}

pub fn type_(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    // db.data_type now accepts &Bytes
    format_simple_string(out, db.data_type(&key))
}

pub fn keys(db: &SharedDatabase, pattern: Bytes, out: &mut BytesMut) {
    match db.keys(&pattern) {
        Ok(keys) => format_array_bytes(out, keys),
        Err(e) => format_error(out, e),
    }
}

pub fn flushall(db: &SharedDatabase, out: &mut BytesMut) {
    db.flush_all();
    format_simple_string(out, "OK")
}

pub fn flushdb(db: &SharedDatabase, out: &mut BytesMut) {
    db.flush_db();
    format_simple_string(out, "OK")
}
