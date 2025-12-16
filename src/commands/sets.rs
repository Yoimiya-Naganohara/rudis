use crate::commands::command_helper::{format_array_bytes, format_error, format_integer};
use crate::database::traits::SetOp;
use crate::database::SharedDatabase;
use bytes::Bytes;

pub fn sadd(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>) -> Bytes {
    format_integer(db.sadd(&key, &values) as i64)
}

pub fn srem(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>) -> Bytes {
    format_integer(db.srem(&key, &values) as i64)
}

pub fn smembers(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.smembers(&key) {
        Ok(value) => format_array_bytes(value),
        Err(e) => format_error(e),
    }
}

pub fn scard(db: &SharedDatabase, key: Bytes) -> Bytes {
    format_integer(db.scard(&key) as i64)
}

pub fn sismember(db: &SharedDatabase, key: Bytes, member: Bytes) -> Bytes {
    format_integer(db.sismember(&key, &member) as i64)
}

pub fn sinter(db: &SharedDatabase, keys: Vec<Bytes>) -> Bytes {
    match db.sinter(&keys) {
        Ok(res) => format_array_bytes(res),
        Err(e) => format_error(e),
    }
}

pub fn sunion(db: &SharedDatabase, keys: Vec<Bytes>) -> Bytes {
    match db.sunion(&keys) {
        Ok(res) => format_array_bytes(res),
        Err(e) => format_error(e),
    }
}

pub fn sdiff(db: &SharedDatabase, keys: Vec<Bytes>) -> Bytes {
    match db.sdiff(&keys) {
        Ok(res) => format_array_bytes(res),
        Err(e) => format_error(e),
    }
}
