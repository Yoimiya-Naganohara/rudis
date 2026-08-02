use crate::commands::command_helper::{format_array_bytes, format_error, format_integer};
use crate::database::traits::SetOp;
use crate::database::SharedDatabase;
use bytes::{Bytes, BytesMut};

pub fn sadd(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>, out: &mut BytesMut) {
    format_integer(out, db.sadd(&key, &values) as i64)
}

pub fn srem(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>, out: &mut BytesMut) {
    format_integer(out, db.srem(&key, &values) as i64)
}

pub fn smembers(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.smembers(&key) {
        Ok(value) => format_array_bytes(out, value),
        Err(e) => format_error(out, e),
    }
}

pub fn scard(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    format_integer(out, db.scard(&key) as i64)
}

pub fn sismember(db: &SharedDatabase, key: Bytes, member: Bytes, out: &mut BytesMut) {
    format_integer(out, db.sismember(&key, &member) as i64)
}

pub fn sinter(db: &SharedDatabase, keys: Vec<Bytes>, out: &mut BytesMut) {
    match db.sinter(&keys) {
        Ok(res) => format_array_bytes(out, res),
        Err(e) => format_error(out, e),
    }
}

pub fn sunion(db: &SharedDatabase, keys: Vec<Bytes>, out: &mut BytesMut) {
    match db.sunion(&keys) {
        Ok(res) => format_array_bytes(out, res),
        Err(e) => format_error(out, e),
    }
}

pub fn sdiff(db: &SharedDatabase, keys: Vec<Bytes>, out: &mut BytesMut) {
    match db.sdiff(&keys) {
        Ok(res) => format_array_bytes(out, res),
        Err(e) => format_error(out, e),
    }
}
