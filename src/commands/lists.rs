use crate::commands::command_helper::{
    format_array_bytes, format_bulk_string, format_error, format_integer, format_null,
    format_simple_string,
};
use crate::database::{ListOp, SharedDatabase};
use bytes::Bytes;

pub fn lpush(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>) -> Bytes {
    format_integer(db.lpush(&key, &values) as i64)
}

pub fn rpush(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>) -> Bytes {
    format_integer(db.rpush(&key, &values) as i64)
}

pub fn lpop(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.lpop(&key) {
        Some(result) => format_bulk_string(&result),
        None => format_null(),
    }
}

pub fn rpop(db: &SharedDatabase, key: Bytes) -> Bytes {
    match db.rpop(&key) {
        Some(result) => format_bulk_string(&result),
        None => format_null(),
    }
}

pub fn llen(db: &SharedDatabase, key: Bytes) -> Bytes {
    format_integer(db.llen(&key) as i64)
}

pub fn lindex(db: &SharedDatabase, key: Bytes, index: Bytes) -> Bytes {
    // Parse index
    let index_str = match std::str::from_utf8(&index) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };
    match index_str.parse::<i64>() {
        Ok(idx) => match db.lindex(&key, idx) {
            Some(val) => format_bulk_string(&val),
            None => format_null(),
        },
        Err(_) => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn lrange(db: &SharedDatabase, key: Bytes, start: Bytes, end: Bytes) -> Bytes {
    let start_str = match std::str::from_utf8(&start) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };
    let end_str = match std::str::from_utf8(&end) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };

    match (start_str.parse::<i64>(), end_str.parse::<i64>()) {
        (Ok(s), Ok(e)) => match db.lrange(&key, s, e) {
            Ok(val) => format_array_bytes(val),
            Err(e) => format_error(e),
        },
        _ => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn ltrim(db: &SharedDatabase, key: Bytes, start: Bytes, end: Bytes) -> Bytes {
    let start_str = match std::str::from_utf8(&start) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };
    let end_str = match std::str::from_utf8(&end) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };

    match (start_str.parse::<i64>(), end_str.parse::<i64>()) {
        (Ok(s), Ok(e)) => match db.ltrim(&key, s, e) {
            Ok(_) => format_simple_string("OK"),
            Err(e) => format_error(e),
        },
        _ => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn lset(db: &SharedDatabase, key: Bytes, index: Bytes, value: Bytes) -> Bytes {
    let index_str = match std::str::from_utf8(&index) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };

    match index_str.parse::<i64>() {
        Ok(idx) => match db.lset(&key, idx, value) {
            Ok(_) => format_simple_string("OK"),
            Err(e) => format_error(e),
        },
        Err(_) => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn linsert(db: &SharedDatabase, key: Bytes, ord: Bytes, pivot: Bytes, value: Bytes) -> Bytes {
    let ord_str = String::from_utf8_lossy(&ord);
    match db.linsert(&key, &ord_str, &pivot, value) {
        Ok(val) => format_integer(val),
        Err(e) => format_error(e),
    }
}
