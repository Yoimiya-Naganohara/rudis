use crate::commands::command_helper::{
    format_array_bytes, format_bulk_string, format_error, format_integer, format_null,
    format_simple_string,
};
use crate::database::traits::ListOp;
use crate::database::SharedDatabase;
use bytes::{Bytes, BytesMut};

pub fn lpush(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>, out: &mut BytesMut) {
    format_integer(out, db.lpush(&key, &values) as i64)
}

pub fn rpush(db: &SharedDatabase, key: Bytes, values: Vec<Bytes>, out: &mut BytesMut) {
    format_integer(out, db.rpush(&key, &values) as i64)
}

pub fn lpop(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.lpop(&key) {
        Some(result) => format_bulk_string(out, &result),
        None => format_null(out),
    }
}

pub fn rpop(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    match db.rpop(&key) {
        Some(result) => format_bulk_string(out, &result),
        None => format_null(out),
    }
}

pub fn llen(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    format_integer(out, db.llen(&key) as i64)
}

pub fn lindex(db: &SharedDatabase, key: Bytes, index: Bytes, out: &mut BytesMut) {
    // Parse index
    let index_str = match std::str::from_utf8(&index) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };
    match index_str.parse::<i64>() {
        Ok(idx) => match db.lindex(&key, idx) {
            Some(val) => format_bulk_string(out, &val),
            None => format_null(out),
        },
        Err(_) => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn lrange(db: &SharedDatabase, key: Bytes, start: Bytes, end: Bytes, out: &mut BytesMut) {
    let start_str = match std::str::from_utf8(&start) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };
    let end_str = match std::str::from_utf8(&end) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };

    match (start_str.parse::<i64>(), end_str.parse::<i64>()) {
        (Ok(s), Ok(e)) => match db.lrange(&key, s, e) {
            Ok(val) => format_array_bytes(out, val),
            Err(e) => format_error(out, e),
        },
        _ => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn ltrim(db: &SharedDatabase, key: Bytes, start: Bytes, end: Bytes, out: &mut BytesMut) {
    let start_str = match std::str::from_utf8(&start) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };
    let end_str = match std::str::from_utf8(&end) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };

    match (start_str.parse::<i64>(), end_str.parse::<i64>()) {
        (Ok(s), Ok(e)) => match db.ltrim(&key, s, e) {
            Ok(_) => format_simple_string(out, "OK"),
            Err(e) => format_error(out, e),
        },
        _ => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn lset(db: &SharedDatabase, key: Bytes, index: Bytes, value: Bytes, out: &mut BytesMut) {
    let index_str = match std::str::from_utf8(&index) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };

    match index_str.parse::<i64>() {
        Ok(idx) => match db.lset(&key, idx, value) {
            Ok(_) => format_simple_string(out, "OK"),
            Err(e) => format_error(out, e),
        },
        Err(_) => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn linsert(
    db: &SharedDatabase,
    key: Bytes,
    ord: Bytes,
    pivot: Bytes,
    value: Bytes,
    out: &mut BytesMut,
) {
    let ord_str = String::from_utf8_lossy(&ord);
    match db.linsert(&key, &ord_str, &pivot, value) {
        Ok(val) => format_integer(out, val),
        Err(e) => format_error(out, e),
    }
}
