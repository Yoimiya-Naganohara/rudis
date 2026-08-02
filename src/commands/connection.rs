use crate::commands::command_helper::{format_bulk_string, format_error, format_simple_string};
use crate::database::traits::KeyOp;
use crate::database::SharedDatabase;
use bytes::{Bytes, BytesMut};

pub fn ping(msg: Option<Bytes>, out: &mut BytesMut) {
    match msg {
        None => format_simple_string(out, "PONG"),
        Some(msg) => {
            // Redis PING returns the argument as bulk string if present
            format_bulk_string(out, &msg)
        }
    }
}

pub fn echo(msg: Bytes, out: &mut BytesMut) {
    format_bulk_string(out, &msg)
}

pub fn select(db: &SharedDatabase, db_index: Bytes, out: &mut BytesMut) {
    let db_idx_str = match std::str::from_utf8(&db_index) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, "ERR invalid DB index");
            return;
        }
    };

    match db_idx_str.parse::<u8>() {
        // Validate against the actual DB count so the command layer agrees
        // with Database::select (which refuses out-of-range indices).
        Ok(db_num) if (db_num as usize) < db.data.len() => {
            db.select(db_num);
            format_simple_string(out, "OK")
        }
        _ => format_error(out, "ERR invalid DB index"),
    }
}

pub fn auth(_: Bytes, out: &mut BytesMut) {
    format_simple_string(out, "OK")
}

pub fn info(_: Option<Bytes>, out: &mut BytesMut) {
    format_bulk_string(out, &Bytes::from("# Server\r\nredis_version:6.0.0\r\n"))
}

pub fn quit() {}
