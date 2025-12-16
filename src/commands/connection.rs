use crate::commands::command_helper::{format_bulk_string, format_error, format_simple_string};
use crate::database::traits::KeyOp;
use crate::database::SharedDatabase;
use bytes::Bytes;

pub fn ping(msg: Option<Bytes>) -> Bytes {
    match msg {
        None => format_simple_string("PONG"),
        Some(msg) => {
            // Redis PING returns the argument as bulk string if present
            format_bulk_string(&msg)
        }
    }
}

pub fn echo(msg: Bytes) -> Bytes {
    format_bulk_string(&msg)
}

pub fn select(db: &SharedDatabase, db_index: Bytes) -> Bytes {
    let db_idx_str = match std::str::from_utf8(&db_index) {
        Ok(s) => s,
        Err(_) => return format_error("ERR invalid DB index"),
    };

    match db_idx_str.parse::<u8>() {
        Ok(db_num) if db_num <= 15 => {
            db.select(db_num);
            format_simple_string("OK")
        }
        _ => format_error("ERR invalid DB index"),
    }
}

pub fn auth(_: Bytes) -> Bytes {
    format_simple_string("OK")
}

pub fn info(_: Option<Bytes>) -> Bytes {
    format_bulk_string(&Bytes::from("# Server\r\nredis_version:6.0.0\r\n"))
}

pub fn quit() -> Bytes {
    Bytes::from_static(b"#QUIT")
}
