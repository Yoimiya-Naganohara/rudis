use crate::commands::command_helper::{
    format_array_bytes, format_bulk_string, format_error, format_integer, format_null,
};
use crate::database::traits::SortedSetOp;
use crate::database::SharedDatabase;
use bytes::{Bytes, BytesMut};

pub fn zadd(db: &SharedDatabase, key: Bytes, pairs: Vec<(Bytes, Bytes)>, out: &mut BytesMut) {
    // Parse scores from Bytes to f64
    let mut parsed_pairs = Vec::with_capacity(pairs.len());
    for (score_bytes, member) in pairs {
        let score_str = match std::str::from_utf8(&score_bytes) {
            Ok(s) => s,
            Err(_) => {
                format_error(out, crate::commands::CommandError::InvalidFloat);
                return;
            }
        };
        match score_str.parse::<f64>() {
            Ok(score) => parsed_pairs.push((score, member)),
            Err(_) => {
                format_error(out, crate::commands::CommandError::InvalidFloat);
                return;
            }
        }
    }

    let added = db.zadd(&key, &parsed_pairs);
    format_integer(out, added as i64)
}

pub fn zrem(db: &SharedDatabase, key: Bytes, members: Vec<Bytes>, out: &mut BytesMut) {
    let removed = db.zrem(&key, &members);
    format_integer(out, removed as i64)
}

pub fn zrange(db: &SharedDatabase, key: Bytes, start: Bytes, stop: Bytes, out: &mut BytesMut) {
    let start_str = match std::str::from_utf8(&start) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };
    let stop_str = match std::str::from_utf8(&stop) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidInteger);
            return;
        }
    };

    match (start_str.parse::<i64>(), stop_str.parse::<i64>()) {
        (Ok(s), Ok(e)) => match db.zrange(&key, s, e) {
            Ok(members) => format_array_bytes(out, members),
            Err(e) => format_error(out, e),
        },
        _ => format_error(out, crate::commands::CommandError::InvalidInteger),
    }
}

pub fn zrangebyscore(db: &SharedDatabase, key: Bytes, min: Bytes, max: Bytes, out: &mut BytesMut) {
    let min_str = match std::str::from_utf8(&min) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidFloat);
            return;
        }
    };
    let max_str = match std::str::from_utf8(&max) {
        Ok(s) => s,
        Err(_) => {
            format_error(out, crate::commands::CommandError::InvalidFloat);
            return;
        }
    };

    match (min_str.parse::<f64>(), max_str.parse::<f64>()) {
        (Ok(mn), Ok(mx)) => match db.zrange_by_score(&key, mn, mx) {
            Ok(members) => format_array_bytes(out, members),
            Err(e) => format_error(out, e),
        },
        _ => format_error(out, crate::commands::CommandError::InvalidFloat),
    }
}

pub fn zcard(db: &SharedDatabase, key: Bytes, out: &mut BytesMut) {
    format_integer(out, db.zcard(&key) as i64)
}

pub fn zscore(db: &SharedDatabase, key: Bytes, member: Bytes, out: &mut BytesMut) {
    match db.zscore(&key, &member) {
        Some(score) => format_bulk_string(out, &Bytes::from(score.to_string())),
        None => format_null(out),
    }
}

pub fn zrank(db: &SharedDatabase, key: Bytes, member: Bytes, out: &mut BytesMut) {
    match db.zrank(&key, &member) {
        Some(rank) => format_integer(out, rank as i64),
        None => format_null(out),
    }
}
