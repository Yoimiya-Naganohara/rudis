use crate::commands::command_helper::{
    format_array_bytes, format_bulk_string, format_error, format_integer, format_null,
};
use crate::database::{SharedDatabase, SortedSetOp};
use bytes::Bytes;

pub fn zadd(db: &SharedDatabase, key: Bytes, pairs: Vec<(Bytes, Bytes)>) -> Bytes {
    // Parse scores from Bytes to f64
    let mut parsed_pairs = Vec::with_capacity(pairs.len());
    for (score_bytes, member) in pairs {
        let score_str = match std::str::from_utf8(&score_bytes) {
            Ok(s) => s,
            Err(_) => return format_error(crate::commands::CommandError::InvalidFloat),
        };
        match score_str.parse::<f64>() {
            Ok(score) => parsed_pairs.push((score, member)),
            Err(_) => return format_error(crate::commands::CommandError::InvalidFloat),
        }
    }

    let added = db.zadd(&key, &parsed_pairs);
    format_integer(added as i64)
}

pub fn zrem(db: &SharedDatabase, key: Bytes, members: Vec<Bytes>) -> Bytes {
    let removed = db.zrem(&key, &members);
    format_integer(removed as i64)
}

pub fn zrange(db: &SharedDatabase, key: Bytes, start: Bytes, stop: Bytes) -> Bytes {
    let start_str = match std::str::from_utf8(&start) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };
    let stop_str = match std::str::from_utf8(&stop) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidInteger),
    };

    match (start_str.parse::<i64>(), stop_str.parse::<i64>()) {
        (Ok(s), Ok(e)) => match db.zrange(&key, s, e) {
            Ok(members) => format_array_bytes(members),
            Err(e) => format_error(e),
        },
        _ => format_error(crate::commands::CommandError::InvalidInteger),
    }
}

pub fn zrangebyscore(db: &SharedDatabase, key: Bytes, min: Bytes, max: Bytes) -> Bytes {
    let min_str = match std::str::from_utf8(&min) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidFloat),
    };
    let max_str = match std::str::from_utf8(&max) {
        Ok(s) => s,
        Err(_) => return format_error(crate::commands::CommandError::InvalidFloat),
    };

    match (min_str.parse::<f64>(), max_str.parse::<f64>()) {
        (Ok(mn), Ok(mx)) => match db.zrange_by_score(&key, mn, mx) {
            Ok(members) => format_array_bytes(members),
            Err(e) => format_error(e),
        },
        _ => format_error(crate::commands::CommandError::InvalidFloat),
    }
}

pub fn zcard(db: &SharedDatabase, key: Bytes) -> Bytes {
    format_integer(db.zcard(&key) as i64)
}

pub fn zscore(db: &SharedDatabase, key: Bytes, member: Bytes) -> Bytes {
    match db.zscore(&key, &member) {
        Some(score) => format_bulk_string(&Bytes::from(score.to_string())),
        None => format_null(),
    }
}

pub fn zrank(db: &SharedDatabase, key: Bytes, member: Bytes) -> Bytes {
    match db.zrank(&key, &member) {
        Some(rank) => format_integer(rank as i64),
        None => format_null(),
    }
}
