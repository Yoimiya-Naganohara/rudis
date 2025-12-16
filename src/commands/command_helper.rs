use crate::{commands::SetOptions, networking::resp::RespValue};
use bytes::{BufMut, Bytes, BytesMut};

// Helper function to extract BulkString value
pub fn extract_bulk_string(resp_value: &RespValue) -> Option<Bytes> {
    match resp_value {
        RespValue::BulkString(bytes) => Some(bytes.clone()),
        RespValue::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

// Helper function to extract multiple BulkString values
pub fn extract_bulk_strings(elements: &[RespValue]) -> Option<Vec<Bytes>> {
    elements.iter().map(extract_bulk_string).collect()
}

// Helper function for commands with single key
pub fn parse_single_key_command(elements: &[RespValue], expected_len: usize) -> Option<Bytes> {
    if elements.len() == expected_len {
        extract_bulk_string(&elements[1])
    } else {
        None
    }
}

// Helper function for commands with key and value
pub fn parse_key_value_command(
    elements: &[RespValue],
    expected_len: usize,
) -> Option<(Bytes, Bytes)> {
    if elements.len() == expected_len {
        let key = extract_bulk_string(&elements[1])?;
        let value = extract_bulk_string(&elements[2])?;
        Some((key, value))
    } else {
        None
    }
}

// Helper function for commands with key, field, and value
pub fn parse_key_field_value_command(
    elements: &[RespValue],
    expected_len: usize,
) -> Option<(Bytes, Bytes, Bytes)> {
    if elements.len() == expected_len {
        let key = extract_bulk_string(&elements[1])?;
        let field = extract_bulk_string(&elements[2])?;
        let value = extract_bulk_string(&elements[3])?;
        Some((key, field, value))
    } else {
        None
    }
}

// Helper function for commands with multiple keys
pub fn parse_keys_command(elements: &[RespValue], min_required_len: usize) -> Option<Vec<Bytes>> {
    if elements.len() >= min_required_len {
        extract_bulk_strings(&elements[1..])
    } else {
        None
    }
}

// Helper function for commands with key and multiple fields
pub fn parse_key_fields_command(
    elements: &[RespValue],
    min_required_len: usize,
) -> Option<(Bytes, Vec<Bytes>)> {
    if elements.len() >= min_required_len {
        let key = extract_bulk_string(&elements[1])?;
        let fields = extract_bulk_strings(&elements[2..])?;
        Some((key, fields))
    } else {
        None
    }
}

// Helper function for commands with multiple key-value pairs
pub fn parse_keys_values_command(
    elements: &[RespValue],
    min_required_len: usize,
) -> Option<Vec<(Bytes, Bytes)>> {
    if elements.len() >= min_required_len && elements.len() % 2 == 1 {
        extract_key_value_strings(&elements[1..])
    } else {
        None
    }
}
pub fn parse_key_pair_values_command(
    elements: &[RespValue],
    min_required_len: usize,
) -> Option<(Bytes, Vec<(Bytes, Bytes)>)> {
    if elements.len() >= min_required_len && elements.len() % 2 == 1 {
        let key = extract_bulk_string(&elements[1])?;
        let pairs = extract_key_value_strings(&elements[2..])?;
        Some((key, pairs))
    } else {
        None
    }
}
pub fn parse_key_value_options_command(
    elements: &[RespValue],
    min_required_len: usize,
) -> Option<(Bytes, Bytes, Option<SetOptions>)> {
    if elements.len() >= min_required_len {
        let key = extract_bulk_string(&elements[1])?;
        let value = extract_bulk_string(&elements[2])?;
        let mut options = None;
        if elements.len() >= 4 {
            let mut opts = SetOptions {
                nx: false,
                xx: false,
                ex: None,
                px: None,
                keepttl: false,
            };
            let mut i = 3;
            while i < elements.len() {
                // Convert to string for option parsing (options are ASCII)
                let opt_bytes = extract_bulk_string(&elements[i])?;
                let opt = String::from_utf8_lossy(&opt_bytes).to_uppercase();
                match opt.as_str() {
                    "NX" => opts.nx = true,
                    "XX" => opts.xx = true,
                    "EX" => {
                        if i + 1 < elements.len() {
                            let val_bytes = extract_bulk_string(&elements[i + 1])?;
                            let val_str = String::from_utf8_lossy(&val_bytes);
                            opts.ex = val_str.parse().ok();
                            i += 1;
                        }
                    }
                    "PX" => {
                        if i + 1 < elements.len() {
                            let val_bytes = extract_bulk_string(&elements[i + 1])?;
                            let val_str = String::from_utf8_lossy(&val_bytes);
                            opts.px = val_str.parse().ok();
                            i += 1;
                        }
                    }
                    "KEEPTTL" => opts.keepttl = true,
                    _ => {}
                }
                i += 1;
            }
            options = Some(opts);
        }
        Some((key, value, options))
    } else {
        None
    }
}
pub fn parse_key_ord_pivot_value_command(
    elements: &[RespValue],
    expected_len: usize,
) -> Option<(Bytes, Bytes, Bytes, Bytes)> {
    if elements.len() == expected_len {
        Some((
            extract_bulk_string(&elements[1])?, // key
            extract_bulk_string(&elements[2])?, // BEFORE/AFTER
            extract_bulk_string(&elements[3])?, // pivot
            extract_bulk_string(&elements[4])?, // element
        ))
    } else {
        None
    }
}
// Helper function to extract key-value pairs from bulk strings
pub fn extract_key_value_strings(elements: &[RespValue]) -> Option<Vec<(Bytes, Bytes)>> {
    elements
        .chunks(2)
        .into_iter()
        .map(|value| {
            if value.len() == 2 {
                // Adapt to Frame variants
                let key = extract_bulk_string(&value[0])?;
                let val = extract_bulk_string(&value[1])?;
                Some((key, val))
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()
}

// Helper functions for response formatting
pub fn format_integer(value: i64) -> Bytes {
    Bytes::from(format!(":{}\r\n", value))
}

pub fn format_array(elements: Vec<String>) -> Bytes {
    // This function still takes Vec<String> which is suboptimal, but we'll adapt it for now.
    // The callers (like keys handler) construct Vec<String> from format_bulk_string (which we will change to return Bytes).
    // So we should change this to take Vec<Bytes>.
    let mut buf = BytesMut::new();
    buf.put_slice(format!("*{}\r\n", elements.len()).as_bytes());
    for element in elements {
        buf.put_slice(element.as_bytes());
    }
    buf.freeze()
}

// New signature for format_array taking Bytes
pub fn format_array_bytes(elements: Vec<Bytes>) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_slice(format!("*{}\r\n", elements.len()).as_bytes());
    for element in elements {
        buf.put_slice(&element);
    }
    buf.freeze()
}

pub fn format_error(error: impl std::fmt::Display) -> Bytes {
    Bytes::from(format!("-ERR {}\r\n", error))
}

pub fn format_bulk_string(value: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(value.len() + 20);
    buf.put_u8(b'$');
    buf.put_slice(value.len().to_string().as_bytes());
    buf.put_slice(b"\r\n");
    buf.put_slice(value);
    buf.put_slice(b"\r\n");
    buf.freeze()
}

pub fn format_null() -> Bytes {
    Bytes::from_static(b"$-1\r\n")
}

pub fn format_simple_string(value: &str) -> Bytes {
    Bytes::from(format!("+{}\r\n", value))
}

pub fn format_hash_response(value: Vec<Bytes>) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_slice(format!("*{}\r\n", value.len()).as_bytes());
    for item in value {
        // Reuse format_bulk_string logic or inline it to avoid excessive allocation if convenient,
        // but format_bulk_string returns Bytes which might be zero-copy from BytesMut if well optimized?
        // Actually format_bulk_string creates a new Bytes.
        // Better to inline the writing here for performance?
        // For simplicity let's append.
        let bulk = format_bulk_string(&item);
        buf.put_slice(&bulk);
    }
    buf.freeze()
}
