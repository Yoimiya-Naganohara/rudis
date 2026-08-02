use crate::{commands::SetOptions, networking::resp::RespValue};
use bytes::{BufMut, Bytes, BytesMut};
use std::fmt::Write;

// Helper function to extract BulkString value
pub fn extract_bulk_string(resp_value: &RespValue) -> Option<Bytes> {
    match resp_value {
        RespValue::BulkString(bytes) => Some(bytes.clone()),
        RespValue::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

// Helper function to get BulkString bytes as a slice, without cloning
pub fn bulk_string_bytes(resp_value: &RespValue) -> Option<&[u8]> {
    match resp_value {
        RespValue::BulkString(bytes) => Some(bytes.as_ref()),
        RespValue::SimpleString(s) => Some(s.as_ref()),
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
    // key + 2n pair elements after the command name, so the total element
    // count is always even (name + key + 2n)
    if elements.len() >= min_required_len && elements.len().is_multiple_of(2) {
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

// Helper functions for response formatting.
// All writers append into `out` so replies accumulate directly in the
// connection's response buffer without intermediate Bytes allocations.

pub fn format_integer(out: &mut BytesMut, value: i64) {
    let _ = write!(out, ":{}\r\n", value);
}

pub fn format_array_bytes(out: &mut BytesMut, elements: Vec<Bytes>) {
    let _ = write!(out, "*{}\r\n", elements.len());
    for element in elements {
        format_bulk_string(out, &element);
    }
}

pub fn format_error(out: &mut BytesMut, error: impl std::fmt::Display) {
    // The Display impl of CommandError (and the error string literals)
    // already carries the leading "ERR ", so only the "-" prefix is added
    // here to form a RESP error reply.
    let _ = write!(out, "-{error}\r\n");
}

pub fn format_bulk_string(out: &mut BytesMut, value: &Bytes) {
    let _ = write!(out, "${}\r\n", value.len());
    out.put_slice(value);
    out.put_slice(b"\r\n");
}

pub fn format_null(out: &mut BytesMut) {
    out.put_slice(b"$-1\r\n");
}

pub fn format_simple_string(out: &mut BytesMut, value: &str) {
    let _ = write!(out, "+{}\r\n", value);
}

pub fn format_hash_response(out: &mut BytesMut, value: Vec<Bytes>) {
    let _ = write!(out, "*{}\r\n", value.len());
    for item in value {
        format_bulk_string(out, &item);
    }
}
