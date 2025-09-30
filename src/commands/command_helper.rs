use crate::{commands::SetOptions, networking::resp::RespValue};

// Helper function to extract BulkString value
pub fn extract_bulk_string(resp_value: &RespValue) -> Option<String> {
    match resp_value {
        RespValue::BulkString(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

// Helper function to extract multiple BulkString values
pub fn extract_bulk_strings(elements: &[RespValue]) -> Option<Vec<String>> {
    elements.iter().map(extract_bulk_string).collect()
}

// Helper function for commands with single key
pub fn parse_single_key_command(elements: &[RespValue], expected_len: usize) -> Option<String> {
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
) -> Option<(String, String)> {
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
) -> Option<(String, String, String)> {
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
pub fn parse_keys_command(elements: &[RespValue], min_required_len: usize) -> Option<Vec<String>> {
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
) -> Option<(String, Vec<String>)> {
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
) -> Option<Vec<(String, String)>> {
    if elements.len() >= min_required_len && elements.len() % 2 == 1 {
        extract_key_value_strings(&elements[1..])
    } else {
        None
    }
}
pub fn parse_key_pair_values_command(
    elements: &[RespValue],
    min_required_len: usize,
) -> Option<(String, Vec<(String, String)>)> {
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
) -> Option<(String, String, Option<SetOptions>)> {
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
                let opt = extract_bulk_string(&elements[i])?.to_uppercase();
                match opt.as_str() {
                    "NX" => opts.nx = true,
                    "XX" => opts.xx = true,
                    "EX" => {
                        if i + 1 < elements.len() {
                            opts.ex = extract_bulk_string(&elements[i + 1])?.parse().ok();
                            i += 1;
                        }
                    }
                    "PX" => {
                        if i + 1 < elements.len() {
                            opts.px = extract_bulk_string(&elements[i + 1])?.parse().ok();
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
) -> Option<(String, String, String, String)> {
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
pub fn extract_key_value_strings(elements: &[RespValue]) -> Option<Vec<(String, String)>> {
    elements
        .chunks(2)
        .into_iter()
        .map(|value| {
            if value.len() == 2 {
                if let (RespValue::BulkString(Some(key)), RespValue::BulkString(Some(val))) =
                    (&value[0], &value[1])
                {
                    Some((key.clone(), val.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Option<Vec<_>>>()
}

// Helper functions for response formatting
pub fn format_integer(value: i64) -> String {
    format!(":{}\r\n", value)
}

pub fn format_array(elements: Vec<String>) -> String {
    let mut result = format!("*{}\r\n", elements.len());
    for element in elements {
        result.push_str(&element);
    }
    result
}

pub fn format_error(error: impl std::fmt::Display) -> String {
    format!("-ERR {}\r\n", error)
}

pub fn format_bulk_string(value: &str) -> String {
    format!("${}\r\n{}\r\n", value.len(), value)
}

pub fn format_null() -> String {
    "$-1\r\n".to_string()
}

pub fn format_simple_string(value: &str) -> String {
    format!("+{}\r\n", value)
}

pub fn format_hash_response(value: Vec<&String>) -> String {
    let mut result = format!("*{}\r\n", value.len());
    for item in value {
        result.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
    }
    result
}
