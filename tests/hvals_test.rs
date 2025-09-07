// Test for HVALS functionality

use rudis::database::{Database, HashOp, StringOp};
use rudis::commands::CommandError;

#[test]
fn test_hvals_functionality() {
    let mut db = Database::new();

    // Test HVALS on non-existent hash
    let result = db.hvals("nonexistent");
    assert_eq!(result, Ok(Vec::new()));

    // Set up a hash with multiple fields
    assert_eq!(db.hset("user:1", "name", "Alice"), Ok(1));
    assert_eq!(db.hset("user:1", "age", "25"), Ok(1));
    assert_eq!(db.hset("user:1", "city", "NYC"), Ok(1));

    // Test HVALS returns only the values
    let values_result = db.hvals("user:1").unwrap();
    assert_eq!(values_result.len(), 3);

    // Convert to owned strings for easier comparison
    let mut values: Vec<String> = values_result.iter().map(|s| s.to_string()).collect();
    values.sort(); // Sort for consistent comparison

    let mut expected = vec!["Alice".to_string(), "25".to_string(), "NYC".to_string()];
    expected.sort();

    assert_eq!(values, expected);

    // Verify HVALS only returns values, not keys
    assert!(!values.contains(&"name".to_string()));
    assert!(!values.contains(&"age".to_string()));
    assert!(!values.contains(&"city".to_string()));

    // Test HVALS vs HGETALL difference
    let getall_result = db.hget_all("user:1").unwrap();
    assert_eq!(getall_result.len(), 6); // Should have 6 items (3 keys + 3 values)
    assert_eq!(values_result.len(), 3);   // Should have 3 items (only values)
}

#[test]
fn test_hvals_type_error() {
    let mut db = Database::new();

    // Set a string value
    db.set("mystring", "value".to_string());

    // Try HVALS on string - should return type error
    let result = db.hvals("mystring");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), CommandError::WrongType);
}
