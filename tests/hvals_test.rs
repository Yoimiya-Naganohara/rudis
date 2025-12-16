// Test for HVALS functionality

use bytes::Bytes;
use rudis::commands::CommandError;
use rudis::database::{Database, traits::{HashOp, StringOp}};

#[test]
fn test_hvals_functionality() {
    let mut db = Database::new(16);

    // Test HVALS on non-existent hash
    let result = db.hvals(&Bytes::from("nonexistent"));
    assert_eq!(result, Ok(Vec::new()));

    // Set up a hash with multiple fields
    assert_eq!(
        db.hset(
            &Bytes::from("user:1"),
            Bytes::from("name"),
            Bytes::from("Alice")
        ),
        Ok(1)
    );
    assert_eq!(
        db.hset(
            &Bytes::from("user:1"),
            Bytes::from("age"),
            Bytes::from("25")
        ),
        Ok(1)
    );
    assert_eq!(
        db.hset(
            &Bytes::from("user:1"),
            Bytes::from("city"),
            Bytes::from("NYC")
        ),
        Ok(1)
    );

    // Test HVALS returns only the values
    let values_result = db.hvals(&Bytes::from("user:1")).unwrap();
    assert_eq!(values_result.len(), 3);

    // Convert to owned strings for easier comparison
    let mut values: Vec<String> = values_result
        .iter()
        .map(|s| String::from_utf8(s.to_vec()).unwrap())
        .collect();
    values.sort(); // Sort for consistent comparison

    let mut expected = vec!["Alice".to_string(), "25".to_string(), "NYC".to_string()];
    expected.sort();

    assert_eq!(values, expected);

    // Verify HVALS only returns values, not keys
    assert!(!values.contains(&"name".to_string()));
    assert!(!values.contains(&"age".to_string()));
    assert!(!values.contains(&"city".to_string()));

    // Test HVALS vs HGETALL difference
    let getall_result = db.hget_all(&Bytes::from("user:1")).unwrap();
    assert_eq!(getall_result.len(), 6); // Should have 6 items (3 keys + 3 values)
    assert_eq!(values_result.len(), 3); // Should have 3 items (only values)
}

#[test]
fn test_hvals_type_error() {
    let mut db = Database::new(16);

    // Set a string value
    db.set(&Bytes::from("mystring"), Bytes::from("value"));

    // Try HVALS on string - should return type error
    let result = db.hvals(&Bytes::from("mystring"));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), CommandError::WrongType);
}
