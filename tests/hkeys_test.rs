// Test for HKEYS functionality

use bytes::Bytes;
use rudis::commands::CommandError;
use rudis::database::{Database, HashOp, StringOp};

#[test]
fn test_hkeys_functionality() {
    let mut db = Database::new(16);

    // Test HKEYS on non-existent hash
    let result = db.hkeys(&Bytes::from("nonexistent"));
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

    // Test HKEYS returns only the field names
    let keys_result = db.hkeys(&Bytes::from("user:1")).unwrap();
    assert_eq!(keys_result.len(), 3);

    // Convert to owned strings for easier comparison
    let mut keys: Vec<String> = keys_result
        .iter()
        .map(|s| String::from_utf8(s.to_vec()).unwrap())
        .collect();
    keys.sort(); // Sort for consistent comparison

    let mut expected = vec!["name".to_string(), "age".to_string(), "city".to_string()];
    expected.sort();

    assert_eq!(keys, expected);

    // Verify HKEYS only returns keys, not values
    assert!(!keys.contains(&"Alice".to_string()));
    assert!(!keys.contains(&"25".to_string()));
    assert!(!keys.contains(&"NYC".to_string()));

    // Test HKEYS vs HGETALL difference
    let getall_result = db.hget_all(&Bytes::from("user:1")).unwrap();
    assert_eq!(getall_result.len(), 6); // Should have 6 items (3 keys + 3 values)
    assert_eq!(keys_result.len(), 3); // Should have 3 items (only keys)
}

#[test]
fn test_hkeys_type_error() {
    let mut db = Database::new(16);

    // Set a string value
    db.set(&Bytes::from("mystring"), Bytes::from("value"));

    // Try HKEYS on string - should return type error
    let result = db.hkeys(&Bytes::from("mystring"));
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), CommandError::WrongType);
}
