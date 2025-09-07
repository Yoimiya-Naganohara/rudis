// Test for HKEYS functionality

use rudis::database::{Database, HashOp, StringOp};
use rudis::commands::CommandError;

#[test]
fn test_hkeys_functionality() {
    let mut db = Database::new();
    
    // Test HKEYS on non-existent hash
    let result = db.hkeys("nonexistent");
    assert_eq!(result, Ok(Vec::new()));
    
    // Set up a hash with multiple fields
    assert_eq!(db.hset("user:1", "name", "Alice"), Ok(1));
    assert_eq!(db.hset("user:1", "age", "25"), Ok(1));
    assert_eq!(db.hset("user:1", "city", "NYC"), Ok(1));
    
    // Test HKEYS returns only the field names
    let keys_result = db.hkeys("user:1").unwrap();
    assert_eq!(keys_result.len(), 3);
    
    // Convert to owned strings for easier comparison
    let mut keys: Vec<String> = keys_result.iter().map(|s| s.to_string()).collect();
    keys.sort(); // Sort for consistent comparison
    
    let mut expected = vec!["name".to_string(), "age".to_string(), "city".to_string()];
    expected.sort();
    
    assert_eq!(keys, expected);
    
    // Verify HKEYS only returns keys, not values
    assert!(!keys.contains(&"Alice".to_string()));
    assert!(!keys.contains(&"25".to_string()));
    assert!(!keys.contains(&"NYC".to_string()));
    
    // Test HKEYS vs HGETALL difference
    let getall_result = db.hget_all("user:1").unwrap();
    assert_eq!(getall_result.len(), 6); // Should have 6 items (3 keys + 3 values)
    assert_eq!(keys_result.len(), 3);   // Should have 3 items (only keys)
}

#[test]
fn test_hkeys_type_error() {
    let mut db = Database::new();
    
    // Set a string value
    db.set("mystring", "value".to_string());
    
    // Try HKEYS on string - should return type error
    let result = db.hkeys("mystring");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), CommandError::WrongType);
}
