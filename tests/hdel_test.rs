// Test for HDEL functionality fix

use rudis::database::{Database, HashOp, StringOp};

#[test]
fn test_hdel_multiple_fields() {
    let mut db = Database::new(16);

    // Set up a hash with multiple fields
    assert_eq!(db.hset("user:1", "name", "Alice"), Ok(1));
    assert_eq!(db.hset("user:1", "age", "25"), Ok(1));
    assert_eq!(db.hset("user:1", "city", "NYC"), Ok(1));
    assert_eq!(db.hset("user:1", "country", "USA"), Ok(1));

    // Delete multiple fields at once
    let deleted_count = db.hdel_multiple("user:1", &["age".to_string(), "city".to_string(), "nonexistent".to_string()]);
    assert_eq!(deleted_count, 2); // Should delete 2 fields (age and city), nonexistent doesn't count

    // Verify the correct fields were deleted
    assert_eq!(db.hget("user:1", "name"), Ok(Some(&"Alice".to_string())));
    assert_eq!(db.hget("user:1", "country"), Ok(Some(&"USA".to_string())));
    assert_eq!(db.hget("user:1", "age"), Ok(None)); // Should be deleted
    assert_eq!(db.hget("user:1", "city"), Ok(None)); // Should be deleted
}

#[test]
fn test_hdel_nonexistent_hash() {
    let mut db = Database::new(16);

    // Try to delete from non-existent hash
    let deleted_count = db.hdel_multiple("nonexistent", &["field1".to_string(), "field2".to_string()]);
    assert_eq!(deleted_count, 0);
}

#[test]
fn test_hdel_wrong_type() {
    let mut db = Database::new(16);

    // Set a string value
    db.set("mystring", "value".to_string());

    // Try HDEL on string - should return 0 (no fields deleted)
    let deleted_count = db.hdel_multiple("mystring", &["field1".to_string()]);
    assert_eq!(deleted_count, 0);
}
