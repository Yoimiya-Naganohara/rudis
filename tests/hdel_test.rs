// Test for HDEL functionality fix

use bytes::Bytes;
use rudis::database::{Database, HashOp, StringOp};

#[test]
fn test_hdel_multiple_fields() {
    let db = Database::new(16);

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
    assert_eq!(
        db.hset(
            &Bytes::from("user:1"),
            Bytes::from("country"),
            Bytes::from("USA")
        ),
        Ok(1)
    );

    // Delete multiple fields at once
    let deleted_count = db.hdel_multiple(
        &Bytes::from("user:1"),
        &[
            Bytes::from("age"),
            Bytes::from("city"),
            Bytes::from("nonexistent"),
        ],
    );
    assert_eq!(deleted_count, 2); // Should delete 2 fields (age and city), nonexistent doesn't count

    // Verify the correct fields were deleted
    assert_eq!(
        db.hget(&Bytes::from("user:1"), &Bytes::from("name")),
        Ok(Some(Bytes::from("Alice")))
    );
    assert_eq!(
        db.hget(&Bytes::from("user:1"), &Bytes::from("country")),
        Ok(Some(Bytes::from("USA")))
    );
    assert_eq!(
        db.hget(&Bytes::from("user:1"), &Bytes::from("age")),
        Ok(None)
    ); // Should be deleted
    assert_eq!(
        db.hget(&Bytes::from("user:1"), &Bytes::from("city")),
        Ok(None)
    ); // Should be deleted
}

#[test]
fn test_hdel_nonexistent_hash() {
    let db = Database::new(16);

    // Try to delete from non-existent hash
    let deleted_count = db.hdel_multiple(
        &Bytes::from("nonexistent"),
        &[Bytes::from("field1"), Bytes::from("field2")],
    );
    assert_eq!(deleted_count, 0);
}

#[test]
fn test_hdel_wrong_type() {
    let db = Database::new(16);

    // Set a string value
    db.set(&Bytes::from("mystring"), Bytes::from("value"));

    // Try HDEL on string - should return 0 (no fields deleted)
    let deleted_count = db.hdel_multiple(&Bytes::from("mystring"), &[Bytes::from("field1")]);
    assert_eq!(deleted_count, 0);
}
