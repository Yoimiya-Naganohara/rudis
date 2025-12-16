// Unit tests for Rudis data structures
// Tests individual components in isolation

use bytes::Bytes;
use rudis::commands::CommandError;
use rudis::data_structures::{RedisHash, RedisList, RedisSet, RedisSortedSet, RedisString};
use rudis::database::{Database, traits::{HashOp, SetOp, StringOp}};

#[test]
fn test_redis_string_operations() {
    // Test basic operations
    let rs = RedisString::new(Bytes::from("hello"));
    // assert_eq!(rs.get(), "hello");

    // Test mutable operations
    let mut rs_mut = RedisString::new(Bytes::from("world"));
    rs_mut.set(Bytes::from("updated"));
    // assert_eq!(rs_mut.get(), "updated");
}

#[test]
fn test_redis_list_operations() {
    let mut list = RedisList::new();
    assert_eq!(list.len(), 0);

    // Test push and pop
    list.push(Bytes::from("item1"));
    assert_eq!(list.len(), 1);

    list.push(Bytes::from("item2"));
    assert_eq!(list.len(), 2);

    // Test pop (LIFO)
    assert_eq!(list.pop(), Some(Bytes::from("item2")));
    assert_eq!(list.len(), 1);

    assert_eq!(list.pop(), Some(Bytes::from("item1")));
    assert_eq!(list.len(), 0);

    // Test pop on empty list
    assert_eq!(list.pop(), None);
}

#[test]
fn test_redis_hash_operations() {
    let mut hash = RedisHash::new();

    // Test hset on new field
    let result = hash.hset(Bytes::from("name"), Bytes::from("Alice"));
    assert_eq!(result, 1); // New field
    assert_eq!(hash.hget(&Bytes::from("name")), Some(&Bytes::from("Alice")));

    // Test hset on existing field
    let result = hash.hset(Bytes::from("name"), Bytes::from("Bob"));
    assert_eq!(result, 0); // Updated existing field
    assert_eq!(hash.hget(&Bytes::from("name")), Some(&Bytes::from("Bob")));

    // Test hget on non-existent field
    assert_eq!(hash.hget(&Bytes::from("age")), None);

    // Test hdel
    assert!(hash.hdel(&Bytes::from("name")));
    assert_eq!(hash.hget(&Bytes::from("name")), None);

    // Test hdel on non-existent field
    assert!(!hash.hdel(&Bytes::from("nonexistent")));

    // Test flatten
    hash.hset(Bytes::from("key1"), Bytes::from("value1"));
    hash.hset(Bytes::from("key2"), Bytes::from("value2"));
    let flattened: Vec<&Bytes> = hash.flatten().collect();
    assert_eq!(flattened.len(), 4); // 2 keys + 2 values
}

#[test]
fn test_redis_set_operations() {
    let mut set = RedisSet::new();

    // Test sadd on new member
    assert!(set.sadd(Bytes::from("member1")));
    assert!(set.sismember(&Bytes::from("member1")));

    // Test sadd on existing member
    assert!(!set.sadd(Bytes::from("member1"))); // Should return false

    // Test srem
    assert!(set.srem(&Bytes::from("member1")));
    assert!(!set.sismember(&Bytes::from("member1")));

    // Test srem on non-existent member
    assert!(!set.srem(&Bytes::from("nonexistent")));

    // Test multiple members
    set.sadd(Bytes::from("a"));
    set.sadd(Bytes::from("b"));
    set.sadd(Bytes::from("c"));

    assert!(set.sismember(&Bytes::from("a")));
    assert!(set.sismember(&Bytes::from("b")));
    assert!(set.sismember(&Bytes::from("c")));
    assert!(!set.sismember(&Bytes::from("d")));
}

#[test]
fn test_database_operations() {
    let mut db = Database::new(16);

    // Test string operations
    db.set(&Bytes::from("key1"), Bytes::from("value1"));
    assert_eq!(db.get(&Bytes::from("key1")), Some(Bytes::from("value1")));
    assert_eq!(db.get(&Bytes::from("nonexistent")), None);

    // Test del
    assert_eq!(db.del(&vec![Bytes::from("key1")]), 1);
    assert_eq!(db.get(&Bytes::from("key1")), None);
    assert_eq!(db.del(&vec![Bytes::from("nonexistent")]), 0);

    // Test numeric operations
    assert_eq!(db.incr(&Bytes::from("counter")), Ok(1));
    assert_eq!(db.incr(&Bytes::from("counter")), Ok(2));
    assert_eq!(db.decr(&Bytes::from("counter")), Ok(1));

    assert_eq!(db.incr_by(&Bytes::from("counter"), Bytes::from("5")), Ok(6));
    assert_eq!(db.decr_by(&Bytes::from("counter"), Bytes::from("2")), Ok(4));

    // Test numeric operations on non-numeric string
    db.set(&Bytes::from("text"), Bytes::from("not_a_number"));
    assert!(db.incr(&Bytes::from("text")).is_err());
    assert!(db.incr_by(&Bytes::from("text"), Bytes::from("5")).is_err());

    // Test append and str_len
    assert_eq!(
        db.append(&Bytes::from("append_key"), Bytes::from("hello")),
        5
    );
    assert_eq!(db.str_len(&Bytes::from("append_key")), 5);

    assert_eq!(
        db.append(&Bytes::from("append_key"), Bytes::from(" world")),
        11
    );
    assert_eq!(db.str_len(&Bytes::from("append_key")), 11);
    assert_eq!(
        db.get(&Bytes::from("append_key")),
        Some(Bytes::from("hello world"))
    );

    assert_eq!(db.str_len(&Bytes::from("nonexistent")), 0);
}

#[test]
fn test_database_hash_operations() {
    let mut db = Database::new(16);

    // Test hset on new hash
    assert_eq!(
        db.hset(
            &Bytes::from("user"),
            Bytes::from("name"),
            Bytes::from("Alice")
        ),
        Ok(1)
    );
    assert_eq!(
        db.hset(&Bytes::from("user"), Bytes::from("age"), Bytes::from("25")),
        Ok(1)
    );

    // Test hset on existing field
    assert_eq!(
        db.hset(
            &Bytes::from("user"),
            Bytes::from("name"),
            Bytes::from("Bob")
        ),
        Ok(0)
    );

    // Test hget
    assert_eq!(
        db.hget(&Bytes::from("user"), &Bytes::from("name")),
        Ok(Some(Bytes::from("Bob")))
    );
    assert_eq!(
        db.hget(&Bytes::from("user"), &Bytes::from("age")),
        Ok(Some(Bytes::from("25")))
    );
    assert_eq!(
        db.hget(&Bytes::from("user"), &Bytes::from("nonexistent")),
        Ok(None)
    );
    assert_eq!(
        db.hget(&Bytes::from("nonexistent_hash"), &Bytes::from("field")),
        Ok(None)
    );

    // Test hdel
    assert!(db.hdel(&Bytes::from("user"), &Bytes::from("age")));
    assert_eq!(db.hget(&Bytes::from("user"), &Bytes::from("age")), Ok(None));
    assert!(!db.hdel(&Bytes::from("user"), &Bytes::from("nonexistent")));

    // Test hget_all
    let all_fields = db.hget_all(&Bytes::from("user")).unwrap();
    assert_eq!(all_fields.len(), 2); // key and value for "name"
    assert!(all_fields.contains(&Bytes::from("name")));
    assert!(all_fields.contains(&Bytes::from("Bob")));

    let empty_hash = db.hget_all(&Bytes::from("nonexistent")).unwrap();
    assert_eq!(empty_hash.len(), 0);
}

#[test]
fn test_database_type_conflicts() {
    let mut db = Database::new(16);

    // Set a string value
    db.set(&Bytes::from("mykey"), Bytes::from("string_value"));

    // Try hash operations on string key - should return WRONGTYPE error
    assert!(db
        .hset(
            &Bytes::from("mykey"),
            Bytes::from("field"),
            Bytes::from("value")
        )
        .is_err());
    assert!(db
        .hget(&Bytes::from("mykey"), &Bytes::from("field"))
        .is_err());
    assert!(db.hget_all(&Bytes::from("mykey")).is_err());

    // Verify the error message
    if let Err(msg) = db.hset(
        &Bytes::from("mykey"),
        Bytes::from("field"),
        Bytes::from("value"),
    ) {
        assert_eq!(msg, CommandError::WrongType);
    }

    if let Err(msg) = db.hget(&Bytes::from("mykey"), &Bytes::from("field")) {
        assert_eq!(msg, CommandError::WrongType);
    }

    if let Err(msg) = db.hget_all(&Bytes::from("mykey")) {
        assert_eq!(msg, CommandError::WrongType);
    }
}

#[test]
fn test_database_edge_cases() {
    let mut db = Database::new(16);

    // Test operations on empty keys
    db.set(&Bytes::from(""), Bytes::from("empty_key"));
    assert_eq!(db.get(&Bytes::from("")), Some(Bytes::from("empty_key")));

    // Test large numbers
    assert_eq!(
        db.incr_by(&Bytes::from("big_num"), Bytes::from("999999999999")),
        Ok(999999999999)
    );

    // Test negative numbers
    assert_eq!(
        db.incr_by(&Bytes::from("neg_num"), Bytes::from("-100")),
        Ok(-100)
    );
    assert_eq!(
        db.decr_by(&Bytes::from("neg_num"), Bytes::from("50")),
        Ok(-150)
    );

    // Test zero
    assert_eq!(db.incr_by(&Bytes::from("zero"), Bytes::from("0")), Ok(0));

    // Test hash with empty field names
    assert_eq!(
        db.hset(
            &Bytes::from("hash"),
            Bytes::from(""),
            Bytes::from("empty_field")
        ),
        Ok(1)
    );
    assert_eq!(
        db.hget(&Bytes::from("hash"), &Bytes::from("")),
        Ok(Some(Bytes::from("empty_field")))
    );
}

#[test]
fn test_database_set_operations() {
    let mut db = Database::new(16);

    // Test sadd on new set
    assert_eq!(db.sadd(&Bytes::from("myset"), &[Bytes::from("member1")]), 1);
    assert_eq!(
        db.sadd(
            &Bytes::from("myset"),
            &[Bytes::from("member2"), Bytes::from("member3")]
        ),
        2
    );

    // Test sadd on existing members
    assert_eq!(db.sadd(&Bytes::from("myset"), &[Bytes::from("member1")]), 0);

    // Test smembers
    let members = db.smembers(&Bytes::from("myset")).unwrap();
    assert_eq!(members.len(), 3);
    assert!(members.contains(&Bytes::from("member1")));
    assert!(members.contains(&Bytes::from("member2")));
    assert!(members.contains(&Bytes::from("member3")));

    // Test scard
    assert_eq!(db.scard(&Bytes::from("myset")), 3);

    // Test sismember
    assert!(db.sismember(&Bytes::from("myset"), &Bytes::from("member1")));
    assert!(!db.sismember(&Bytes::from("myset"), &Bytes::from("nonexistent")));

    // Test srem
    assert_eq!(db.srem(&Bytes::from("myset"), &[Bytes::from("member2")]), 1);
    assert_eq!(db.scard(&Bytes::from("myset")), 2);
    assert!(!db.sismember(&Bytes::from("myset"), &Bytes::from("member2")));

    // Test srem on non-existent members
    assert_eq!(
        db.srem(&Bytes::from("myset"), &[Bytes::from("nonexistent")]),
        0
    );

    // Test operations on non-existent set
    assert_eq!(db.scard(&Bytes::from("nonexistent")), 0);
    assert!(!db.sismember(&Bytes::from("nonexistent"), &Bytes::from("anything")));
    assert_eq!(
        db.srem(&Bytes::from("nonexistent"), &[Bytes::from("anything")]),
        0
    );

    // Test type conflicts
    db.set(&Bytes::from("notaset"), Bytes::from("string"));
    assert!(db.smembers(&Bytes::from("notaset")).is_err());
    assert_eq!(
        db.smembers(&Bytes::from("notaset")).unwrap_err(),
        CommandError::WrongType
    );
}
