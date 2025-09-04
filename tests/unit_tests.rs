// Unit tests for Rudis data structures
// Tests individual components in isolation

use rudis::data_structures::{RedisString, RedisHash, RedisList, RedisSet, RedisSortedSet};
use rudis::database::Database;

#[test]
fn test_redis_string_operations() {
    // Test basic operations
    let rs = RedisString::new("hello".to_string());
    assert_eq!(rs.get(), "hello");

    // Test mutable operations
    let mut rs_mut = RedisString::new("world".to_string());
    rs_mut.set("updated".to_string());
    assert_eq!(rs_mut.get(), "updated");
}

#[test]
fn test_redis_list_operations() {
    let mut list = RedisList::new();
    assert_eq!(list.len(), 0);

    // Test push and pop
    list.push("item1".to_string());
    assert_eq!(list.len(), 1);

    list.push("item2".to_string());
    assert_eq!(list.len(), 2);

    // Test pop (LIFO)
    assert_eq!(list.pop(), Some("item2".to_string()));
    assert_eq!(list.len(), 1);

    assert_eq!(list.pop(), Some("item1".to_string()));
    assert_eq!(list.len(), 0);

    // Test pop on empty list
    assert_eq!(list.pop(), None);
}

#[test]
fn test_redis_hash_operations() {
    let mut hash = RedisHash::new();

    // Test hset on new field
    let result = hash.hset("name".to_string(), "Alice".to_string());
    assert_eq!(result, 1); // New field
    assert_eq!(hash.hget("name"), Some(&"Alice".to_string()));

    // Test hset on existing field
    let result = hash.hset("name".to_string(), "Bob".to_string());
    assert_eq!(result, 0); // Updated existing field
    assert_eq!(hash.hget("name"), Some(&"Bob".to_string()));

    // Test hget on non-existent field
    assert_eq!(hash.hget("age"), None);

    // Test hdel
    assert!(hash.hdel("name"));
    assert_eq!(hash.hget("name"), None);

    // Test hdel on non-existent field
    assert!(!hash.hdel("nonexistent"));

    // Test flatten
    hash.hset("key1".to_string(), "value1".to_string());
    hash.hset("key2".to_string(), "value2".to_string());
    let flattened: Vec<&String> = hash.flatten().collect();
    assert_eq!(flattened.len(), 4); // 2 keys + 2 values
}

#[test]
fn test_redis_set_operations() {
    let mut set = RedisSet::new();

    // Test sadd on new member
    assert!(set.sadd("member1".to_string()));
    assert!(set.sismember("member1"));

    // Test sadd on existing member
    assert!(!set.sadd("member1".to_string())); // Should return false

    // Test srem
    assert!(set.srem("member1"));
    assert!(!set.sismember("member1"));

    // Test srem on non-existent member
    assert!(!set.srem("nonexistent"));

    // Test multiple members
    set.sadd("a".to_string());
    set.sadd("b".to_string());
    set.sadd("c".to_string());

    assert!(set.sismember("a"));
    assert!(set.sismember("b"));
    assert!(set.sismember("c"));
    assert!(!set.sismember("d"));
}

#[test]
fn test_redis_sorted_set_operations() {
    let mut zset = RedisSortedSet::new();

    // Test zadd
    zset.zadd("alice".to_string(), 10.5);
    zset.zadd("bob".to_string(), 5.2);
    zset.zadd("charlie".to_string(), 15.8);

    // Test zscore
    assert_eq!(zset.zscore("alice"), Some(&10.5));
    assert_eq!(zset.zscore("bob"), Some(&5.2));
    assert_eq!(zset.zscore("charlie"), Some(&15.8));
    assert_eq!(zset.zscore("nonexistent"), None);

    // Test zrem
    assert!(zset.zrem("bob"));
    assert_eq!(zset.zscore("bob"), None);

    // Test zrem on non-existent member
    assert!(!zset.zrem("nonexistent"));

    // Test updating score
    zset.zadd("alice".to_string(), 20.0);
    assert_eq!(zset.zscore("alice"), Some(&20.0));
}

#[test]
fn test_database_operations() {
    let mut db = Database::new();

    // Test string operations
    db.set("key1".to_string(), "value1".to_string());
    assert_eq!(db.get("key1"), Some("value1"));
    assert_eq!(db.get("nonexistent"), None);

    // Test del
    assert!(db.del("key1"));
    assert_eq!(db.get("key1"), None);
    assert!(!db.del("nonexistent"));

    // Test numeric operations
    assert_eq!(db.incr("counter"), Ok(1));
    assert_eq!(db.incr("counter"), Ok(2));
    assert_eq!(db.decr("counter"), Ok(1));

    assert_eq!(db.incr_by("counter", "5"), Ok(6));
    assert_eq!(db.decr_by("counter", "2"), Ok(4));

    // Test numeric operations on non-numeric string
    db.set("text".to_string(), "not_a_number".to_string());
    assert!(db.incr("text").is_err());
    assert!(db.incr_by("text", "5").is_err());

    // Test append and str_len
    assert_eq!(db.append("append_key", "hello"), 5);
    assert_eq!(db.str_len("append_key"), 5);

    assert_eq!(db.append("append_key", " world"), 11);
    assert_eq!(db.str_len("append_key"), 11);
    assert_eq!(db.get("append_key"), Some("hello world"));

    assert_eq!(db.str_len("nonexistent"), 0);
}

#[test]
fn test_database_hash_operations() {
    let mut db = Database::new();

    // Test hset on new hash
    assert_eq!(db.hset("user", "name", "Alice"), Ok(1));
    assert_eq!(db.hset("user", "age", "25"), Ok(1));

    // Test hset on existing field
    assert_eq!(db.hset("user", "name", "Bob"), Ok(0));

    // Test hget
    assert_eq!(db.hget("user", "name"), Ok(Some(&"Bob".to_string())));
    assert_eq!(db.hget("user", "age"), Ok(Some(&"25".to_string())));
    assert_eq!(db.hget("user", "nonexistent"), Ok(None));
    assert_eq!(db.hget("nonexistent_hash", "field"), Ok(None));

    // Test hdel
    assert!(db.hdel("user", "age"));
    assert_eq!(db.hget("user", "age"), Ok(None));
    assert!(!db.hdel("user", "nonexistent"));

    // Test hget_all
    let all_fields = db.hget_all("user").unwrap();
    assert_eq!(all_fields.len(), 2); // key and value for "name"
    assert!(all_fields.contains(&"name".to_string()));
    assert!(all_fields.contains(&"Bob".to_string()));

    let empty_hash = db.hget_all("nonexistent").unwrap();
    assert_eq!(empty_hash.len(), 0);
}

#[test]
fn test_database_type_conflicts() {
    let mut db = Database::new();

    // Set a string value
    db.set("mykey".to_string(), "string_value".to_string());

    // Try hash operations on string key - should return WRONGTYPE error
    assert!(db.hset("mykey", "field", "value").is_err());
    assert!(db.hget("mykey", "field").is_err());
    assert!(db.hget_all("mykey").is_err());

    // Verify the error message
    if let Err(msg) = db.hset("mykey", "field", "value") {
        assert!(msg.contains("WRONGTYPE"));
    }

    if let Err(msg) = db.hget("mykey", "field") {
        assert!(msg.contains("WRONGTYPE"));
    }

    if let Err(msg) = db.hget_all("mykey") {
        assert!(msg.contains("WRONGTYPE"));
    }
}

#[test]
fn test_database_edge_cases() {
    let mut db = Database::new();

    // Test operations on empty keys
    db.set("".to_string(), "empty_key".to_string());
    assert_eq!(db.get(""), Some("empty_key"));

    // Test large numbers
    assert_eq!(db.incr_by("big_num", "999999999999"), Ok(999999999999));

    // Test negative numbers
    assert_eq!(db.incr_by("neg_num", "-100"), Ok(-100));
    assert_eq!(db.decr_by("neg_num", "50"), Ok(-150));

    // Test zero
    assert_eq!(db.incr_by("zero", "0"), Ok(0));

    // Test hash with empty field names
    assert_eq!(db.hset("hash", "", "empty_field"), Ok(1));
    assert_eq!(db.hget("hash", ""), Ok(Some(&"empty_field".to_string())));
}
