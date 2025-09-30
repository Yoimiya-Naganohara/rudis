// Integration tests for Rudis
// Tests the full server functionality with command parsing and execution

use rudis::commands::Command;
use rudis::database::Database;
use rudis::networking::resp::RespValue;

#[test]
fn test_command_parsing_and_execution_integration() {
    // Test that commands can be parsed and executed on a database
    let db = Database::new_shared(16);

    // Test SET command
    let set_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("SET".to_string())),
        RespValue::BulkString(Some("integration_key".to_string())),
        RespValue::BulkString(Some("integration_value".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&set_cmd) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "+OK\r\n");
    } else {
        panic!("Failed to parse SET command");
    }

    // Test GET command
    let get_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("GET".to_string())),
        RespValue::BulkString(Some("integration_key".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&get_cmd) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "$17\r\nintegration_value\r\n");
    } else {
        panic!("Failed to parse GET command");
    }
}

#[test]
fn test_hash_operations_integration() {
    // Test hash operations end-to-end
    let db = Database::new_shared(16);

    // Test HSET
    let hset_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HSET".to_string())),
        RespValue::BulkString(Some("user".to_string())),
        RespValue::BulkString(Some("name".to_string())),
        RespValue::BulkString(Some("Alice".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hset_cmd) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":1\r\n");
    }

    // Test HGET
    let hget_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HGET".to_string())),
        RespValue::BulkString(Some("user".to_string())),
        RespValue::BulkString(Some("name".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hget_cmd) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "$5\r\nAlice\r\n");
    }

    // Test HGETALL
    let hgetall_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HGETALL".to_string())),
        RespValue::BulkString(Some("user".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hgetall_cmd) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cmd.execute(&db));
        assert!(result.contains("$4\r\nname\r\n"));
        assert!(result.contains("$5\r\nAlice\r\n"));
    }
}

#[test]
fn test_multiple_operations_integration() {
    // Test a sequence of operations
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // SET multiple keys
    let commands = vec![
        ("SET key1 value1", "+OK\r\n"),
        ("SET key2 value2", "+OK\r\n"),
        ("GET key1", "$6\r\nvalue1\r\n"),
        ("GET key2", "$6\r\nvalue2\r\n"),
        (
            "MGET key1 key2 nonexistent",
            "*3\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$-1\r\n",
        ),
    ];

    for (cmd_str, expected) in commands {
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        let resp_parts: Vec<RespValue> = parts
            .into_iter()
            .map(|p| RespValue::BulkString(Some(p.to_string())))
            .collect();
        let resp_value = RespValue::Array(resp_parts);

        if let Some(cmd) = Command::parse(&resp_value) {
            let result = rt.block_on(cmd.execute(&db));
            assert_eq!(result, expected, "Command '{}' failed", cmd_str);
        } else {
            panic!("Failed to parse command: {}", cmd_str);
        }
    }
}

#[test]
fn test_error_handling_integration() {
    // Test error conditions
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Test INCR on non-integer value
    let set_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("SET".to_string())),
        RespValue::BulkString(Some("text".to_string())),
        RespValue::BulkString(Some("not_a_number".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&set_cmd) {
        rt.block_on(cmd.execute(&db));
    }

    let incr_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("INCR".to_string())),
        RespValue::BulkString(Some("text".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&incr_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert!(result.contains("-ERR"));
    }
}

#[test]
fn test_numeric_operations_integration() {
    // Test INCR, DECR, INCRBY, DECRBY operations
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Test INCR on non-existent key
    let incr_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("INCR".to_string())),
        RespValue::BulkString(Some("counter".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&incr_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":1\r\n");
    }

    // Test INCR on existing value
    if let Some(cmd) = Command::parse(&incr_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":2\r\n");
    }

    // Test DECR
    let decr_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("DECR".to_string())),
        RespValue::BulkString(Some("counter".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&decr_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":1\r\n");
    }

    // Test INCRBY
    let incrby_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("INCRBY".to_string())),
        RespValue::BulkString(Some("counter".to_string())),
        RespValue::BulkString(Some("5".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&incrby_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":6\r\n");
    }

    // Test DECRBY
    let decrby_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("DECRBY".to_string())),
        RespValue::BulkString(Some("counter".to_string())),
        RespValue::BulkString(Some("3".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&decrby_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":3\r\n");
    }
}

#[test]
fn test_string_operations_integration() {
    // Test APPEND and STRLEN operations
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Test APPEND on non-existent key
    let append_cmd1 = RespValue::Array(vec![
        RespValue::BulkString(Some("APPEND".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
        RespValue::BulkString(Some("Hello".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&append_cmd1) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":5\r\n");
    }

    // Test APPEND on existing key
    let append_cmd2 = RespValue::Array(vec![
        RespValue::BulkString(Some("APPEND".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
        RespValue::BulkString(Some(" World".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&append_cmd2) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":11\r\n");
    }

    // Test STRLEN
    let strlen_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("STRLEN".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&strlen_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":11\r\n");
    }

    // Test STRLEN on non-existent key
    let strlen_cmd2 = RespValue::Array(vec![
        RespValue::BulkString(Some("STRLEN".to_string())),
        RespValue::BulkString(Some("nonexistent".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&strlen_cmd2) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":0\r\n");
    }
}

#[test]
fn test_del_operations_integration() {
    // Test DEL command with multiple keys
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Set up some keys
    let set_cmds = vec![
        ("SET key1 value1", "+OK\r\n"),
        ("SET key2 value2", "+OK\r\n"),
        ("SET key3 value3", "+OK\r\n"),
    ];

    for (cmd_str, expected) in set_cmds {
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        let resp_parts: Vec<RespValue> = parts
            .into_iter()
            .map(|p| RespValue::BulkString(Some(p.to_string())))
            .collect();
        let resp_value = RespValue::Array(resp_parts);

        if let Some(cmd) = Command::parse(&resp_value) {
            let result = rt.block_on(cmd.execute(&db));
            assert_eq!(result, expected);
        }
    }

    // Test DEL with multiple keys
    let del_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("DEL".to_string())),
        RespValue::BulkString(Some("key1".to_string())),
        RespValue::BulkString(Some("key2".to_string())),
        RespValue::BulkString(Some("nonexistent".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&del_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":2\r\n"); // 2 keys deleted, 1 didn't exist
    }

    // Verify keys are gone
    let get_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("GET".to_string())),
        RespValue::BulkString(Some("key1".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&get_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "$-1\r\n"); // Key doesn't exist
    }
}

#[test]
fn test_ping_variations_integration() {
    // Test PING command variations
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Test PING without argument
    let ping_cmd1 = RespValue::Array(vec![RespValue::BulkString(Some("PING".to_string()))]);

    if let Some(cmd) = Command::parse(&ping_cmd1) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "+PONG\r\n");
    }

    // Test PING with argument
    let ping_cmd2 = RespValue::Array(vec![
        RespValue::BulkString(Some("PING".to_string())),
        RespValue::BulkString(Some("hello world".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&ping_cmd2) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "+hello world\r\n");
    }
}

#[test]
fn test_hash_comprehensive_integration() {
    // Comprehensive hash operations test
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Set up hash with multiple fields
    let hset_cmds = vec![
        ("HSET user name Alice", ":1\r\n"),
        ("HSET user age 25", ":1\r\n"),
        ("HSET user city NYC", ":1\r\n"),
    ];

    for (cmd_str, expected) in hset_cmds {
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        let resp_parts: Vec<RespValue> = parts
            .into_iter()
            .map(|p| RespValue::BulkString(Some(p.to_string())))
            .collect();
        let resp_value = RespValue::Array(resp_parts);

        if let Some(cmd) = Command::parse(&resp_value) {
            let result = rt.block_on(cmd.execute(&db));
            assert_eq!(result, expected);
        }
    }

    // Test HGETALL
    let hgetall_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HGETALL".to_string())),
        RespValue::BulkString(Some("user".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hgetall_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        // Should contain all key-value pairs
        assert!(result.contains("$4\r\nname\r\n"));
        assert!(result.contains("$5\r\nAlice\r\n"));
        assert!(result.contains("$3\r\nage\r\n"));
        assert!(result.contains("$2\r\n25\r\n"));
        assert!(result.contains("$4\r\ncity\r\n"));
        assert!(result.contains("$3\r\nNYC\r\n"));
    }

    // Test HDEL
    let hdel_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HDEL".to_string())),
        RespValue::BulkString(Some("user".to_string())),
        RespValue::BulkString(Some("age".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hdel_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, ":1\r\n"); // 1 field deleted
    }

    // Test HGET on deleted field
    let hget_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HGET".to_string())),
        RespValue::BulkString(Some("user".to_string())),
        RespValue::BulkString(Some("age".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hget_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert_eq!(result, "$-1\r\n"); // Field doesn't exist
    }
}

#[test]
fn test_type_conflicts_integration() {
    // Test WRONGTYPE errors when operations are performed on wrong data types
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Set a string value
    let set_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("SET".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
        RespValue::BulkString(Some("string_value".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&set_cmd) {
        rt.block_on(cmd.execute(&db));
    }

    // Try to perform hash operations on string key
    let hget_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HGET".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
        RespValue::BulkString(Some("field".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hget_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert!(result.contains("WRONGTYPE"));
    }

    let hset_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HSET".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
        RespValue::BulkString(Some("field".to_string())),
        RespValue::BulkString(Some("value".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hset_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert!(result.contains("WRONGTYPE"));
    }

    let hgetall_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some("HGETALL".to_string())),
        RespValue::BulkString(Some("mykey".to_string())),
    ]);

    if let Some(cmd) = Command::parse(&hgetall_cmd) {
        let result = rt.block_on(cmd.execute(&db));
        assert!(result.contains("WRONGTYPE"));
    }
}

#[test]
fn test_invalid_commands_integration() {
    // Test parsing of invalid commands
    let invalid_cmds = vec![
        RespValue::Array(vec![]), // Empty array
        RespValue::Array(vec![RespValue::BulkString(Some("INVALID".to_string()))]), // Unknown command
        RespValue::Array(vec![RespValue::BulkString(Some("SET".to_string()))]), // Missing arguments
        RespValue::Array(vec![RespValue::BulkString(Some("GET".to_string()))]), // Missing key
        RespValue::BulkString(Some("NOT_AN_ARRAY".to_string())),                // Not an array
    ];

    for invalid_cmd in invalid_cmds {
        assert!(
            Command::parse(&invalid_cmd).is_none(),
            "Expected command to be invalid"
        );
    }
}

#[test]
fn test_complex_sequence_integration() {
    // Test a complex sequence of operations mixing different data types
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let commands = vec![
        // String operations
        ("SET str_key hello", "+OK\r\n"),
        ("APPEND str_key _world", ":11\r\n"),
        ("GET str_key", "$11\r\nhello_world\r\n"),
        ("STRLEN str_key", ":11\r\n"),
        // Numeric operations
        ("INCR counter", ":1\r\n"),
        ("INCRBY counter 5", ":6\r\n"),
        ("DECRBY counter 2", ":4\r\n"),
        // Hash operations
        ("HSET user name Alice", ":1\r\n"),
        ("HSET user age 30", ":1\r\n"),
        ("HGET user name", "$5\r\nAlice\r\n"),
        // Mixed operations
        (
            "MGET str_key counter nonexistent",
            "*3\r\n$11\r\nhello_world\r\n$1\r\n4\r\n$-1\r\n",
        ),
    ];

    for (cmd_str, expected) in commands {
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        let resp_parts: Vec<RespValue> = parts
            .into_iter()
            .map(|p| RespValue::BulkString(Some(p.to_string())))
            .collect();
        let resp_value = RespValue::Array(resp_parts);

        if let Some(cmd) = Command::parse(&resp_value) {
            let result = rt.block_on(cmd.execute(&db));
            assert_eq!(result, expected, "Command '{}' failed", cmd_str);
        } else {
            panic!("Failed to parse command: {}", cmd_str);
        }
    }
}

// Placeholder for future TCP server tests
// These would require a running server instance
/*
#[test]
fn test_tcp_server_integration() {
    // This test would start the server and connect via TCP
    // For now, it's commented out until server implementation is complete
}
*/
