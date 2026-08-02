// Tests for Command::parse dispatch (case-insensitivity, arity, unknown commands)

use bytes::Bytes;
use rudis::commands::Command;
use rudis::database::Database;
use rudis::networking::resp::RespValue;

fn cmd(name: &str, args: &[&str]) -> RespValue {
    RespValue::Array(
        std::iter::once(name)
            .chain(args.iter().copied())
            .map(|s| RespValue::BulkString(Bytes::from(s.to_string())))
            .collect(),
    )
}

#[test]
fn test_parse_is_case_insensitive() {
    // Lowercase, uppercase, and mixed-case must all dispatch to Get
    for name in ["GET", "get", "GeT", "gEt"] {
        let parsed = Command::parse(&cmd(name, &["mykey"]));
        assert!(matches!(parsed, Some(Command::Get(_))), "failed for {name}");
    }
    let parsed = Command::parse(&cmd("hGetAlL", &["myhash"]));
    assert!(matches!(parsed, Some(Command::HGetAll(_))));
}

#[test]
fn test_parse_commands_with_camelcase_bug() {
    // These names were previously unreachable: the parser uppercased the
    // input but the match arms were written in camel case
    assert!(matches!(
        Command::parse(&cmd("LLEN", &["mylist"])),
        Some(Command::LLen(_))
    ));
    assert!(matches!(
        Command::parse(&cmd("SCARD", &["myset"])),
        Some(Command::SCard(_))
    ));
    assert!(matches!(
        Command::parse(&cmd("SDIFF", &["s1", "s2"])),
        Some(Command::SDiff(_))
    ));
}

#[test]
fn test_parse_unknown_command_returns_none() {
    assert!(Command::parse(&cmd("NOSUCHCMD", &["k"])).is_none());
    // Valid name with too many/few arguments
    assert!(Command::parse(&cmd("GET", &[])).is_none());
    assert!(Command::parse(&cmd("GET", &["k", "extra"])).is_none());
    // Command name longer than the stack buffer
    assert!(Command::parse(&cmd("A".repeat(64).as_str(), &["k"])).is_none());
}

#[test]
fn test_parse_non_array_frames_return_none() {
    assert!(Command::parse(&RespValue::BulkString(Bytes::from("GET"))).is_none());
    assert!(Command::parse(&RespValue::Array(vec![])).is_none());
    // First element not a bulk string
    assert!(Command::parse(&RespValue::Array(vec![RespValue::Integer(1)])).is_none());
}

#[test]
fn test_parse_variants() {
    assert!(matches!(
        Command::parse(&cmd("PING", &[])),
        Some(Command::Ping(None))
    ));
    assert!(matches!(
        Command::parse(&cmd("PING", &["hello"])),
        Some(Command::Ping(Some(_)))
    ));
    assert!(matches!(
        Command::parse(&cmd("INFO", &[])),
        Some(Command::Info(None))
    ));
    assert!(matches!(
        Command::parse(&cmd("INFO", &["server"])),
        Some(Command::Info(Some(_)))
    ));
    assert!(matches!(
        Command::parse(&cmd("QUIT", &[])),
        Some(Command::Quit)
    ));
    assert!(matches!(
        Command::parse(&cmd("FLUSHALL", &[])),
        Some(Command::FlushAll)
    ));
    assert!(matches!(
        Command::parse(&cmd("SET", &["k", "v"])),
        Some(Command::Set(_, _, None))
    ));
    assert!(matches!(
        Command::parse(&cmd("SET", &["k", "v", "NX"])),
        Some(Command::Set(_, _, Some(opts))) if opts.nx
    ));
    assert!(matches!(
        Command::parse(&cmd("MGET", &["a", "b", "c"])),
        Some(Command::MGet(keys)) if keys.len() == 3
    ));
}

#[test]
fn test_hset_accepts_multiple_field_value_pairs() {
    // Single pair
    assert!(matches!(
        Command::parse(&cmd("HSET", &["h", "f", "v"])),
        Some(Command::HSet(key, pairs)) if key == "h" && pairs.len() == 1
    ));
    // Multiple pairs
    assert!(matches!(
        Command::parse(&cmd("HSET", &["h", "f1", "v1", "f2", "v2", "f3", "v3"])),
        Some(Command::HSet(key, pairs)) if key == "h" && pairs.len() == 3
    ));
    // Odd trailing element must be rejected
    assert!(Command::parse(&cmd("HSET", &["h", "f1", "v1", "f2"])).is_none());
}

#[test]
fn test_zadd_parses() {
    // ZADD used to be unparseable due to the same parity bug as HSET
    assert!(matches!(
        Command::parse(&cmd("ZADD", &["z", "1.5", "m1"])),
        Some(Command::ZAdd(key, pairs)) if key == "z" && pairs.len() == 1
    ));
    assert!(matches!(
        Command::parse(&cmd("ZADD", &["z", "1", "m1", "2", "m2"])),
        Some(Command::ZAdd(_, pairs)) if pairs.len() == 2
    ));
    assert!(Command::parse(&cmd("ZADD", &["z", "1"])).is_none());
}

#[test]
fn test_hset_multiple_pairs_executes() {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let parsed = Command::parse(&cmd("HSET", &["h", "f1", "v1", "f2", "v2"])).unwrap();
    // First call: both fields are new
    assert_eq!(rt.block_on(parsed.execute(&db)), Bytes::from(":2\r\n"));
    // Second call: both fields already exist, nothing added
    let parsed = Command::parse(&cmd("HSET", &["h", "f1", "v1", "f2", "v2"])).unwrap();
    assert_eq!(rt.block_on(parsed.execute(&db)), Bytes::from(":0\r\n"));
    // Update one field: only the new one counts
    let parsed = Command::parse(&cmd("HSET", &["h", "f1", "v1b", "f3", "v3"])).unwrap();
    assert_eq!(rt.block_on(parsed.execute(&db)), Bytes::from(":1\r\n"));
}
