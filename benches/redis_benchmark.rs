// Benchmarks for Rudis Redis Clone
// Tests performance of various Redis operations

use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rudis::commands::Command;
use rudis::database::Database;
use rudis::networking::resp::RespValue;

// Command construction and parsing happen outside `b.iter` so the measured
// loop covers only parse + execute, not the benchmark's own allocations.
// The response buffer is reused across iterations like the production
// networking loop does.

fn bench_string_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("string_set", |b| {
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SET")),
            RespValue::BulkString(Bytes::from_static(b"bench_key")),
            RespValue::BulkString(Bytes::from_static(b"bench_value")),
        ]);
        let cmd = Command::parse(&set_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("string_get", |b| {
        // Setup: ensure key exists
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SET")),
            RespValue::BulkString(Bytes::from_static(b"bench_key")),
            RespValue::BulkString(Bytes::from_static(b"bench_value")),
        ]);
        rt.block_on(
            Command::parse(&set_cmd)
                .unwrap()
                .execute(&db, &mut BytesMut::new()),
        );

        let get_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"GET")),
            RespValue::BulkString(Bytes::from_static(b"bench_key")),
        ]);
        let cmd = Command::parse(&get_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn bench_hash_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create a hash with multiple fields
    for i in 0..100 {
        let hset_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HSET")),
            RespValue::BulkString(Bytes::from_static(b"bench_hash")),
            RespValue::BulkString(Bytes::from(format!("field_{}", i))),
            RespValue::BulkString(Bytes::from(format!("value_{}", i))),
        ]);
        rt.block_on(
            Command::parse(&hset_cmd)
                .unwrap()
                .execute(&db, &mut BytesMut::new()),
        );
    }

    c.bench_function("hash_hset", |b| {
        let hset_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HSET")),
            RespValue::BulkString(Bytes::from_static(b"bench_hash")),
            RespValue::BulkString(Bytes::from_static(b"new_field")),
            RespValue::BulkString(Bytes::from_static(b"new_value")),
        ]);
        let cmd = Command::parse(&hset_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("hash_hget", |b| {
        let hget_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HGET")),
            RespValue::BulkString(Bytes::from_static(b"bench_hash")),
            RespValue::BulkString(Bytes::from_static(b"field_50")),
        ]);
        let cmd = Command::parse(&hget_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("hash_hgetall_small", |b| {
        // Test with small hash (10 fields)
        let small_db = Database::new_shared(16);
        let small_rt = tokio::runtime::Runtime::new().unwrap();

        for i in 0..10 {
            let hset_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"HSET")),
                RespValue::BulkString(Bytes::from_static(b"small_hash")),
                RespValue::BulkString(Bytes::from(format!("field_{}", i))),
                RespValue::BulkString(Bytes::from(format!("value_{}", i))),
            ]);
            small_rt.block_on(
                Command::parse(&hset_cmd)
                    .unwrap()
                    .execute(&small_db, &mut BytesMut::new()),
            );
        }

        let hgetall_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HGETALL")),
            RespValue::BulkString(Bytes::from_static(b"small_hash")),
        ]);
        let cmd = Command::parse(&hgetall_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            small_rt.block_on(cmd.clone().execute(&small_db, &mut out));
        })
    });

    c.bench_function("hash_hgetall_large", |b| {
        let hgetall_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HGETALL")),
            RespValue::BulkString(Bytes::from_static(b"bench_hash")),
        ]);
        let cmd = Command::parse(&hgetall_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn bench_list_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create a list with some items
    for i in 0..100 {
        let rpush_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"RPUSH")),
            RespValue::BulkString(Bytes::from_static(b"bench_list")),
            RespValue::BulkString(Bytes::from(format!("item_{}", i))),
        ]);
        rt.block_on(
            Command::parse(&rpush_cmd)
                .unwrap()
                .execute(&db, &mut BytesMut::new()),
        );
    }

    c.bench_function("list_rpush", |b| {
        let rpush_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"RPUSH")),
            RespValue::BulkString(Bytes::from_static(b"bench_list")),
            RespValue::BulkString(Bytes::from_static(b"new_item")),
        ]);
        let cmd = Command::parse(&rpush_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("list_lpop", |b| {
        // Pair a push with the pop so the list never drains (a plain LPOP
        // loop would measure the empty-list miss path after a few iterations).
        let rpush_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"RPUSH")),
            RespValue::BulkString(Bytes::from_static(b"bench_list")),
            RespValue::BulkString(Bytes::from_static(b"pop_pair_item")),
        ]);
        let lpop_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"LPOP")),
            RespValue::BulkString(Bytes::from_static(b"bench_list")),
        ]);
        let rpush = Command::parse(&rpush_cmd).unwrap();
        let lpop = Command::parse(&lpop_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(rpush.clone().execute(&db, &mut out));
            out.clear();
            rt.block_on(lpop.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("list_lrange", |b| {
        let lrange_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"LRANGE")),
            RespValue::BulkString(Bytes::from_static(b"bench_list")),
            RespValue::BulkString(Bytes::from_static(b"0")),
            RespValue::BulkString(Bytes::from_static(b"10")),
        ]);
        let cmd = Command::parse(&lrange_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("list_lindex", |b| {
        let lindex_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"LINDEX")),
            RespValue::BulkString(Bytes::from_static(b"bench_list")),
            RespValue::BulkString(Bytes::from_static(b"50")),
        ]);
        let cmd = Command::parse(&lindex_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn bench_numeric_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("numeric_incr", |b| {
        let incr_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"INCR")),
            RespValue::BulkString(Bytes::from_static(b"bench_counter")),
        ]);
        let cmd = Command::parse(&incr_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("numeric_incrby", |b| {
        let incrby_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"INCRBY")),
            RespValue::BulkString(Bytes::from_static(b"bench_counter2")),
            RespValue::BulkString(Bytes::from_static(b"5")),
        ]);
        let cmd = Command::parse(&incrby_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn bench_set_zset_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("set_sadd", |b| {
        let sadd_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SADD")),
            RespValue::BulkString(Bytes::from_static(b"bench_set")),
            RespValue::BulkString(Bytes::from_static(b"member")),
        ]);
        let cmd = Command::parse(&sadd_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("set_smembers", |b| {
        let smembers_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SMEMBERS")),
            RespValue::BulkString(Bytes::from_static(b"bench_set")),
        ]);
        let cmd = Command::parse(&smembers_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("zset_zadd", |b| {
        let zadd_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"ZADD")),
            RespValue::BulkString(Bytes::from_static(b"bench_zset")),
            RespValue::BulkString(Bytes::from_static(b"1.5")),
            RespValue::BulkString(Bytes::from_static(b"member")),
        ]);
        let cmd = Command::parse(&zadd_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("zset_zrange", |b| {
        let zrange_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"ZRANGE")),
            RespValue::BulkString(Bytes::from_static(b"bench_zset")),
            RespValue::BulkString(Bytes::from_static(b"0")),
            RespValue::BulkString(Bytes::from_static(b"10")),
        ]);
        let cmd = Command::parse(&zrange_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn bench_bulk_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create multiple keys
    for i in 0..10 {
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SET")),
            RespValue::BulkString(Bytes::from(format!("bulk_key_{}", i))),
            RespValue::BulkString(Bytes::from(format!("bulk_value_{}", i))),
        ]);
        rt.block_on(
            Command::parse(&set_cmd)
                .unwrap()
                .execute(&db, &mut BytesMut::new()),
        );
    }

    c.bench_function("bulk_mget", |b| {
        let mget_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"MGET")),
            RespValue::BulkString(Bytes::from_static(b"bulk_key_0")),
            RespValue::BulkString(Bytes::from_static(b"bulk_key_1")),
            RespValue::BulkString(Bytes::from_static(b"bulk_key_2")),
            RespValue::BulkString(Bytes::from_static(b"bulk_key_3")),
            RespValue::BulkString(Bytes::from_static(b"bulk_key_4")),
        ]);
        let cmd = Command::parse(&mget_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("bulk_mset", |b| {
        let mset_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"MSET")),
            RespValue::BulkString(Bytes::from_static(b"mset_key_a")),
            RespValue::BulkString(Bytes::from_static(b"mset_value_a")),
            RespValue::BulkString(Bytes::from_static(b"mset_key_b")),
            RespValue::BulkString(Bytes::from_static(b"mset_value_b")),
        ]);
        let cmd = Command::parse(&mset_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn bench_command_parsing(c: &mut Criterion) {
    c.bench_function("parse_get_command", |b| {
        let get_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"GET")),
            RespValue::BulkString(Bytes::from_static(b"test_key")),
        ]);
        b.iter(|| {
            black_box(Command::parse(&get_cmd));
        })
    });

    c.bench_function("parse_hgetall_command", |b| {
        let hgetall_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HGETALL")),
            RespValue::BulkString(Bytes::from_static(b"test_hash")),
        ]);
        b.iter(|| {
            black_box(Command::parse(&hgetall_cmd));
        })
    });
}

fn stress_test_concurrent_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);

    c.bench_function("stress_concurrent_sets", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter(|| {
            let mut handles = vec![];

            // Spawn 100 concurrent SET operations
            for i in 0..100 {
                let db_clone = db.clone();
                let handle = rt.spawn(async move {
                    let set_cmd = RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from_static(b"SET")),
                        RespValue::BulkString(Bytes::from(format!("stress_key_{}", i))),
                        RespValue::BulkString(Bytes::from(format!("stress_value_{}", i))),
                    ]);

                    if let Some(cmd) = Command::parse(&set_cmd) {
                        cmd.execute(&db_clone, &mut BytesMut::new()).await;
                    }
                });
                handles.push(handle);
            }

            // Wait for all operations to complete
            for handle in handles {
                rt.block_on(handle).unwrap();
            }
        })
    });

    c.bench_function("stress_large_hash_operations", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Pre-populate a large hash
        for i in 0..1000 {
            let hset_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"HSET")),
                RespValue::BulkString(Bytes::from_static(b"stress_large_hash")),
                RespValue::BulkString(Bytes::from(format!("field_{}", i))),
                RespValue::BulkString(Bytes::from(format!("value_{}", i))),
            ]);
            rt.block_on(
                Command::parse(&hset_cmd)
                    .unwrap()
                    .execute(&db, &mut BytesMut::new()),
            );
        }

        let hgetall_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HGETALL")),
            RespValue::BulkString(Bytes::from_static(b"stress_large_hash")),
        ]);
        let cmd = Command::parse(&hgetall_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn stress_test_memory_pressure(c: &mut Criterion) {
    let value100 = Bytes::from("x".repeat(100));
    let value1024 = Bytes::from("x".repeat(1024));

    c.bench_function("stress_many_keys", |b| {
        let db = Database::new_shared(16);
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            // Create 1000 keys in one iteration
            for i in 0..1000 {
                let set_cmd = RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from_static(b"SET")),
                    RespValue::BulkString(Bytes::from(format!("mem_key_{}", i))),
                    RespValue::BulkString(value100.clone()),
                ]);
                rt.block_on(
                    Command::parse(&set_cmd)
                        .unwrap()
                        .execute(&db, &mut BytesMut::new()),
                );
            }
        })
    });

    c.bench_function("stress_large_values", |b| {
        let db = Database::new_shared(16);
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            // Create keys with 1KB values
            for i in 0..100 {
                let set_cmd = RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from_static(b"SET")),
                    RespValue::BulkString(Bytes::from(format!("large_key_{}", i))),
                    RespValue::BulkString(value1024.clone()),
                ]);
                rt.block_on(
                    Command::parse(&set_cmd)
                        .unwrap()
                        .execute(&db, &mut BytesMut::new()),
                );
            }
        })
    });
}

fn stress_test_error_conditions(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("stress_invalid_commands", |b| {
        let invalid_cmds = vec![
            RespValue::Array(vec![RespValue::BulkString(Bytes::from_static(b"INVALID"))]),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"GET")),
                // Missing key
            ]),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"SET")),
                RespValue::BulkString(Bytes::from_static(b"key")),
                // Missing value
            ]),
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from_static(b"HGET")),
                RespValue::BulkString(Bytes::from_static(b"hash")),
                // Missing field
            ]),
        ];

        b.iter(|| {
            for cmd in &invalid_cmds {
                black_box(Command::parse(cmd));
            }
        })
    });

    c.bench_function("stress_type_conflicts", |b| {
        // Setup: create a string key
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"SET")),
            RespValue::BulkString(Bytes::from_static(b"conflict_key")),
            RespValue::BulkString(Bytes::from_static(b"string_value")),
        ]);
        rt.block_on(
            Command::parse(&set_cmd)
                .unwrap()
                .execute(&db, &mut BytesMut::new()),
        );

        let hget_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HGET")),
            RespValue::BulkString(Bytes::from_static(b"conflict_key")),
            RespValue::BulkString(Bytes::from_static(b"field")),
        ]);
        let cmd = Command::parse(&hget_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

fn stress_test_numeric_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("stress_numeric_overflow", |b| {
        let incrby_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"INCRBY")),
            RespValue::BulkString(Bytes::from_static(b"overflow_test")),
            RespValue::BulkString(Bytes::from(i64::MAX.to_string())),
        ]);
        let cmd = Command::parse(&incrby_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });

    c.bench_function("stress_floating_point_precision", |b| {
        let hincrbyfloat_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"HINCRBYFLOAT")),
            RespValue::BulkString(Bytes::from_static(b"float_hash")),
            RespValue::BulkString(Bytes::from_static(b"float_field")),
            RespValue::BulkString(Bytes::from_static(b"0.1")),
        ]);
        let cmd = Command::parse(&hincrbyfloat_cmd).unwrap();
        let mut out = BytesMut::new();
        b.iter(|| {
            out.clear();
            rt.block_on(cmd.clone().execute(&db, &mut out));
        })
    });
}

criterion_group!(
    benches,
    bench_string_operations,
    bench_hash_operations,
    bench_list_operations,
    bench_numeric_operations,
    bench_set_zset_operations,
    bench_bulk_operations,
    bench_command_parsing,
    stress_test_concurrent_operations,
    stress_test_memory_pressure,
    stress_test_error_conditions,
    stress_test_numeric_operations,
);
criterion_main!(benches);
