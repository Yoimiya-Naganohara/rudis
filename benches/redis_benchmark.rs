// Benchmarks for Rudis Redis Clone
// Tests performance of various Redis operations

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rudis::commands::Command;
use rudis::database::Database;
use rudis::networking::resp::RespValue;

fn bench_string_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("string_set", |b| {
        b.iter(|| {
            let set_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("SET".to_string())),
                RespValue::BulkString(Bytes::from("bench_key".to_string())),
                RespValue::BulkString(Bytes::from("bench_value".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&set_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("string_get", |b| {
        // Setup: ensure key exists
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("SET".to_string())),
            RespValue::BulkString(Bytes::from("bench_key".to_string())),
            RespValue::BulkString(Bytes::from("bench_value".to_string())),
        ]);

        if let Some(cmd) = Command::parse(&set_cmd) {
            rt.block_on(cmd.execute(&db));
        }

        b.iter(|| {
            let get_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("GET".to_string())),
                RespValue::BulkString(Bytes::from("bench_key".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&get_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_hash_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create a hash with multiple fields
    for i in 0..100 {
        let hset_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("HSET".to_string())),
            RespValue::BulkString(Bytes::from("bench_hash".to_string())),
            RespValue::BulkString(Bytes::from(format!("field_{}", i))),
            RespValue::BulkString(Bytes::from(format!("value_{}", i))),
        ]);

        if let Some(cmd) = Command::parse(&hset_cmd) {
            rt.block_on(cmd.execute(&db));
        }
    }

    c.bench_function("hash_hset", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let hset_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HSET".to_string())),
                RespValue::BulkString(Bytes::from("bench_hash".to_string())),
                RespValue::BulkString(Bytes::from(format!("new_field_{}", counter))),
                RespValue::BulkString(Bytes::from(format!("new_value_{}", counter))),
            ]);

            if let Some(cmd) = Command::parse(&hset_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("hash_hget", |b| {
        b.iter(|| {
            let hget_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HGET".to_string())),
                RespValue::BulkString(Bytes::from("bench_hash".to_string())),
                RespValue::BulkString(Bytes::from("field_50".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hget_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("hash_hgetall_small", |b| {
        // Test with small hash (10 fields)
        let small_db = Database::new_shared(16);
        let small_rt = tokio::runtime::Runtime::new().unwrap();

        for i in 0..10 {
            let hset_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HSET".to_string())),
                RespValue::BulkString(Bytes::from("small_hash".to_string())),
                RespValue::BulkString(Bytes::from(format!("field_{}", i))),
                RespValue::BulkString(Bytes::from(format!("value_{}", i))),
            ]);

            if let Some(cmd) = Command::parse(&hset_cmd) {
                small_rt.block_on(cmd.execute(&small_db));
            }
        }

        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HGETALL".to_string())),
                RespValue::BulkString(Bytes::from("small_hash".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hgetall_cmd) {
                small_rt.block_on(cmd.execute(&small_db));
            }
        })
    });

    c.bench_function("hash_hgetall_large", |b| {
        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HGETALL".to_string())),
                RespValue::BulkString(Bytes::from("bench_hash".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hgetall_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_list_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create a list with some items
    for i in 0..100 {
        let rpush_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("RPUSH".to_string())),
            RespValue::BulkString(Bytes::from("bench_list".to_string())),
            RespValue::BulkString(Bytes::from(format!("item_{}", i))),
        ]);

        if let Some(cmd) = Command::parse(&rpush_cmd) {
            rt.block_on(cmd.execute(&db));
        }
    }

    c.bench_function("list_rpush", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let rpush_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("RPUSH".to_string())),
                RespValue::BulkString(Bytes::from("bench_list".to_string())),
                RespValue::BulkString(Bytes::from(format!("new_item_{}", counter))),
            ]);

            if let Some(cmd) = Command::parse(&rpush_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("list_lpop", |b| {
        b.iter(|| {
            let lpop_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("LPOP".to_string())),
                RespValue::BulkString(Bytes::from("bench_list".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&lpop_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("list_lrange", |b| {
        b.iter(|| {
            let lrange_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("LRANGE".to_string())),
                RespValue::BulkString(Bytes::from("bench_list".to_string())),
                RespValue::BulkString(Bytes::from("0".to_string())),
                RespValue::BulkString(Bytes::from("10".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&lrange_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("list_lindex", |b| {
        b.iter(|| {
            let lindex_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("LINDEX".to_string())),
                RespValue::BulkString(Bytes::from("bench_list".to_string())),
                RespValue::BulkString(Bytes::from("50".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&lindex_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_numeric_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("numeric_incr", |b| {
        b.iter(|| {
            let incr_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("INCR".to_string())),
                RespValue::BulkString(Bytes::from("bench_counter".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&incr_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("numeric_incrby", |b| {
        b.iter(|| {
            let incrby_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("INCRBY".to_string())),
                RespValue::BulkString(Bytes::from("bench_counter2".to_string())),
                RespValue::BulkString(Bytes::from("5".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&incrby_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_bulk_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create multiple keys
    for i in 0..10 {
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("SET".to_string())),
            RespValue::BulkString(Bytes::from(format!("bulk_key_{}", i))),
            RespValue::BulkString(Bytes::from(format!("bulk_value_{}", i))),
        ]);

        if let Some(cmd) = Command::parse(&set_cmd) {
            rt.block_on(cmd.execute(&db));
        }
    }

    c.bench_function("bulk_mget", |b| {
        b.iter(|| {
            let mget_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("MGET".to_string())),
                RespValue::BulkString(Bytes::from("bulk_key_0".to_string())),
                RespValue::BulkString(Bytes::from("bulk_key_1".to_string())),
                RespValue::BulkString(Bytes::from("bulk_key_2".to_string())),
                RespValue::BulkString(Bytes::from("bulk_key_3".to_string())),
                RespValue::BulkString(Bytes::from("bulk_key_4".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&mget_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("bulk_mset", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let mset_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("MSET".to_string())),
                RespValue::BulkString(Bytes::from(format!("mset_key_{}_{}", counter, 0))),
                RespValue::BulkString(Bytes::from(format!("mset_value_{}_{}", counter, 0))),
                RespValue::BulkString(Bytes::from(format!("mset_key_{}_{}", counter, 1))),
                RespValue::BulkString(Bytes::from(format!("mset_value_{}_{}", counter, 1))),
            ]);

            if let Some(cmd) = Command::parse(&mset_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_command_parsing(c: &mut Criterion) {
    c.bench_function("parse_get_command", |b| {
        b.iter(|| {
            let get_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("GET".to_string())),
                RespValue::BulkString(Bytes::from("test_key".to_string())),
            ]);

            black_box(Command::parse(&get_cmd));
        })
    });

    c.bench_function("parse_hgetall_command", |b| {
        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HGETALL".to_string())),
                RespValue::BulkString(Bytes::from("test_hash".to_string())),
            ]);

            black_box(Command::parse(&hgetall_cmd));
        })
    });
}

fn stress_test_concurrent_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);

    c.bench_function("stress_concurrent_sets", |b| {
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut handles = vec![];

            // Spawn 100 concurrent SET operations
            for i in 0..100 {
                let db_clone = db.clone();
                let handle = rt.spawn(async move {
                    let set_cmd = RespValue::Array(vec![
                        RespValue::BulkString(Bytes::from("SET".to_string())),
                        RespValue::BulkString(Bytes::from(format!("stress_key_{}", i))),
                        RespValue::BulkString(Bytes::from(format!("stress_value_{}", i))),
                    ]);

                    if let Some(cmd) = Command::parse(&set_cmd) {
                        cmd.execute(&db_clone).await;
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
                RespValue::BulkString(Bytes::from("HSET".to_string())),
                RespValue::BulkString(Bytes::from("stress_large_hash".to_string())),
                RespValue::BulkString(Bytes::from(format!("field_{}", i))),
                RespValue::BulkString(Bytes::from(format!("value_{}", i))),
            ]);

            if let Some(cmd) = Command::parse(&hset_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        }

        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HGETALL".to_string())),
                RespValue::BulkString(Bytes::from("stress_large_hash".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hgetall_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn stress_test_memory_pressure(c: &mut Criterion) {
    c.bench_function("stress_many_keys", |b| {
        let db = Database::new_shared(16);
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            // Create 1000 keys in one iteration
            for i in 0..1000 {
                let set_cmd = RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("SET".to_string())),
                    RespValue::BulkString(Bytes::from(format!("mem_key_{}", i))),
                    RespValue::BulkString(Bytes::from("x".repeat(100))), // 100 byte values
                ]);

                if let Some(cmd) = Command::parse(&set_cmd) {
                    rt.block_on(cmd.execute(&db));
                }
            }
        })
    });

    c.bench_function("stress_large_values", |b| {
        let db = Database::new_shared(16);
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            // Create keys with 1KB values
            for i in 0..100 {
                let large_value = "x".repeat(1024);
                let set_cmd = RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("SET".to_string())),
                    RespValue::BulkString(Bytes::from(format!("large_key_{}", i))),
                    RespValue::BulkString(Bytes::from(large_value)),
                ]);

                if let Some(cmd) = Command::parse(&set_cmd) {
                    rt.block_on(cmd.execute(&db));
                }
            }
        })
    });
}

fn stress_test_error_conditions(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("stress_invalid_commands", |b| {
        b.iter(|| {
            // Test various invalid commands
            let invalid_cmds = vec![
                RespValue::Array(vec![RespValue::BulkString(Bytes::from(
                    "INVALID".to_string(),
                ))]),
                RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("GET".to_string())),
                    // Missing key
                ]),
                RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("SET".to_string())),
                    RespValue::BulkString(Bytes::from("key".to_string())),
                    // Missing value
                ]),
                RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("HGET".to_string())),
                    RespValue::BulkString(Bytes::from("hash".to_string())),
                    // Missing field
                ]),
            ];

            for cmd in invalid_cmds {
                black_box(Command::parse(&cmd));
            }
        })
    });

    c.bench_function("stress_type_conflicts", |b| {
        // Setup: create a string key
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("SET".to_string())),
            RespValue::BulkString(Bytes::from("conflict_key".to_string())),
            RespValue::BulkString(Bytes::from("string_value".to_string())),
        ]);

        if let Some(cmd) = Command::parse(&set_cmd) {
            rt.block_on(cmd.execute(&db));
        }

        b.iter(|| {
            // Try hash operations on string key
            let hget_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HGET".to_string())),
                RespValue::BulkString(Bytes::from("conflict_key".to_string())),
                RespValue::BulkString(Bytes::from("field".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hget_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn stress_test_numeric_operations(c: &mut Criterion) {
    let db = Database::new_shared(16);
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("stress_numeric_overflow", |b| {
        b.iter(|| {
            // Test with very large numbers
            let large_num = i64::MAX.to_string();
            let incrby_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("INCRBY".to_string())),
                RespValue::BulkString(Bytes::from("overflow_test".to_string())),
                RespValue::BulkString(Bytes::from(large_num)),
            ]);

            if let Some(cmd) = Command::parse(&incrby_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("stress_floating_point_precision", |b| {
        b.iter(|| {
            // Test floating point operations
            let hincrbyfloat_cmd = RespValue::Array(vec![
                RespValue::BulkString(Bytes::from("HINCRBYFLOAT".to_string())),
                RespValue::BulkString(Bytes::from("float_hash".to_string())),
                RespValue::BulkString(Bytes::from("float_field".to_string())),
                RespValue::BulkString(Bytes::from("0.1".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hincrbyfloat_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_string_operations,
    bench_hash_operations,
    bench_list_operations,
    bench_numeric_operations,
    bench_bulk_operations,
    bench_command_parsing,
    stress_test_concurrent_operations,
    stress_test_memory_pressure,
    stress_test_error_conditions,
    stress_test_numeric_operations
);
criterion_main!(benches);
