// Benchmarks for Rudis Redis Clone
// Tests performance of various Redis operations

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rudis::database::Database;
use rudis::commands::Command;
use rudis::networking::resp::RespValue;

fn bench_string_operations(c: &mut Criterion) {
    let db = Database::new_shared();
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("string_set", |b| {
        b.iter(|| {
            let set_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("SET".to_string())),
                RespValue::BulkString(Some("bench_key".to_string())),
                RespValue::BulkString(Some("bench_value".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&set_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("string_get", |b| {
        // Setup: ensure key exists
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Some("SET".to_string())),
            RespValue::BulkString(Some("bench_key".to_string())),
            RespValue::BulkString(Some("bench_value".to_string())),
        ]);

        if let Some(cmd) = Command::parse(&set_cmd) {
            rt.block_on(cmd.execute(&db));
        }

        b.iter(|| {
            let get_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("GET".to_string())),
                RespValue::BulkString(Some("bench_key".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&get_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_hash_operations(c: &mut Criterion) {
    let db = Database::new_shared();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create a hash with multiple fields
    for i in 0..100 {
        let hset_cmd = RespValue::Array(vec![
            RespValue::BulkString(Some("HSET".to_string())),
            RespValue::BulkString(Some("bench_hash".to_string())),
            RespValue::BulkString(Some(format!("field_{}", i))),
            RespValue::BulkString(Some(format!("value_{}", i))),
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
                RespValue::BulkString(Some("HSET".to_string())),
                RespValue::BulkString(Some("bench_hash".to_string())),
                RespValue::BulkString(Some(format!("new_field_{}", counter))),
                RespValue::BulkString(Some(format!("new_value_{}", counter))),
            ]);

            if let Some(cmd) = Command::parse(&hset_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("hash_hget", |b| {
        b.iter(|| {
            let hget_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("HGET".to_string())),
                RespValue::BulkString(Some("bench_hash".to_string())),
                RespValue::BulkString(Some("field_50".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hget_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("hash_hgetall_small", |b| {
        // Test with small hash (10 fields)
        let small_db = Database::new_shared();
        let small_rt = tokio::runtime::Runtime::new().unwrap();

        for i in 0..10 {
            let hset_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("HSET".to_string())),
                RespValue::BulkString(Some("small_hash".to_string())),
                RespValue::BulkString(Some(format!("field_{}", i))),
                RespValue::BulkString(Some(format!("value_{}", i))),
            ]);

            if let Some(cmd) = Command::parse(&hset_cmd) {
                small_rt.block_on(cmd.execute(&small_db));
            }
        }

        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("HGETALL".to_string())),
                RespValue::BulkString(Some("small_hash".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hgetall_cmd) {
                small_rt.block_on(cmd.execute(&small_db));
            }
        })
    });

    c.bench_function("hash_hgetall_large", |b| {
        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("HGETALL".to_string())),
                RespValue::BulkString(Some("bench_hash".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&hgetall_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_numeric_operations(c: &mut Criterion) {
    let db = Database::new_shared();
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("numeric_incr", |b| {
        b.iter(|| {
            let incr_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("INCR".to_string())),
                RespValue::BulkString(Some("bench_counter".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&incr_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });

    c.bench_function("numeric_incrby", |b| {
        b.iter(|| {
            let incrby_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("INCRBY".to_string())),
                RespValue::BulkString(Some("bench_counter2".to_string())),
                RespValue::BulkString(Some("5".to_string())),
            ]);

            if let Some(cmd) = Command::parse(&incrby_cmd) {
                rt.block_on(cmd.execute(&db));
            }
        })
    });
}

fn bench_bulk_operations(c: &mut Criterion) {
    let db = Database::new_shared();
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: create multiple keys
    for i in 0..10 {
        let set_cmd = RespValue::Array(vec![
            RespValue::BulkString(Some("SET".to_string())),
            RespValue::BulkString(Some(format!("bulk_key_{}", i))),
            RespValue::BulkString(Some(format!("bulk_value_{}", i))),
        ]);

        if let Some(cmd) = Command::parse(&set_cmd) {
            rt.block_on(cmd.execute(&db));
        }
    }

    c.bench_function("bulk_mget", |b| {
        b.iter(|| {
            let mget_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("MGET".to_string())),
                RespValue::BulkString(Some("bulk_key_0".to_string())),
                RespValue::BulkString(Some("bulk_key_1".to_string())),
                RespValue::BulkString(Some("bulk_key_2".to_string())),
                RespValue::BulkString(Some("bulk_key_3".to_string())),
                RespValue::BulkString(Some("bulk_key_4".to_string())),
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
                RespValue::BulkString(Some("MSET".to_string())),
                RespValue::BulkString(Some(format!("mset_key_{}_{}", counter, 0))),
                RespValue::BulkString(Some(format!("mset_value_{}_{}", counter, 0))),
                RespValue::BulkString(Some(format!("mset_key_{}_{}", counter, 1))),
                RespValue::BulkString(Some(format!("mset_value_{}_{}", counter, 1))),
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
                RespValue::BulkString(Some("GET".to_string())),
                RespValue::BulkString(Some("test_key".to_string())),
            ]);

            black_box(Command::parse(&get_cmd));
        })
    });

    c.bench_function("parse_hgetall_command", |b| {
        b.iter(|| {
            let hgetall_cmd = RespValue::Array(vec![
                RespValue::BulkString(Some("HGETALL".to_string())),
                RespValue::BulkString(Some("test_hash".to_string())),
            ]);

            black_box(Command::parse(&hgetall_cmd));
        })
    });
}

criterion_group!(
    benches,
    bench_string_operations,
    bench_hash_operations,
    bench_numeric_operations,
    bench_bulk_operations,
    bench_command_parsing
);
criterion_main!(benches);
