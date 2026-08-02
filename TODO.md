# Rudis - Todo List

Status of the project as of the latest benchmark round. Checked items are implemented and covered by tests where noted.

## Core Infrastructure
- [x] TCP server with async runtime (Tokio, multi-threaded)
- [x] RESP protocol parser (zero-copy via `redis-protocol` `decode-mut`)
- [x] Command dispatcher and handler framework
- [x] Logging (`tracing`) and error handling
- [x] Connection handling (per-connection tasks, pipelined batch processing)
- [x] Response buffering with flush threshold

## Data Structures
- [x] String (`RedisString`)
- [x] Hash (`RedisHash`, backed by `HashMap<Bytes, Bytes>`)
- [x] List (`RedisList`, backed by `VecDeque`)
- [x] Set (`RedisSet`)
- [x] Sorted Set (`RedisSortedSet`, `HashMap` + `BTreeSet`)
- [x] Expiration (`EXPIRE`/`TTL`; passive cleanup policy still TBD, see below)

## Core Commands
### String Commands
- [x] SET (with NX/XX/EX/PX/KEEPTTL), GET, DEL
- [x] SETNX, SETEX, GETSET
- [x] INCR, DECR, INCRBY, DECRBY
- [x] APPEND, STRLEN
- [x] MGET, MSET

### Hash Commands
- [x] HSET (multi-pair), HGET, HDEL (multi-field)
- [x] HGETALL, HKEYS, HVALS
- [x] HLEN, HEXISTS
- [x] HINCRBY, HINCRBYFLOAT

### List Commands
- [x] LPUSH, RPUSH, LPOP, RPOP
- [x] LLEN, LINDEX, LRANGE
- [x] LTRIM, LSET, LINSERT

### Set Commands
- [x] SADD, SREM, SMEMBERS
- [x] SCARD, SISMEMBER
- [x] SINTER, SUNION, SDIFF

### Sorted Set Commands
- [x] ZADD (returns added count per Redis semantics), ZREM, ZSCORE
- [x] ZRANGE, ZRANGEBYSCORE, ZRANK, ZCARD
- [ ] ZREVRANGE, ZREVRANGEBYSCORE
- [ ] ZCOUNT, ZINCRBY, ZPOPMIN/ZPOPMAX
- [ ] ZADD options (NX/XX/GT/LT/CH/INCR)

### Key Management
- [x] EXISTS, TYPE, KEYS
- [x] EXPIRE, TTL
- [ ] PEXPIRE, PTTL
- [ ] RENAME, RENAMENX
- [ ] RANDOMKEY, SCAN

## Correctness & Compatibility Gaps
- [ ] `CONFIG GET`/`SET` (valkey-benchmark and client libraries probe for it; see "WARNING: Could not fetch server CONFIG")
- [ ] `SADD`/`ZADD` return `0` instead of `WRONGTYPE` error on type mismatch (trait signatures return `usize`; would need `Result<usize>`)
- [ ] Passive key expiration: expired keys are only checked when accessed; no active/background cleanup
- [ ] `INFO` returns a stub; no stats tracking (connections, commands, keyspace)

## Performance
- [x] Zero-copy RESP decoding (`decode-mut` feature)
- [x] Response formatting writes directly into the connection output buffer
- [x] `AtomicU8` database index (no global mutex on the hot path)
- [x] Single-lookup hash inserts (`HashMap::entry`)
- [x] Benchmarked against Valkey 9.0.5 (see README): 4-8x faster under deep pipelining with a real TCP client; Valkey's single-threaded event loop tops out at 2-3M req/s
- [ ] Add random-key support to the `bench_client` example to match valkey-benchmark's key distribution (current numbers use fixed keys per connection)
- [ ] Small-hash listpack-style encoding (`Vec` storage under a threshold, convert to `HashMap` above) — closes the HSET gap under deep pipelining
- [ ] Reuse per-connection response buffer growth policy review (currently grows by 64 KiB per read cycle)
- [ ] `zrank` is O(n) (`BTreeSet` position scan); an index structure would make it O(log n)

## Persistence
- [ ] RDB snapshot functionality (stub exists, not wired into the server)
- [ ] AOF (Append Only File) logging
- [ ] Background save process
- [ ] Automatic snapshot scheduling
- [ ] SAVE, BGSAVE commands

## Advanced Features
- [ ] Pub/Sub messaging
- [ ] Transaction support (MULTI/EXEC/DISCARD)
- [ ] Lua scripting engine
- [ ] Replication (master-slave)
- [ ] Clustering support

## Configuration & Administration
- [x] INFO command (stub response)
- [ ] Configuration file parsing (stub exists in `src/config/mod.rs`)
- [ ] CONFIG GET/SET
- [ ] Use `max_connections` (currently unused)

## Testing & Quality
- [x] Unit tests for all data structures (38 tests total, including sorted-set semantics, NaN score rejection, hash edge cases)
- [x] Integration tests for command handling
- [x] Criterion benchmarks (parse, per-command, concurrent stress)
- [ ] Memory usage optimization (bounded response buffers, allocation profiling)
- [ ] Run `cargo clippy` and fix warnings (several dead-code warnings remain)

## Documentation
- [x] README with command list, benchmark results, and implementation notes
- [ ] API documentation (rustdoc)
- [ ] Deployment instructions
