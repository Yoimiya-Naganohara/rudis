# Rudis - A Redis-like Server in Rust

[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![CI](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml/badge.svg)](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml)

Rudis is a high-performance, Redis-compatible server implementation written in Rust. It provides a drop-in replacement for Redis with improved performance, memory efficiency, and safety guarantees through Rust's ownership system.

## Features

- **Redis Protocol Compatibility**: Supports core Redis commands and data structures
- **High Performance**: Multi-threaded Tokio runtime with zero-copy RESP decoding and allocation-free response formatting
- **Memory Safe**: Prevents common memory errors through Rust's type system
- **Concurrent**: DashMap-sharded data store with per-shard locking for concurrent reads and writes
- **Persistence**: Supports RDB snapshots for data durability
- **Data Structures**: Implements strings, lists, hashes, sets, and sorted sets
- **Extensible**: Modular architecture for easy addition of new features

## Supported Commands

Rudis implements a subset of Redis commands, including:

### Connection
- `PING`, `QUIT`, `ECHO`, `AUTH`, `SELECT`, `INFO`

### Strings
- `SET`, `GET`, `SETNX`, `SETEX`, `GETSET`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `DEL`

### Hashes
- `HSET`, `HGET`, `HGETALL`, `HDEL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HINCRBY`, `HINCRBYFLOAT`

### Lists
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LTRIM`, `LSET`, `LINSERT`

### Sets
- `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SINTER`, `SUNION`, `SDIFF`

### Sorted Sets
- `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZREM`, `ZCARD`, `ZSCORE`, `ZRANK`

### Keys
- `EXISTS`, `EXPIRE`, `TTL`, `TYPE`, `KEYS`, `FLUSHALL`, `FLUSHDB`

*Note: Not all Redis commands are implemented yet. Check [TODO.md](TODO.md) for planned additions.*

## Prerequisites

- Rust 1.80 or later
- Cargo (comes with Rust)

## Installation

### Building from Source

1. **Clone the repository**
   ```bash
   git clone https://github.com/Yoimiya-Naganohara/rudis.git
   cd rudis
   ```

2. **Build the project**
   ```bash
   cargo build --release
   ```

3. **Run the server**
   ```bash
   cargo run --release
   ```

The server will start on the default port (typically 6379).

## Usage

### Basic Commands

Connect using any Redis client (e.g., `redis-cli`):

```bash
redis-cli -p 6379
```

Example commands:
```redis
SET key "Hello, Rudis!"
GET key
HSET myhash field1 "value1"
HGET myhash field1
LPUSH mylist "item1"
LPOP mylist
```

### Configuration

The server binds to `127.0.0.1:6379` with 16 databases by default. Configuration is hard-coded in `src/config/mod.rs` (config-file loading is on the roadmap, see [TODO.md](TODO.md)).

## Performance

Rudis is benchmarked against **Valkey 9.0.5** on the same machine (16-core, WSL2) with persistence disabled on both servers. Two clients are used:

- **`bench_client`** (in `examples/`): an independent Tokio load client with real TCP connections and RESP pipelining (`cargo run --release --example bench_client -- <addr> <conns> <pipeline> <total> <get|set|hset>`).
- `valkey-benchmark` (cross-checked: its numbers match `bench_client` for Valkey within ~3%, but it **underreports Rudis** because its single-threaded client caps out around 9M req/s — below what Rudis can serve).

Numbers below are single runs of `bench_client`, 1M requests per command, fixed key per connection (valkey-benchmark random-key results for Rudis are client-bound and not representative).

### Deep pipelining (`-c 50 -P 100` and `-c 100 -P 200` equivalents)

| Command | Rudis (req/sec) | Valkey (req/sec) | Delta |
|---------|-----------------|------------------|-------|
| GET (50x100)   | 13,200,000 | 2,960,000 | +346% |
| GET (100x200)  | 21,600,000 | 2,920,000 | +640% |
| SET (50x100)   | 13,500,000 | 2,130,000 | +534% |
| SET (100x200)  | 17,900,000 | 2,270,000 | +688% |
| HSET (50x100)  | 11,900,000 | 2,130,000 | +558% |

### Low concurrency (500K requests per command)

| Config     | GET (Rudis/Valkey) |
|------------|--------------------|
| `1x500`    | 2.53M / 2.17M     |
| `16x16`    | 3.53M / 1.83M     |

### Interpretation

- **Rudis scales with connection count** (2.5M single-connection up to 21.6M at 100 connections x 200 pipeline) thanks to the multi-threaded Tokio runtime and DashMap-sharded data store; **Valkey's single-threaded event loop tops out at 2-3M** regardless of load.
- At a single connection both servers are close (pipeline round-trip bound).
- valkey-benchmark data published in earlier revisions of this README understated Rudis (client bottleneck); the `bench_client` numbers above are the authoritative ones.

### Implementation notes

- Zero-copy RESP decoding via `redis-protocol`'s `decode-mut` feature (bulk strings are refcounted slices, no per-frame copying)
- Response formatting writes directly into the connection's output buffer (no intermediate allocations)
- `AtomicU8` database index instead of a global mutex
- Single-lookup hash inserts via `HashMap::entry`

## Project Structure

### Core Source Code (`src/`)

- `main.rs`: Application entry point, server initialization
- `lib.rs`: Library exports and shared utilities
- `error.rs`: Error handling types

#### `src/server/`
- `mod.rs`: Core server logic, client management, and event loop

#### `src/commands/`
- `mod.rs`: Command parsing and routing
- `command_helper.rs`: Helper functions for command processing
- `errors.rs`: Command-specific error handling

#### `src/database/`
- `mod.rs`: In-memory database implementation

#### `src/persistence/`
- `mod.rs`: Persistence mechanisms (RDB snapshots)

#### `src/networking/`
- `mod.rs`: TCP networking and Redis protocol handling
- `resp.rs`: RESP type alias (backed by `redis-protocol` crate)

#### `src/data_structures/`
- `mod.rs`: Data structure module declarations
- `string.rs`: String operations
- `list.rs`: List operations
- `hash.rs`: Hash/dictionary operations
- `set.rs`: Set operations
- `sorted_set.rs`: Sorted set with scoring

### Benchmarks (`benches/`)
- `redis_benchmark.rs`: Performance benchmarks for various operations

### Tests (`tests/`)
- `integration_tests.rs`: End-to-end integration tests
- `unit_tests.rs`: Unit tests for individual components
- Various command-specific tests (e.g., `hdel_test.rs`, `hkeys_test.rs`)

### Build Artifacts (`target/`)
- Automatically generated by Cargo (ignored in version control)

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐
│   Networking    │◄──►│     Server      │
│   (TCP/RESP)    │    │   (Main Loop)   │
└─────────────────┘    └─────────────────┘
                               │
                               ▼
┌─────────────────┐    ┌─────────────────┐
│   Commands      │◄──►│   Database      │
│   (Parsing &    │    │   (In-Memory    │
│    Routing)     │    │     Store)      │
└─────────────────┘    └─────────────────┘
                               │
                               ▼
┌─────────────────┐    ┌─────────────────┐
│ Data Structures │    │  Persistence    │
│ (Strings, Lists,│    │     (RDB)       │
│  Hashes, Sets)  │    └─────────────────┘
└─────────────────┘
```

## Development

### Setup
```bash
cargo build
```

### Running Tests
```bash
cargo test
```

### Benchmarks
```bash
cargo bench
```

### Code Quality
- `cargo check`: Quick compilation checks
- `cargo fmt`: Format code
- `cargo clippy`: Linting

## Testing

Rudis includes comprehensive tests to ensure reliability:

### Unit Tests
```bash
cargo test --lib
```

### Integration Tests
```bash
cargo test --test integration_tests
```

### All Tests
```bash
cargo test
```

### Benchmarks
```bash
cargo bench
```

Test coverage includes:
- Command parsing and execution
- Data structure operations
- Network protocol handling
- Concurrent access patterns
- Persistence functionality

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Guidelines
- Follow the existing code structure
- Add tests for new features
- Update documentation as needed
- Ensure all checks pass (`cargo fmt`, `cargo clippy`, `cargo test`)

## Roadmap

See [TODO.md](TODO.md) for planned features and improvements.

## Acknowledgements

- Inspired by [Redis](https://redis.io/), the original in-memory data structure store
- Built with [Rust](https://www.rust-lang.org/) for performance and safety
- Uses the RESP protocol for Redis compatibility

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
