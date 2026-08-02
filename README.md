# Rudis - A Redis-like Server in Rust

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![CI](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml/badge.svg)](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml)

Rudis is a high-performance, Redis-compatible server implementation written in Rust. It provides a drop-in replacement for Redis with improved performance, memory efficiency, and safety guarantees through Rust's ownership system.

## Features

- **Redis Protocol Compatibility**: Supports core Redis commands and data structures
- **High Performance**: Leverages Rust's zero-cost abstractions for optimal speed
- **Memory Safe**: Prevents common memory errors through Rust's type system
- **Concurrent**: Handles multiple client connections efficiently
- **Persistence**: Supports RDB snapshots for data durability
- **Data Structures**: Implements strings, lists, hashes, sets, and sorted sets
- **Extensible**: Modular architecture for easy addition of new features

## Supported Commands

Rudis implements a subset of Redis commands, including:

### Strings
- `SET`, `GET`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`

### Hashes
- `HSET`, `HGET`, `HGETALL`, `HDEL`, `HKEYS`, `HVALS`, `HLEN`

### Lists
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`

### Sets
- `SADD`, `SMEMBERS`, `SREM`, `SCARD`

### Sorted Sets
- `ZADD`, `ZRANGE`, `ZREM`, `ZCARD`

*Note: Not all Redis commands are implemented yet. Check [TODO.md](TODO.md) for planned additions.*

## Prerequisites

- Rust 1.70 or later
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

Rudis uses default settings but can be configured via command-line arguments or environment variables. Check `src/main.rs` for available options.

## Performance

Rudis has been benchmarked against **Valkey 9.0.5** using `valkey-benchmark` on the same machine (16-core i5-13500H, WSL2). Persistence was disabled on both servers (Valkey started with `--save "" --appendonly no`; Rudis persistence is stubbed out). Results depend heavily on the load profile, so three profiles are reported.

### Profile 1: high connection concurrency (`--threads 500`, 5M requests per command)

| Command | Rudis (req/sec) | Rudis (p50 ms) | Valkey (req/sec) | Valkey (p50 ms) |
|---------|-----------------|----------------|------------------|-----------------|
| SET     | 201,499        | 0.207          | 87,923           | 0.527           |
| GET     | 199,457        | 0.215          | 89,524           | 0.519           |
| LPUSH   | 214,445        | 0.199          | 86,414           | 0.543           |
| RPUSH   | 207,779        | 0.199          | 88,726           | 0.527           |
| LPOP    | 216,816        | 0.191          | 89,526           | 0.527           |
| RPOP    | 223,984        | 0.191          | 90,779           | 0.527           |
| HSET    | 226,665        | 0.191          | 89,923           | 0.527           |

*Command: `valkey-benchmark -t set,get,hset,hget,lpush,lpop,rpush,rpop -n 5000000 --threads 500 -q`*

### Profile 2: steady load, 50 connections (`-c 50`, 1M requests per command)

| Command | Rudis (req/sec) | Rudis (p50 ms) | Valkey (req/sec) | Valkey (p50 ms) |
|---------|-----------------|----------------|------------------|-----------------|
| SET     | 253,678        | 0.111          | 227,376          | 0.111           |
| GET     | 248,200        | 0.111          | 220,946          | 0.119           |

### Profile 3: pipelined (`-c 50 -P 100`, 2M requests per command)

| Command | Rudis (req/sec) | Rudis (p50 ms) | Valkey (req/sec) | Valkey (p50 ms) |
|---------|-----------------|----------------|------------------|-----------------|
| SET     | 112,752        | 0.695          | 2,234,637        | 2.103           |
| GET     | 112,822        | 1.087          | 3,039,514        | 1.527           |

*Command: `valkey-benchmark -t set,get -n 2000000 -c 50 -P 100 -q`*

### Interpretation

- **Rudis wins under high connection concurrency** (Tokio multi-threaded runtime vs. Valkey's single-threaded event loop), but by a much smaller margin (roughly 10%) under steady low-connection load.
- **Rudis is far slower under pipelining** (about 20x): each pipelined request is processed sequentially and the per-request cost grows with batch size. The root cause is the networking loop in `src/networking/mod.rs`, which copies the whole receive buffer (`Bytes::copy_from_slice`) for every decoded frame (O(n^2) total work) and issues a separate `write_all` syscall per reply.

*Notes:*
- *HGET is omitted: the `hget` test in valkey-benchmark 9.0.5 silently produces no results on any server (verified against both Rudis and Valkey).*
- *Results may vary based on hardware and configuration.*

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
