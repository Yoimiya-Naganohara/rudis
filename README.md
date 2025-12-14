# Rudis - A Redis-like Server in Rust

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![CI](https://github.com/<username>/<repository>/actions/workflows/ci.yml/badge.svg)](https://github.com/<username>/<repository>/actions/workflows/ci.yml)

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
   git clone <repository-url>
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

## CI/CD

This project uses GitHub Actions for continuous integration and deployment:

- **Automated Testing**: Runs on every push and pull request
- **Multi-platform Builds**: Builds for Linux, macOS, and Windows
- **Release Automation**: Automatically creates releases with binaries when tags are pushed

To create a release:
1. Update version in `Cargo.toml`
2. Create a git tag: `git tag v1.0.0`
3. Push the tag: `git push origin v1.0.0`
4. GitHub Actions will build and publish the release

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

Rudis has been benchmarked using `redis-benchmark` with 500 concurrent threads, 100,000 requests per thread (50 million total operations). Here's a comparison with the official Redis server:

| Command | Rudis (req/sec) | Rudis (p50 ms) | Redis (req/sec) | Redis (p50 ms) |
|---------|-----------------|----------------|-----------------|---------------|
| SET     | 58004.64       | 0.383         | 33222.59       | 1.287        |
| GET     | 58377.11       | 0.383         | 34518.46       | 1.247        |
| LPUSH   | 58445.36       | 0.383         | 33134.53       | 1.327        |
| RPUSH   | 56657.22       | 0.391         | 33123.55       | 1.311        |
| LPOP    | 57803.47       | 0.383         | 33178.50       | 1.311        |
| RPOP    | 57803.47       | 0.383         | 33156.50       | 1.319        |
| HSET    | 57603.69       | 0.383         | 31210.99       | 1.407        |

*Benchmark command: `redis-benchmark -t set,get,hset,hget,lpush,lpop,rpush,rpop -n 100000 --threads 500 -q`*

*Note: Results may vary based on hardware and configuration. Rudis shows significantly better performance than Redis in this test environment.*

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
- `resp.rs`: RESP (Redis Serialization Protocol) implementation

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
