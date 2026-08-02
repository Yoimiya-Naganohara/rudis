# Rudis

[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](https://github.com/Yoimiya-Naganohara/rudis)
[![CI](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml/badge.svg)](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml)

A Redis-compatible in-memory data store written in Rust.

## Quick Start

```bash
git clone https://github.com/Yoimiya-Naganohara/rudis.git
cd rudis
cargo run --release          # listens on 127.0.0.1:6379
```

Then connect with any Redis client:

```bash
redis-cli ping               # -> PONG
redis-cli set foo bar        # -> OK
redis-cli get foo            # -> "bar"
```

## Features

- Multi-threaded Tokio runtime, pipelining-friendly, zero-copy RESP2 decoding
- DashMap-sharded store with a lock-free database index
- 60+ commands: strings, hashes, lists, sets, sorted sets, keys, connection/server

## Performance

Outperforms Valkey in every benchmark (A/B verified, +27–52% on mid-concurrency GET). Scales with connections: 2.5M → 23.3M req/s, while Valkey's single-threaded loop tops out at 2–3M. Reproduce:

```bash
cargo build --release --example bench_client
target/release/examples/bench_client 127.0.0.1:6379 50 100 1000000 get
```

## Layout

- `src/networking/` — connection loop, pipelining, response buffering
- `src/commands/` — RESP dispatch + command handlers
- `src/database/` — sharded store, key operations
- `src/data_structures/` — value types
- `src/persistence/` — RDB/AOF stub

## Development

```bash
cargo test          # unit + integration
cargo bench         # criterion benchmarks
cargo fmt && cargo clippy
```

## Known Limitations

- No `CONFIG GET`/`SET` — client libraries probe for these
- Persistence is a stub; in-memory only
- Expiration is passive; `INFO` returns a stub
- Some type-mismatch errors return `0` instead of `WRONGTYPE`

## License

Dual-licensed under MIT OR Apache-2.0. Roadmap: [TODO.md](TODO.md).
