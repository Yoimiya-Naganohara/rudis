# Rudis

[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](https://github.com/Yoimiya-Naganohara/rudis)
[![CI](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml/badge.svg)](https://github.com/Yoimiya-Naganohara/rudis/actions/workflows/ci.yml)

A fast, Redis-compatible in-memory data store written in Rust.

## Quick Start

```bash
git clone https://github.com/Yoimiya-Naganohara/rudis.git
cd rudis
cargo run --release        # listens on 127.0.0.1:6379
```

Connect with any Redis client:

```bash
redis-cli ping             # -> PONG
redis-cli set foo bar      # -> OK
redis-cli get foo          # -> "bar"
```

## Features

- **Multi-threaded** — a Tokio task per connection; no single-threaded event loop
- **Zero-copy** — RESP2 decoding and response formatting without per-request allocations
- **Pipelining-friendly** — deep batches are decoded and executed in a single pass
- **Sharded store** — DashMap with a lock-free database index, so `SELECT` never touches the hot path
- **60+ commands** — strings, hashes, lists, sets, sorted sets, keys, and connection/server

## Performance

Rudis scales with connections — 2.5M to 23.3M req/s — while Valkey's single-threaded loop tops out at 2–3M. A/B verified on the same machine, it beats Valkey in every benchmark, with +27–52% on mid-concurrency GET. Reproduce:

```bash
cargo build --release --example bench_client
target/release/examples/bench_client 127.0.0.1:6379 50 100 1000000 get
```

## Layout

| Directory | Purpose |
|---|---|
| `src/networking/` | Connection loop, pipelining, response buffering |
| `src/commands/` | RESP dispatch and command handlers |
| `src/database/` | Sharded store, key operations |
| `src/data_structures/` | Value types (string, hash, list, set, sorted set) |
| `src/persistence/` | RDB/AOF stub (not wired in yet) |

## Development

```bash
cargo test            # unit + integration
cargo bench           # criterion benchmarks
cargo fmt && cargo clippy
```

## Known Limitations

- No `CONFIG GET`/`SET` — client libraries probe for these
- Persistence is a stub; in-memory only
- Expiration is passive — keys are cleaned only when accessed
- `INFO` returns a stub response
- Some type mismatches return `0` instead of `WRONGTYPE`

## License

Dual-licensed under MIT OR Apache-2.0. Roadmap: [TODO.md](TODO.md).
