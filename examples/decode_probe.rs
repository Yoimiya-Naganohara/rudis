// Micro-benchmark: isolate where pipelined decoding time goes.
// Run with: cargo run --release --example decode_probe
use bytes::{Buf, Bytes, BytesMut};
use std::time::Instant;

fn main() {
    let ping = b"*1\r\n$4\r\nPING\r\n";
    let set = b"*3\r\n$3\r\nSET\r\n$8\r\nbenchkey\r\n$3\r\nval\r\n";
    for (name, frame_bytes, expect_frames) in [
        ("PING", &ping[..], 10_000usize),
        ("SET", &set[..], 10_000usize),
    ] {
        let mut payload = Vec::with_capacity(frame_bytes.len() * 10_000);
        for _ in 0..10_000 {
            payload.extend_from_slice(frame_bytes);
        }
        let data = Bytes::from(payload);

        // 1. decode + advance + Command::parse
        let mut d = data.clone();
        let mut out = BytesMut::with_capacity(4096);
        let t0 = Instant::now();
        let mut frames = 0usize;
        loop {
            match redis_protocol::resp2::decode::decode(&d) {
                Ok(Some((frame, consumed))) => {
                    d.advance(consumed);
                    let _cmd = rudis::commands::Command::parse(&frame);
                    out.extend_from_slice(b"+OK\r\n");
                    frames += 1;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        assert_eq!(frames, expect_frames);
        println!(
            "{} decode+parse: {} frames in {:?} ({:.2}us/frame)",
            name,
            frames,
            t0.elapsed(),
            t0.elapsed().as_secs_f64() * 1e6 / frames as f64
        );
    }

    // 2. database set: 10000 inserts of the same key
    use rudis::database::traits::StringOp;
    let db = rudis::database::Database::new_shared(16);
    let key = Bytes::from_static(b"benchkey");
    let value = Bytes::from_static(b"val");
    let t0 = Instant::now();
    for _ in 0..10_000 {
        db.set(&key, value.clone());
    }
    let elapsed = t0.elapsed();
    println!(
        "db.set x10000: {:?} ({:.2}us/op)",
        elapsed,
        elapsed.as_secs_f64() * 1e6 / 10_000.0
    );

    // 3. get_mut update path: same key already exists
    let t0 = Instant::now();
    for _ in 0..10_000 {
        db.set(&key, value.clone());
    }
    let elapsed = t0.elapsed();
    println!(
        "db.set update x10000: {:?} ({:.2}us/op)",
        elapsed,
        elapsed.as_secs_f64() * 1e6 / 10_000.0
    );
}
