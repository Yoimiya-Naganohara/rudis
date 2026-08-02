// Independent load-test client: real TCP connections with RESP pipelining.
// Used to cross-check valkey-benchmark results against a self-contained client.
//
// Usage:
//   cargo run --release --example bench_client -- <addr> <conns> <pipeline> <total> <get|set|hset>

use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn resp_cmd(name: &str, args: &[&str]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(format!("*{}\r\n", args.len() + 1).as_bytes());
    buf.extend_from_slice(format!("${}\r\n{}\r\n", name.len(), name).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    buf
}

// Minimal RESP frame reader: returns the payload of one frame (the value of
// +OK / :int / $bulk replies) so callers can verify reply contents.
// Supports +, -, : and $ replies (no array replies needed by this client).
async fn read_line(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<Vec<u8>> {
    loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line_end = pos + 2;
            let kind = buf[0];
            let rest = buf[1..pos].to_vec();
            buf.drain(..line_end);
            match kind {
                b'+' | b'-' | b':' => return Ok(rest),
                b'$' => {
                    let len: isize = std::str::from_utf8(&rest).unwrap().parse().unwrap();
                    if len == -1 {
                        return Ok(Vec::new());
                    }
                    // Need len + 2 more bytes
                    while buf.len() < len as usize + 2 {
                        let mut tmp = [0u8; 8192];
                        let n = stream.read(&mut tmp).await?;
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    let payload = buf[..len as usize].to_vec();
                    buf.drain(..len as usize + 2);
                    return Ok(payload);
                }
                b'*' => {
                    panic!("array replies not supported by bench client");
                }
                _ => panic!("unexpected RESP byte {:?}", kind as char),
            }
        }
        let mut tmp = [0u8; 8192];
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn worker(
    addr: String,
    conn: usize,
    pipeline: usize,
    per_conn: u64,
    cmd: String,
    latencies: std::sync::Arc<std::sync::Mutex<Vec<f64>>>,
) {
    let mut stream = TcpStream::connect(&addr).await.unwrap();

    let (setup, batch) = match cmd.as_str() {
        "get" => {
            let key = format!("bench_key_{}", conn);
            let setup = resp_cmd("SET", &[&key, "value"]);
            let batch = resp_cmd("GET", &[&key]);
            (setup, batch)
        }
        "set" => {
            let key = format!("bench_key_{}", conn);
            let setup = resp_cmd("SET", &[&key, "value"]);
            let batch = resp_cmd("SET", &[&key, "value"]);
            (setup, batch)
        }
        "hset" => {
            let key = format!("bench_hash_{}", conn);
            let setup = resp_cmd("HSET", &[&key, "field", "value"]);
            let batch = resp_cmd("HSET", &[&key, "field", "value"]);
            (setup, batch)
        }
        _ => panic!("unknown command"),
    };

    // Setup: ensure the key exists, and verify the reply
    stream.write_all(&setup).await.unwrap();
    let mut rbuf = Vec::new();
    let setup_reply = read_line(&mut stream, &mut rbuf).await.unwrap();
    // HSET replies with the number of newly added fields (0 when re-running
    // against a server where the field already exists), SET replies +OK
    match cmd.as_str() {
        "hset" => assert!(
            setup_reply == b"0" || setup_reply == b"1",
            "SETUP failed: {:?}",
            setup_reply
        ),
        _ => assert_eq!(setup_reply, b"OK", "SETUP failed: {:?}", setup_reply),
    }

    // Pre-build the pipelined batch
    let mut batch_bytes = Vec::with_capacity(batch.len() * pipeline);
    for _ in 0..pipeline {
        batch_bytes.extend_from_slice(&batch);
    }
    let expected_reply: &[u8] = match cmd.as_str() {
        "get" => b"value", // $1\r\nvalue\r\n
        "set" => b"OK",    // +OK\r\n
        "hset" => b"0",    // :1\r\n or :0\r\n (added or updated)
        _ => unreachable!(),
    };

    let mut batches = per_conn / pipeline as u64;
    let mut leftover = per_conn % pipeline as u64;

    let mut samples = Vec::new();
    while batches > 0 || leftover > 0 {
        let count = if batches > 0 {
            batches -= 1;
            pipeline
        } else {
            let c = leftover as usize;
            leftover = 0;
            c
        };
        let t0 = Instant::now();
        stream
            .write_all(&batch_bytes[..batch.len() * count])
            .await
            .unwrap();
        for _ in 0..count {
            let reply = read_line(&mut stream, &mut rbuf).await.unwrap();
            if cmd == "hset" {
                assert!(
                    reply == b"0" || reply == b"1",
                    "reply mismatch: {:?}",
                    reply
                );
            } else {
                assert_eq!(reply, expected_reply, "reply mismatch: {:?}", reply);
            }
        }
        let t1 = Instant::now();
        samples.push(t1.duration_since(t0).as_secs_f64() * 1e6 / count as f64); // us per request
    }

    latencies.lock().unwrap().extend(samples);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 {
        eprintln!("usage: bench_client <addr> <conns> <pipeline> <total> <get|set|hset>");
        std::process::exit(1);
    }
    let addr = args[1].clone();
    let conns: usize = args[2].parse().unwrap();
    let pipeline: usize = args[3].parse().unwrap();
    let total: u64 = args[4].parse().unwrap();
    let cmd = args[5].clone();

    let per_conn = total / conns as u64;
    let latencies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    let t0 = Instant::now();
    let mut handles = Vec::new();
    for conn in 0..conns {
        let addr = addr.clone();
        let cmd = cmd.clone();
        let lat = latencies.clone();
        handles.push(tokio::spawn(async move {
            worker(addr, conn, pipeline, per_conn, cmd, lat).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = t0.elapsed().as_secs_f64();

    let mut samples = latencies.lock().unwrap().clone();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |q: f64| -> f64 {
        let idx = ((samples.len() as f64) * q).floor() as usize;
        samples[idx.min(samples.len() - 1)]
    };

    println!(
        "{}: {} conns, pipeline {}, {} reqs: {:.0} req/s, p50 {:.1}us, p95 {:.1}us, p99 {:.1}us",
        cmd,
        conns,
        pipeline,
        total,
        total as f64 / elapsed,
        p(0.50),
        p(0.95),
        p(0.99),
    );
}
