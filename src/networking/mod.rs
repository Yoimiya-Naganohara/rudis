// Networking module for Rudis
// Handles TCP connections and protocol parsing
pub mod resp;
use crate::commands::{command_helper::format_error, Command};
use crate::database::SharedDatabase;
use bytes::BytesMut;
use std::{io, net::SocketAddr};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::info;

pub struct Networking {
    listener: TcpListener,
}

impl Networking {
    pub async fn new(addr: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Networking { listener })
    }

    pub async fn listen(&self, db: &SharedDatabase) -> tokio::io::Result<()> {
        // TODO: Implement connection handling
        info!("Listening for connections...");

        loop {
            let (stream, _addr) = self.listener.accept().await?;
            let db_ref = db.clone();
            tokio::spawn(async move { Self::handle(stream, _addr, &db_ref).await });
        }
    }
    pub async fn handle(
        mut stream: TcpStream,
        _addr: SocketAddr,
        db: &SharedDatabase,
    ) -> tokio::io::Result<()> {
        use bytes::BytesMut;
        use redis_protocol::resp2::decode::decode_mut;
        use tokio::io::AsyncReadExt;

        // Flush buffered replies once they exceed this size to bound memory
        // usage on very large pipelined batches.
        const RESPONSE_FLUSH_THRESHOLD: usize = 16 * 1024;

        let (mut reader, mut writer) = stream.split();
        let mut buffer = BytesMut::with_capacity(4096);
        let mut responses = BytesMut::with_capacity(4096);

        loop {
            // Read more data into buffer
            buffer.reserve(64 * 1024);
            let n = reader.read_buf(&mut buffer).await?;
            if n == 0 {
                // Connection closed
                break;
            }

            // Zero-copy decode: `decode_mut` parses one frame at a time and
            // splits the consumed bytes off `buffer` (O(1)); bulk strings are
            // returned as refcounted slices of the same allocation, so no
            // copying per frame. On an incomplete frame the buffer is left
            // untouched and the loop exits to wait for more data.
            loop {
                match decode_mut(&mut buffer) {
                    Ok(Some((frame, _consumed, _frozen))) => {
                        match Command::parse(&frame) {
                            Some(cmd) => {
                                if cmd == Command::Quit {
                                    writer.write_all(&responses).await?;
                                    return Ok(());
                                }
                                cmd.execute(&db, &mut responses).await;
                            }
                            None => format_error(
                                &mut responses,
                                crate::commands::CommandError::UnknownCommand,
                            ),
                        }
                        if responses.len() >= RESPONSE_FLUSH_THRESHOLD {
                            writer.write_all(&responses).await?;
                            responses.clear();
                        }
                    }
                    Ok(None) => break, // Incomplete frame, keep the leftover for the next read
                    Err(_e) => {
                        // Protocol error
                        // eprintln!("Protocol error: {:?}", e);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "Protocol Error"));
                    }
                }
            }

            // Flush any replies accumulated for this batch of commands
            if !responses.is_empty() {
                writer.write_all(&responses).await?;
                responses.clear();
            }
        }
        Ok(())
    }
}
