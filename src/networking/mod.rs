// Networking module for Rudis
// Handles TCP connections and protocol parsing
pub mod resp;
use crate::commands::{command_helper::format_error, Command};
use crate::database::SharedDatabase;
use bytes::Buf;
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
        use tokio::io::AsyncReadExt;

        // Flush buffered replies once they exceed this size to bound memory
        // usage on very large pipelined batches.
        const RESPONSE_FLUSH_THRESHOLD: usize = 16 * 1024;

        let (mut reader, mut writer) = stream.split();
        let mut buffer = BytesMut::with_capacity(4096);
        let mut responses = BytesMut::with_capacity(4096);

        loop {
            // // Read more data into buffer
            // buffer.reserve(64 * 1024);
            let n = reader.read_buf(&mut buffer).await?;
            if n == 0 {
                // Connection closed
                break;
            }

            // Detach the received bytes as an immutable view (O(1)) and decode
            // every complete frame, advancing through the buffer without copying
            // it per frame. An incomplete trailing frame is moved back into
            // `buffer` for the next read.
            let mut data = buffer.freeze();
            loop {
                match redis_protocol::resp2::decode::decode(&data) {
                    Ok(Some((frame, consumed))) => {
                        data.advance(consumed);

                        let response = match Command::parse(&frame) {
                            Some(cmd) => {
                                if cmd == Command::Quit {
                                    writer.write_all(&responses).await?;
                                    return Ok(());
                                }
                                cmd.execute(&db).await
                            }
                            None => format_error(crate::commands::CommandError::UnknownCommand),
                        };
                        responses.extend_from_slice(&response);
                        if responses.len() >= RESPONSE_FLUSH_THRESHOLD {
                            writer.write_all(&responses).await?;
                            responses.clear();
                        }
                    }
                    Ok(None) => {
                        // Incomplete frame, keep the leftover for the next read
                        buffer = match data.try_into_mut() {
                            Ok(mut_buf) => mut_buf,
                            Err(shared) => BytesMut::from(&shared[..]),
                        };
                        break;
                    }
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
