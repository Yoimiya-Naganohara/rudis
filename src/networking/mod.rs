// Networking module for Rudis
// Handles TCP connections and protocol parsing
pub mod resp;
use crate::commands::{command_helper::format_error, Command};
use crate::database::SharedDatabase;
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

        let (mut reader, mut writer) = stream.split();
        let mut buffer = BytesMut::with_capacity(4096);

        loop {
            // Try to decode frames from the buffer
            // We use a loop here to handle multiple pipelined commands in one buffer
            loop {
                use bytes::Bytes;
                // Peek at the buffer to decode
                let peek_bytes = Bytes::copy_from_slice(&buffer);
                match redis_protocol::resp2::decode::decode(&peek_bytes) {
                    Ok(Some((frame, consumed))) => {
                        // We have a complete frame

                        // Advance the buffer by the number of bytes consumed
                        let _ = buffer.split_to(consumed);

                        let response = match Command::parse(&frame) {
                            Some(cmd) => {
                                if cmd == Command::Quit {
                                    return Ok(());
                                }
                                cmd.execute(&db).await
                            }
                            None => format_error(crate::commands::CommandError::UnknownCommand),
                        };
                        writer.write_all(&response).await?;
                    }
                    Ok(None) => {
                        // Incomplete frame, break inner loop to read more data
                        break;
                    }
                    Err(_e) => {
                        // Protocol error
                        // eprintln!("Protocol error: {:?}", e);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "Protocol Error"));
                    }
                }
            }

            // Read more data into buffer
            let n = reader.read_buf(&mut buffer).await?;
            if n == 0 {
                // Connection closed
                break;
            }
        }
        Ok(())
    }
}
