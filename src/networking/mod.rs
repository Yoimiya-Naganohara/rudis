// Networking module for Rudis
// Handles TCP connections and protocol parsing
pub mod resp;
use crate::commands::{Command, command_helper::format_error};
use crate::{database::SharedDatabase, networking::resp::RespParser};
use std::{io, net::SocketAddr};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

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
        println!("Listening for connections...");

        loop {
            let (stream, _addr) = self.listener.accept().await?;
            let db_ref = db.clone();
            tokio::spawn(async move { Self::handle(stream, _addr, &db_ref).await });
        }
    }
    pub async fn handle(
        mut stream: TcpStream,
        addr: SocketAddr,
        db: &SharedDatabase,
    ) -> tokio::io::Result<()> {
        let (reader, mut writer) = stream.split();
        let mut buf_reader = BufReader::new(reader);
        let mut parser = RespParser::new();
        loop {
            match parser.read_value(&mut buf_reader).await {
                Ok(resp_value) => {
                    dbg!(&resp_value);
                    let response = match Command::parse(&resp_value) {
                        Some(cmd) => {
                            println!("Parsed command: {:?}", cmd);
                            cmd.execute(&db).await
                        }
                        
                        None => format_error(crate::commands::CommandError::UnknownCommand),
                    };
                    writer.write_all(response.as_bytes()).await?;
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    eprintln!("Read error: {e}")
                }
            }
            // line.clear();
            // match parser.read_value(&mut line).await {
            //     Ok(0) => break,
            //     Ok(_) => {
            //         let command = line.trim_end();
            //         println!("Received: {:?}", command);
            //         if let Ok(cmd) = parser.read_value(command).await {

            //         }
            //     }
            //     Err(e) => eprintln!("Read error: {e}"),
            // }
        }
        Ok(())
    }
}
