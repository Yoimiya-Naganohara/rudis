// Networking module for Rudis
// Handles TCP connections and protocol parsing

use std::net::TcpListener;

pub struct Networking {
    listener: TcpListener,
}

impl Networking {
    pub fn new(addr: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        Ok(Networking { listener })
    }

    pub fn listen(&self) {
        // TODO: Implement connection handling
        println!("Listening for connections...");
    }
}
