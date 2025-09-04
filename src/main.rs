mod server;
mod commands;
mod database;
mod persistence;
mod networking;
mod data_structures;
mod config;


use std::io;

use crate::server::Server;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    println!("Rudis - A Redis-like server in Rust");
    // TODO: Initialize and start the server
    let server =Server::new().await;
    server.run().await;
    Ok(())
}
