mod server;
mod commands;
mod database;
mod persistence;
mod networking;
mod data_structures;
mod config;

use std::io;

fn main() -> io::Result<()> {
    println!("Rudis - A Redis-like server in Rust");
    // TODO: Initialize and start the server
    Ok(())
}
