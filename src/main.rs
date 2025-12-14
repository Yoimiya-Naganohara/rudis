use std::process;
use tracing::{error, info};

mod commands;
mod config;
mod data_structures;
mod database;
mod error;
mod networking;
mod persistence;
mod server;

use crate::{config::Config, error::Result, server::Server};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting Rudis - A Redis-like server in Rust");

    // Load configuration
    let config = Config::default();

    // Initialize and start the server
    let server = Server::new(config).await?;
    info!(
        "Server listening on {}:{}",
        server.config().host,
        server.config().port
    );

    if let Err(e) = server.run().await {
        error!("Server error: {}", e);
        process::exit(1);
    }

    Ok(())
}
