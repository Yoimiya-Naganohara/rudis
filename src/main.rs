use std::process;
use tracing::{error, info};

mod server;
mod commands;
mod database;
mod persistence;
mod networking;
mod data_structures;
mod config;
mod error;

use crate::{
    config::Config,
    error::Result,
    server::Server,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("🚀 Starting Rudis - A Redis-like server in Rust");

    // Load configuration
    let config = Config::default();

    // Initialize and start the server
    let server = Server::new(config).await?;
    info!("📡 Server listening on {}:{}", server.config().host, server.config().port);

    if let Err(e) = server.run().await {
        error!("❌ Server error: {}", e);
        process::exit(1);
    }

    Ok(())
}
