// Server module for Rudis
// Handles the main server loop and client connections

use tokio::net::TcpListener;

use crate::{config::Config, database::{Database, SharedDatabase}, networking::Networking};

pub struct Server {
    networking: Networking,
    config: Config, // TODO: Add server fields
}

impl Server {
    pub async fn new() -> Self {
        let config = Config::new();
        let networking = Networking::new(&format!("{}:{}", &config.host, &config.port))
            .await.expect("Failed to create networking");
        Server { networking, config }
    }

    pub async fn run(&self) {
        let db=Database::new_shared();
        
        
        loop {
            self.networking.listen(&db).await;
        }
    }
}
