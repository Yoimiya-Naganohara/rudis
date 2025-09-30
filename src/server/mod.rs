// Server module for Rudis
// Handles the main server loop and client connections

use crate::{
    config::Config,
    database::{Database, SharedDatabase},
    error::{Error, Result},
    networking::Networking,
};

pub struct Server {
    networking: Networking,
    config: Config,
    database: SharedDatabase,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self> {
        let networking = Networking::new(&format!("{}:{}", &config.host, &config.port))
            .await
            .map_err(Error::Io)?;

        let database = Database::new_shared(config.db_num);

        Ok(Server {
            networking,
            config,
            database,
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            self.networking.listen(&self.database).await?;
        }
    }
}
