// Config module for Rudis
// Configuration management

pub struct Config {
    pub port: u16,
    pub host: String,
    pub max_connections: usize,
    pub db_num: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 6379,
            host: "127.0.0.1".to_string(),
            max_connections: 1000,
            db_num: 16,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn load() -> Self {
        // TODO: Implement config file loading with fallback to defaults
        Self::new()
    }

    pub fn load_from_file(&mut self, _path: &str) {
        // TODO: Implement config file loading
    }
}
