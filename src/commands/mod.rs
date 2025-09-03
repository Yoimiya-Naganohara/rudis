// Commands module for Rudis
// Handles parsing and executing Redis commands

pub enum Command {
    // TODO: Add command variants
    Ping,
    Get(String),
    Set(String, String),
}

impl Command {
    pub fn parse(_input: &str) -> Option<Self> {
        // TODO: Implement command parsing
        None
    }

    pub fn execute(&self) {
        // TODO: Implement command execution
    }
}
