// Persistence module for Rudis
// Handles AOF and RDB persistence
//
// Not wired into the server yet (in-memory only); kept as scaffolding for
// future AOF/RDB support.
#[allow(dead_code)]
pub struct Persistence {
    // TODO: Add persistence fields
}

impl Default for Persistence {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl Persistence {
    pub fn new() -> Self {
        Persistence {}
    }

    pub fn save(&self) {
        // TODO: Implement save to disk
        println!("Saving data...");
    }

    pub fn load(&self) {
        // TODO: Implement load from disk
        println!("Loading data...");
    }
}
