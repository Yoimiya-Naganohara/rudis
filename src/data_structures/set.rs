// Set data structure for Rudis

use std::collections::HashSet;

pub struct RedisSet {
    members: HashSet<String>,
}

impl RedisSet {
    pub fn new() -> Self {
        RedisSet { members: HashSet::new() }
    }

    pub fn sadd(&mut self, member: String) -> bool {
        self.members.insert(member)
    }

    pub fn srem(&mut self, member: &str) -> bool {
        self.members.remove(member)
    }

    pub fn sismember(&self, member: &str) -> bool {
        self.members.contains(member)
    }
}
