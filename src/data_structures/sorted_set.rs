// Sorted Set data structure for Rudis

use std::collections::BTreeMap;

pub struct RedisSortedSet {
    members: BTreeMap<String, f64>,
}

impl RedisSortedSet {
    pub fn new() -> Self {
        RedisSortedSet { members: BTreeMap::new() }
    }

    pub fn zadd(&mut self, member: String, score: f64) {
        self.members.insert(member, score);
    }

    pub fn zscore(&self, member: &str) -> Option<&f64> {
        self.members.get(member)
    }

    pub fn zrem(&mut self, member: &str) -> bool {
        self.members.remove(member).is_some()
    }
}
