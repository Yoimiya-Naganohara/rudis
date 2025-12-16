// Set data structure for Rudis

use bytes::Bytes;
use std::collections::HashSet;
#[derive(Debug)]
pub struct RedisSet {
    members: HashSet<Bytes>,
}

impl RedisSet {
    pub fn new() -> Self {
        RedisSet {
            members: HashSet::new(),
        }
    }

    pub fn sadd(&mut self, member: Bytes) -> bool {
        self.members.insert(member)
    }

    pub fn srem(&mut self, member: &Bytes) -> bool {
        self.members.remove(member)
    }

    pub fn sismember(&self, member: &Bytes) -> bool {
        self.members.contains(member)
    }
    pub fn smembers(&self) -> Vec<&Bytes> {
        self.members.iter().collect()
    }

    pub(crate) fn scard(&self) -> usize {
        self.members.len()
    }
}
