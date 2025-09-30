// Sorted Set data structure for Rudis

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq)]
pub struct Score(f64);

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Score {}

#[derive(Debug)]
pub struct RedisSortedSet {
    members: HashMap<String, Score>,
    ordered_members: BTreeSet<(Score, String)>,
}

impl RedisSortedSet {
    pub fn new() -> Self {
        RedisSortedSet {
            members: HashMap::new(),
            ordered_members: BTreeSet::new(),
        }
    }

    pub fn zadd(&mut self, member: String, score: f64) {
        let score = Score(score);
        // Remove old entry if exists
        if let Some(old_score) = self.members.get(&member) {
            self.ordered_members
                .remove(&(old_score.clone(), member.clone()));
        }
        self.members.insert(member.clone(), score.clone());
        self.ordered_members.insert((score, member));
    }

    pub fn zscore(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn zrem(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.ordered_members.remove(&(score, member.to_string()));
            true
        } else {
            false
        }
    }

    pub fn zcard(&self) -> usize {
        self.members.len()
    }

    pub fn zrank(&self, member: &str) -> Option<usize> {
        self.ordered_members.iter().position(|(_, m)| m == member)
    }

    pub fn zrange(&self, start: i64, stop: i64) -> Vec<&String> {
        let sorted: Vec<_> = self.ordered_members.iter().map(|(_, m)| m).collect();
        let len = sorted.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start < 0 || stop < start || start >= len {
            vec![]
        } else {
            sorted
                .into_iter()
                .skip(start as usize)
                .take((stop - start + 1) as usize)
                .collect()
        }
    }

    pub fn zrange_by_score(&self, min: f64, max: f64) -> Vec<&String> {
        let min_score = Score(min);
        let max_score = Score(max);
        self.ordered_members
            .range((
                std::ops::Bound::Included((min_score, "".to_string())),
                std::ops::Bound::Included((max_score, "\u{10FFFF}".to_string())),
            ))
            .map(|(_, m)| m)
            .collect()
    }
}
