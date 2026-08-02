// Sorted Set data structure for Rudis

use bytes::Bytes;
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
    members: HashMap<Bytes, Score>,
    ordered_members: BTreeSet<(Score, Bytes)>,
}

impl Default for RedisSortedSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisSortedSet {
    pub fn new() -> Self {
        RedisSortedSet {
            members: HashMap::new(),
            ordered_members: BTreeSet::new(),
        }
    }

    /// Adds a member with the given score. Returns 1 if the member was newly
    /// added, 0 if the score of an existing member was updated (Redis ZADD
    /// semantics: only new members count).
    pub fn zadd(&mut self, member: Bytes, score: f64) -> usize {
        let score = Score(score);
        let is_new = !self.members.contains_key(&member);
        // Remove old entry if exists
        if let Some(old_score) = self.members.get(&member) {
            self.ordered_members
                .remove(&(old_score.clone(), member.clone()));
        }
        self.members.insert(member.clone(), score.clone());
        self.ordered_members.insert((score, member));
        is_new as usize
    }

    pub fn zscore(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn zrem(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.ordered_members.remove(&(score, member.clone()));
            true
        } else {
            false
        }
    }

    pub fn zcard(&self) -> usize {
        self.members.len()
    }

    pub fn zrank(&self, member: &Bytes) -> Option<usize> {
        self.ordered_members.iter().position(|(_, m)| m == member)
    }

    pub fn zrange(&self, start: i64, stop: i64) -> Vec<Bytes> {
        let len = self.ordered_members.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start < 0 || stop < start || start >= len {
            vec![]
        } else {
            // Iterate directly instead of collecting the whole set first
            self.ordered_members
                .iter()
                .skip(start as usize)
                .take((stop - start + 1) as usize)
                .map(|(_, m)| m.clone())
                .collect()
        }
    }

    pub fn zrange_by_score(&self, min: f64, max: f64) -> Vec<Bytes> {
        let min_score = Score(min);
        let max_score = Score(max);
        // Start the range at (min_score, empty bytes) so members sharing the
        // min score are included regardless of their bytes. The upper bound is
        // unbounded and `take_while` stops at the first score above max: the
        // set is sorted by (score, member), so this is both correct and
        // bounded by the matching prefix instead of the whole set.
        self.ordered_members
            .range((
                std::ops::Bound::Included((min_score, Bytes::new())),
                std::ops::Bound::Unbounded,
            ))
            .take_while(|(s, _)| *s <= max_score)
            .map(|(_, m)| m.clone())
            .collect()
    }
}
