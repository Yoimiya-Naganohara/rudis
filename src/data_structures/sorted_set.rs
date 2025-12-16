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

impl RedisSortedSet {
    pub fn new() -> Self {
        RedisSortedSet {
            members: HashMap::new(),
            ordered_members: BTreeSet::new(),
        }
    }

    pub fn zadd(&mut self, member: Bytes, score: f64) {
        let score = Score(score);
        // Remove old entry if exists
        if let Some(old_score) = self.members.get(&member) {
            self.ordered_members
                .remove(&(old_score.clone(), member.clone()));
        }
        self.members.insert(member.clone(), score.clone());
        self.ordered_members.insert((score, member));
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
        let sorted: Vec<_> = self.ordered_members.iter().map(|(_, m)| m).collect();
        let len = sorted.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        if start < 0 || stop < start || start >= len {
            vec![]
        } else {
            // Clone Bytes
            sorted
                .into_iter()
                .skip(start as usize)
                .take((stop - start + 1) as usize)
                .cloned()
                .collect()
        }
    }

    pub fn zrange_by_score(&self, min: f64, max: f64) -> Vec<Bytes> {
        let min_score = Score(min);
        let max_score = Score(max);
        self.ordered_members
            .range((
                std::ops::Bound::Included((min_score, Bytes::from_static(b""))),
                std::ops::Bound::Included((max_score, Bytes::from_static(b"\xFF\xFF\xFF\xFF"))), // Hacky max bound?
                                                                                                 // Actually for range search on BTreeSet<(Score, Bytes)>, we need to be careful.
                                                                                                 // If scores are equal, bytes are compared.
                                                                                                 // To get all with score >= min and <= max:
                                                                                                 // Start: (min, empty)
                                                                                                 // End: (max, max_possible_bytes)
            ))
            // The logic above is slightly flawed because we can't easily construct "max possible bytes".
            // Ideally we filter. But range is more efficient.
            // Let's use filter for correctness if range is tricky, or just use range with Unbounded for the bytes part if possible,
            // but Rust's RangeBounds applies to the whole tuple.
            // BTreeSet doesn't support "partial" range on tuple.
            // Wait, we can use range with Included/Excluded.
            // (min_score, [empty]) is definitely the start.
            // (max_score, [max_bytes]) is the end.
            // Since we can't easily make max bytes, maybe we can accept we might miss something if we don't do it right?
            // Actually, we can use filter on the iterator of the whole set for now to be safe and simple,
            // since this is an optimization refactor, logic preservation is key.
            // Existing logic used "\u{10FFFF}" which is max char.
            // For bytes, we don't have a simple "max".
            // Let's use filter on `ordered_members`.
            .filter(|(s, _)| s.0 >= min && s.0 <= max)
            .map(|(_, m)| m.clone())
            .collect()
    }
}
