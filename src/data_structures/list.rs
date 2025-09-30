// List data structure for Rudis

use std::collections::VecDeque;

#[derive(Debug)]
pub struct RedisList {
    items: VecDeque<String>,
}

impl RedisList {
    pub fn new() -> Self {
        RedisList {
            items: VecDeque::new(),
        }
    }

    pub fn rpush(&mut self, item: String) {
        self.items.push_back(item);
    }
    pub fn lpush(&mut self, item: String) {
        self.items.push_front(item);
    }
    pub fn push(&mut self, item: String) {
        self.rpush(item);
    }
    pub fn lpop(&mut self) -> Option<String> {
        self.items.pop_front()
    }
    pub fn rpop(&mut self) -> Option<String> {
        self.items.pop_back()
    }
    pub fn pop(&mut self) -> Option<String> {
        self.rpop()
    }
    pub fn index(&self, index: i64) -> Option<&String> {
        let len = self.items.len() as i64;
        let actual_index = if index < 0 { len + index } else { index };
        if actual_index >= 0 && actual_index < len {
            self.items.get(actual_index as usize)
        } else {
            None
        }
    }
    pub fn range(&self, start: i64, end: i64) -> Vec<&String> {
        let len = self.items.len() as i64;
        let actual_start = if start < 0 { len + start } else { start };
        let actual_end = if end < 0 { len + end } else { end };
        let start_idx = actual_start.max(0) as usize;
        let end_idx = (actual_end + 1).min(len) as usize;
        if start_idx >= end_idx {
            vec![]
        } else {
            self.items.range(start_idx..end_idx).collect()
        }
    }
    pub fn trim(&mut self, start: i64, end: i64) {
        let len = self.items.len() as i64;
        let actual_start = if start < 0 { len + start } else { start };
        let actual_end = if end < 0 { len + end } else { end };
        if actual_start > actual_end {
            self.items.clear();
            return;
        }
        let start_idx = actual_start.max(0) as usize;
        let end_idx = (actual_end + 1).min(len) as usize;
        if start_idx >= end_idx {
            self.items.clear();
        } else {
            // Keep only start_idx..end_idx
            let new_items = self
                .items
                .range(start_idx..end_idx)
                .cloned()
                .collect::<VecDeque<_>>();
            self.items = new_items;
        }
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn set(&mut self, index: i64, value: String) {
        let len = self.items.len() as i64;
        let actual_index = if index < 0 { len + index } else { index };
        if let Some(val) = self.items.get_mut(actual_index as usize) {
            *val = value;
        }
    }

    pub(crate) fn insert(
        &mut self,
        ord: &str,
        pivot: &str,
        value: String,
    ) -> Result<i64, crate::commands::CommandError> {
        if let Some(idx) = self.items.iter().position(|x| x == pivot) {
            match ord {
                "BEFORE" => {
                    self.items.insert(idx, value);
                    Ok(idx as i64)
                }
                "AFTER" => {
                    self.items.insert(idx + 1, value);
                    Ok((idx + 1) as i64)
                }
                _ => Err(crate::commands::CommandError::InvalidInsertDirection),
            }
        } else {
            Err(crate::commands::CommandError::PivotNotFound)
        }
    }
}
