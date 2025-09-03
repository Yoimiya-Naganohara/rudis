// List data structure for Rudis

pub struct RedisList {
    items: Vec<String>,
}

impl RedisList {
    pub fn new() -> Self {
        RedisList { items: Vec::new() }
    }

    pub fn push(&mut self, item: String) {
        self.items.push(item);
    }

    pub fn pop(&mut self) -> Option<String> {
        self.items.pop()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
}
