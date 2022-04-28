use std::collections::HashMap;

pub struct KvStore {
    mem_store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore { mem_store: HashMap::new() }
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.mem_store.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        let _ = self.mem_store.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        let _ = self.mem_store.remove(&key);
    }
}