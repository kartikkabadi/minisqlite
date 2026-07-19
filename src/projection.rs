use std::collections::BTreeMap;

/// A single entry in a projection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ProjectionEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl ProjectionEntry {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// In-memory state for one projection collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionState {
    pub version: u64,
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl ProjectionState {
    pub fn new() -> Self {
        Self {
            version: 0,
            data: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.data.remove(key);
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn replace(&mut self, entries: &[ProjectionEntry]) {
        self.data.clear();
        for e in entries {
            self.data.insert(e.key.clone(), e.value.clone());
        }
    }

    pub fn put_changes(&self, key: &[u8], value: &[u8]) -> bool {
        self.data.get(key).map_or(true, |v| v.as_slice() != value)
    }

    pub fn delete_changes(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    pub fn clear_changes(&self) -> bool {
        !self.data.is_empty()
    }

    pub fn replace_changes(&self, entries: &[ProjectionEntry]) -> bool {
        if entries.len() != self.data.len() {
            return true;
        }
        entries.iter().any(|e| {
            self.data
                .get(&e.key)
                .map_or(true, |dv| dv.as_slice() != e.value.as_slice())
        })
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<ProjectionEntry> {
        if prefix.is_empty() {
            return self
                .data
                .iter()
                .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
                .collect();
        }
        match prefix_upper_bound(prefix) {
            Some(upper) => self
                .data
                .range(prefix.to_vec()..upper)
                .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
                .collect(),
            None => self
                .data
                .range(prefix.to_vec()..)
                .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
                .collect(),
        }
    }

    pub fn scan_range(&self, start: &[u8], end: &[u8]) -> Vec<ProjectionEntry> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| ProjectionEntry::new(k.clone(), v.clone()))
            .collect()
    }
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] == u8::MAX {
            upper.pop();
        } else {
            upper[i] += 1;
            return Some(upper);
        }
    }
    None
}

impl Default for ProjectionState {
    fn default() -> Self {
        Self::new()
    }
}
