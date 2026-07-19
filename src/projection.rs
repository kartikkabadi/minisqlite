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
}

impl Default for ProjectionState {
    fn default() -> Self {
        Self::new()
    }
}
