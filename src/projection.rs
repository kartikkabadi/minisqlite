use std::collections::BTreeSet;

use crate::error::ValidationError;

/// A single entry in a projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionEntry {
    /// The entry's key, unique within the projection.
    pub key: Vec<u8>,
    /// The opaque value stored under the key.
    pub value: Vec<u8>,
}

impl ProjectionEntry {
    /// Create an entry from a key and value.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// A single mutation within a [`ProjectionPatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionMutation {
    /// Insert or overwrite a key.
    Put {
        /// The key to insert or overwrite.
        key: Vec<u8>,
        /// The value to store under the key.
        value: Vec<u8>,
    },
    /// Delete a key (deleting an absent key is a no-op).
    Delete {
        /// The key to delete.
        key: Vec<u8>,
    },
    /// Remove all entries from the projection.
    Clear,
    /// Replace the entire projection contents with the given entries.
    Replace {
        /// The entries that become the projection's full contents.
        entries: Vec<ProjectionEntry>,
    },
}

/// A versioned batch of mutations to one projection.
///
/// `new_version` must be exactly `expected_version + 1`, and contradictory duplicate
/// keys within one patch are rejected during validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionPatch {
    /// The projection being patched.
    pub projection: String,
    /// The durable version the projection must be at for the patch to apply.
    pub expected_version: u64,
    /// The version the projection moves to; must be `expected_version + 1`.
    pub new_version: u64,
    /// The mutations applied in order.
    pub mutations: Vec<ProjectionMutation>,
}

impl ProjectionPatch {
    /// Create a patch that advances `projection` from `expected_version` to
    /// `expected_version + 1`.
    pub fn new(projection: impl Into<String>, expected_version: u64) -> Self {
        Self {
            projection: projection.into(),
            expected_version,
            new_version: expected_version.saturating_add(1),
            mutations: Vec::new(),
        }
    }

    /// Add a put mutation.
    pub fn put(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.mutations.push(ProjectionMutation::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// Add a delete mutation.
    pub fn delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.mutations
            .push(ProjectionMutation::Delete { key: key.into() });
        self
    }

    /// Add a clear mutation.
    pub fn clear(mut self) -> Self {
        self.mutations.push(ProjectionMutation::Clear);
        self
    }

    /// Add a replace mutation.
    pub fn replace(mut self, entries: impl IntoIterator<Item = ProjectionEntry>) -> Self {
        self.mutations.push(ProjectionMutation::Replace {
            entries: entries.into_iter().collect(),
        });
        self
    }

    /// Validate the patch's static invariants: version increment and no contradictory
    /// duplicate keys after the last `Clear`/`Replace` boundary.
    pub(crate) fn validate(&self) -> Result<(), ValidationError> {
        if self.new_version != self.expected_version.wrapping_add(1) || self.new_version == 0 {
            return Err(ValidationError(format!(
                "projection {} patch new_version {} must be expected_version {} + 1",
                self.projection, self.new_version, self.expected_version
            )));
        }
        // Duplicate keys within one patch are contradictory: the caller's intent for
        // the key is ambiguous. Clear/Replace reset the tracked key set because they
        // define a fresh baseline for subsequent mutations.
        let mut seen: BTreeSet<&[u8]> = BTreeSet::new();
        for mutation in &self.mutations {
            match mutation {
                ProjectionMutation::Put { key, .. } | ProjectionMutation::Delete { key } => {
                    if !seen.insert(key.as_slice()) {
                        return Err(ValidationError(format!(
                            "projection {} patch has contradictory duplicate key",
                            self.projection
                        )));
                    }
                }
                ProjectionMutation::Clear => seen.clear(),
                ProjectionMutation::Replace { entries } => {
                    // The replaced keys become the new baseline: a later Put or
                    // Delete of one of them within the same patch is contradictory.
                    seen.clear();
                    for entry in entries {
                        if !seen.insert(entry.key.as_slice()) {
                            return Err(ValidationError(format!(
                                "projection {} replace has contradictory duplicate key",
                                self.projection
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_must_increment_by_one() {
        let mut patch = ProjectionPatch::new("p", 3);
        assert_eq!(patch.new_version, 4);
        assert!(patch.validate().is_ok());
        patch.new_version = 5;
        assert!(patch.validate().is_err());
    }

    #[test]
    fn contradictory_duplicate_keys_rejected() {
        let patch = ProjectionPatch::new("p", 0).put("k", "v1").delete("k");
        assert!(patch.validate().is_err());
    }

    #[test]
    fn clear_resets_duplicate_tracking() {
        let patch = ProjectionPatch::new("p", 0)
            .put("k", "v1")
            .clear()
            .put("k", "v2");
        assert!(patch.validate().is_ok());
    }

    #[test]
    fn put_or_delete_of_a_replaced_key_is_rejected() {
        let put_after = ProjectionPatch::new("p", 0)
            .replace(vec![ProjectionEntry::new("k", "v1")])
            .put("k", "v2");
        assert!(put_after.validate().is_err());
        let delete_after = ProjectionPatch::new("p", 0)
            .replace(vec![ProjectionEntry::new("k", "v1")])
            .delete("k");
        assert!(delete_after.validate().is_err());
    }

    #[test]
    fn replace_with_duplicate_keys_rejected() {
        let patch = ProjectionPatch::new("p", 0).replace(vec![
            ProjectionEntry::new("k", "v1"),
            ProjectionEntry::new("k", "v2"),
        ]);
        assert!(patch.validate().is_err());
    }
}
