//! Thread-safe typed in-memory storage with expiration and atomic snapshots.
#![allow(clippy::missing_errors_doc)]

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const SNAPSHOT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SortedSetEntry {
    member: Vec<u8>,
    score: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum StoredValue {
    String(Vec<u8>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    List(Vec<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    SortedSet(Vec<SortedSetEntry>),
}

impl StoredValue {
    fn estimated_size(&self) -> usize {
        match self {
            Self::String(value) => value.len(),
            Self::Hash(fields) => fields
                .iter()
                .map(|(field, value)| field.len().saturating_add(value.len()))
                .sum(),
            Self::List(values) => values.iter().map(Vec::len).sum(),
            Self::Set(values) => values.iter().map(Vec::len).sum(),
            Self::SortedSet(values) => values
                .iter()
                .map(|entry| entry.member.len().saturating_add(size_of::<f64>()))
                .sum(),
        }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    value: StoredValue,
    expires_at: Option<Instant>,
    expires_at_unix_ms: Option<u64>,
}

impl Entry {
    fn persistent(value: StoredValue) -> Self {
        Self {
            value,
            expires_at: None,
            expires_at_unix_ms: None,
        }
    }

    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|deadline| deadline <= now)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    entries: Vec<PersistedEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedEntry {
    key: Vec<u8>,
    value: StoredValue,
    expires_at_unix_ms: Option<u64>,
}

/// Storage operation failure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    /// The key contains a different value type than the command requires.
    #[error("operation against a key holding the wrong kind of value")]
    WrongType,
    /// Sorted-set scores must be finite numbers.
    #[error("sorted-set score must be finite")]
    InvalidScore,
}

/// Snapshot load or save failure.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Filesystem operation failed.
    #[error("snapshot I/O error: {0}")]
    Io(#[from] io::Error),
    /// Snapshot encoding or decoding failed.
    #[error("invalid snapshot encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// The snapshot format version is unsupported.
    #[error("unsupported snapshot version {0}")]
    UnsupportedVersion(u32),
}

/// Remaining lifetime of a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeToLive {
    /// The key is absent (or has expired).
    Missing,
    /// The key exists without an expiry.
    Persistent,
    /// The key exists and expires after this duration.
    ExpiresIn(Duration),
}

/// A cloneable handle to the shared in-memory store.
///
/// Operations hold a standard-library write lock only for bounded map work.
/// The lock is never held across an `.await`.
#[derive(Clone, Debug, Default)]
pub struct Engine {
    entries: Arc<RwLock<HashMap<Vec<u8>, Entry>>>,
}

impl Engine {
    /// Create an empty engine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Load an engine from a versioned snapshot.
    ///
    /// Expired entries are discarded while loading and live expirations are
    /// reconstructed against the monotonic clock.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable, malformed, or unsupported snapshots.
    pub fn load_snapshot(path: &Path) -> Result<Self, SnapshotError> {
        let bytes = fs::read(path)?;
        let snapshot: Snapshot = serde_json::from_slice(&bytes)?;
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(snapshot.version));
        }
        let now_unix_ms = unix_time_ms();
        let now = Instant::now();
        let mut entries = HashMap::with_capacity(snapshot.entries.len());
        for persisted in snapshot.entries {
            let expires_at = match persisted.expires_at_unix_ms {
                Some(deadline) if deadline <= now_unix_ms => continue,
                Some(deadline) => now.checked_add(Duration::from_millis(deadline - now_unix_ms)),
                None => None,
            };
            entries.insert(
                persisted.key,
                Entry {
                    value: persisted.value,
                    expires_at,
                    expires_at_unix_ms: persisted.expires_at_unix_ms,
                },
            );
        }
        Ok(Self {
            entries: Arc::new(RwLock::new(entries)),
        })
    }

    /// Write a versioned snapshot using a same-directory temporary file,
    /// `fsync`, and atomic rename.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or any filesystem operation fails.
    pub fn save_snapshot(&self, path: &Path) -> Result<(), SnapshotError> {
        let snapshot = self.snapshot();
        let bytes = serde_json::to_vec(&snapshot)?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let temporary = temporary_snapshot_path(path);
        let result = (|| -> Result<(), SnapshotError> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    /// Set a string value, replacing any existing value and expiry.
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let now = Instant::now();
        let expires_at = ttl.and_then(|duration| now.checked_add(duration));
        let expires_at_unix_ms = ttl.and_then(|duration| {
            u64::try_from(duration.as_millis())
                .ok()
                .and_then(|millis| unix_time_ms().checked_add(millis))
        });
        self.write_entries().insert(
            key,
            Entry {
                value: StoredValue::String(value),
                expires_at,
                expires_at_unix_ms,
            },
        );
    }

    /// Get a string value. Expired entries are removed before returning.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::WrongType`] for non-string values.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(None),
            Some(StoredValue::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Delete keys and return how many distinct live keys existed.
    pub fn delete<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>) -> usize {
        let now = Instant::now();
        let mut entries = self.write_entries();
        keys.into_iter()
            .filter(|key| {
                if entries.get(*key).is_some_and(|entry| entry.is_expired(now)) {
                    entries.remove(*key);
                    false
                } else {
                    entries.remove(*key).is_some()
                }
            })
            .count()
    }

    /// Count live keys. Repeated keys are counted repeatedly.
    pub fn exists<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>) -> usize {
        let now = Instant::now();
        let mut entries = self.write_entries();
        let mut count = 0;
        for key in keys {
            if entries.get(key).is_some_and(|entry| entry.is_expired(now)) {
                entries.remove(key);
            } else if entries.contains_key(key) {
                count += 1;
            }
        }
        count
    }

    /// Set hash fields and return the number of newly added fields.
    pub fn hash_set(
        &self,
        key: &[u8],
        fields: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<usize, EngineError> {
        let mut entries = self.live_entries(key);
        let entry = entries
            .entry(key.to_vec())
            .or_insert_with(|| Entry::persistent(StoredValue::Hash(Vec::new())));
        let StoredValue::Hash(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        let mut added = 0;
        for (field, value) in fields {
            if let Some((_, stored)) = existing.iter_mut().find(|(name, _)| name == field) {
                stored.clone_from(value);
            } else {
                existing.push((field.clone(), value.clone()));
                added += 1;
            }
        }
        Ok(added)
    }

    /// Get a hash field.
    pub fn hash_get(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(None),
            Some(StoredValue::Hash(fields)) => Ok(fields
                .iter()
                .find(|(name, _)| name == field)
                .map(|(_, value)| value.clone())),
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Delete hash fields and return the number removed.
    pub fn hash_delete<'a>(
        &self,
        key: &[u8],
        fields: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<usize, EngineError> {
        let mut entries = self.live_entries(key);
        let Some(entry) = entries.get_mut(key) else {
            return Ok(0);
        };
        let StoredValue::Hash(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        let before = existing.len();
        let targets: HashSet<&[u8]> = fields.into_iter().collect();
        existing.retain(|(field, _)| !targets.contains(field.as_slice()));
        let removed = before - existing.len();
        if existing.is_empty() {
            entries.remove(key);
        }
        Ok(removed)
    }

    /// Return the number of fields in a hash.
    pub fn hash_len(&self, key: &[u8]) -> Result<usize, EngineError> {
        self.collection_len(key, |value| match value {
            StoredValue::Hash(values) => Some(values.len()),
            _ => None,
        })
    }

    /// Push values to a list and return its new length.
    pub fn list_push(
        &self,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
    ) -> Result<usize, EngineError> {
        let mut entries = self.live_entries(key);
        let entry = entries
            .entry(key.to_vec())
            .or_insert_with(|| Entry::persistent(StoredValue::List(Vec::new())));
        let StoredValue::List(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        if left {
            for value in values {
                existing.insert(0, value.clone());
            }
        } else {
            existing.extend(values.iter().cloned());
        }
        Ok(existing.len())
    }

    /// Pop one value from a list.
    pub fn list_pop(&self, key: &[u8], left: bool) -> Result<Option<Vec<u8>>, EngineError> {
        let mut entries = self.live_entries(key);
        let Some(entry) = entries.get_mut(key) else {
            return Ok(None);
        };
        let StoredValue::List(values) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        let value = if values.is_empty() {
            None
        } else if left {
            Some(values.remove(0))
        } else {
            values.pop()
        };
        if values.is_empty() {
            entries.remove(key);
        }
        Ok(value)
    }

    /// Return the number of values in a list.
    pub fn list_len(&self, key: &[u8]) -> Result<usize, EngineError> {
        self.collection_len(key, |value| match value {
            StoredValue::List(values) => Some(values.len()),
            _ => None,
        })
    }

    /// Return an inclusive list range using Redis-style negative indexes.
    pub fn list_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(Vec::new()),
            Some(StoredValue::List(values)) => Ok(range_bounds(values.len(), start, stop)
                .map_or_else(Vec::new, |(first, last)| values[first..=last].to_vec())),
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Add set members and return the number newly added.
    pub fn set_add(&self, key: &[u8], members: &[Vec<u8>]) -> Result<usize, EngineError> {
        let mut entries = self.live_entries(key);
        let entry = entries
            .entry(key.to_vec())
            .or_insert_with(|| Entry::persistent(StoredValue::Set(HashSet::new())));
        let StoredValue::Set(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        Ok(members
            .iter()
            .filter(|member| existing.insert((*member).clone()))
            .count())
    }

    /// Remove set members and return the number removed.
    pub fn set_remove<'a>(
        &self,
        key: &[u8],
        members: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<usize, EngineError> {
        let mut entries = self.live_entries(key);
        let Some(entry) = entries.get_mut(key) else {
            return Ok(0);
        };
        let StoredValue::Set(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        let removed = members
            .into_iter()
            .filter(|member| existing.remove(*member))
            .count();
        if existing.is_empty() {
            entries.remove(key);
        }
        Ok(removed)
    }

    /// Test set membership.
    pub fn set_contains(&self, key: &[u8], member: &[u8]) -> Result<bool, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(false),
            Some(StoredValue::Set(values)) => Ok(values.contains(member)),
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Return sorted set members for deterministic wire output.
    pub fn set_members(&self, key: &[u8]) -> Result<Vec<Vec<u8>>, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(Vec::new()),
            Some(StoredValue::Set(values)) => {
                let mut members: Vec<Vec<u8>> = values.iter().cloned().collect();
                members.sort();
                Ok(members)
            }
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Return the number of members in a set.
    pub fn set_len(&self, key: &[u8]) -> Result<usize, EngineError> {
        self.collection_len(key, |value| match value {
            StoredValue::Set(values) => Some(values.len()),
            _ => None,
        })
    }

    /// Add or update sorted-set members and return the number newly added.
    pub fn sorted_set_add(
        &self,
        key: &[u8],
        values: &[(f64, Vec<u8>)],
    ) -> Result<usize, EngineError> {
        if values.iter().any(|(score, _)| !score.is_finite()) {
            return Err(EngineError::InvalidScore);
        }
        let mut entries = self.live_entries(key);
        let entry = entries
            .entry(key.to_vec())
            .or_insert_with(|| Entry::persistent(StoredValue::SortedSet(Vec::new())));
        let StoredValue::SortedSet(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        let mut added = 0;
        for (score, member) in values {
            if let Some(entry) = existing.iter_mut().find(|entry| entry.member == *member) {
                entry.score = *score;
            } else {
                existing.push(SortedSetEntry {
                    member: member.clone(),
                    score: *score,
                });
                added += 1;
            }
        }
        Ok(added)
    }

    /// Remove sorted-set members and return the number removed.
    pub fn sorted_set_remove<'a>(
        &self,
        key: &[u8],
        members: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<usize, EngineError> {
        let mut entries = self.live_entries(key);
        let Some(entry) = entries.get_mut(key) else {
            return Ok(0);
        };
        let StoredValue::SortedSet(existing) = &mut entry.value else {
            return Err(EngineError::WrongType);
        };
        let targets: HashSet<&[u8]> = members.into_iter().collect();
        let before = existing.len();
        existing.retain(|entry| !targets.contains(entry.member.as_slice()));
        let removed = before - existing.len();
        if existing.is_empty() {
            entries.remove(key);
        }
        Ok(removed)
    }

    /// Get a sorted-set member's score.
    pub fn sorted_set_score(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(None),
            Some(StoredValue::SortedSet(values)) => Ok(values
                .iter()
                .find(|entry| entry.member == member)
                .map(|entry| entry.score)),
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Return a sorted-set range ordered by score and member bytes.
    pub fn sorted_set_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<(Vec<u8>, f64)>, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key).map(|entry| &entry.value) {
            None => Ok(Vec::new()),
            Some(StoredValue::SortedSet(values)) => {
                let mut values = values.clone();
                values.sort_by(|left, right| {
                    left.score
                        .total_cmp(&right.score)
                        .then_with(|| left.member.cmp(&right.member))
                });
                Ok(range_bounds(values.len(), start, stop).map_or_else(
                    Vec::new,
                    |(first, last)| {
                        values[first..=last]
                            .iter()
                            .map(|entry| (entry.member.clone(), entry.score))
                            .collect()
                    },
                ))
            }
            Some(_) => Err(EngineError::WrongType),
        }
    }

    /// Return the number of members in a sorted set.
    pub fn sorted_set_len(&self, key: &[u8]) -> Result<usize, EngineError> {
        self.collection_len(key, |value| match value {
            StoredValue::SortedSet(values) => Some(values.len()),
            _ => None,
        })
    }

    /// Return an approximate byte count for a live key's key and payload data.
    #[must_use]
    pub fn memory_usage(&self, key: &[u8]) -> Option<usize> {
        let entries = self.live_entries(key);
        entries
            .get(key)
            .map(|entry| key.len().saturating_add(entry.value.estimated_size()))
    }

    /// Return a key's expiry state, lazily deleting it if necessary.
    #[must_use]
    pub fn ttl(&self, key: &[u8]) -> TimeToLive {
        let entries = self.live_entries(key);
        let Some(entry) = entries.get(key) else {
            return TimeToLive::Missing;
        };
        entry.expires_at.map_or(TimeToLive::Persistent, |deadline| {
            TimeToLive::ExpiresIn(deadline.saturating_duration_since(Instant::now()))
        })
    }

    /// Remove at most `limit` expired entries and return the number removed.
    #[must_use]
    pub fn cleanup_expired(&self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let now = Instant::now();
        let mut entries = self.write_entries();
        let expired: Vec<Vec<u8>> = entries
            .iter()
            .filter(|(_, entry)| entry.is_expired(now))
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect();
        let count = expired.len();
        for key in expired {
            entries.remove(&key);
        }
        count
    }

    /// Return the number of live keys after a complete expiry pass.
    #[must_use]
    pub fn len(&self) -> usize {
        let _ = self.cleanup_expired(usize::MAX);
        self.write_entries().len()
    }

    /// Return whether the engine contains no live keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn collection_len(
        &self,
        key: &[u8],
        length: impl FnOnce(&StoredValue) -> Option<usize>,
    ) -> Result<usize, EngineError> {
        let entries = self.live_entries(key);
        match entries.get(key) {
            None => Ok(0),
            Some(entry) => length(&entry.value).ok_or(EngineError::WrongType),
        }
    }

    fn live_entries(&self, key: &[u8]) -> RwLockWriteGuard<'_, HashMap<Vec<u8>, Entry>> {
        let now = Instant::now();
        let mut entries = self.write_entries();
        if entries.get(key).is_some_and(|entry| entry.is_expired(now)) {
            entries.remove(key);
        }
        entries
    }

    fn snapshot(&self) -> Snapshot {
        let _ = self.cleanup_expired(usize::MAX);
        let entries = self.write_entries();
        Snapshot {
            version: SNAPSHOT_VERSION,
            entries: entries
                .iter()
                .map(|(key, entry)| PersistedEntry {
                    key: key.clone(),
                    value: entry.value.clone(),
                    expires_at_unix_ms: entry.expires_at_unix_ms,
                })
                .collect(),
        }
    }

    fn write_entries(&self) -> RwLockWriteGuard<'_, HashMap<Vec<u8>, Entry>> {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn range_bounds(length: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if length == 0 {
        return None;
    }
    let length = i64::try_from(length).unwrap_or(i64::MAX);
    let start = if start < 0 {
        length.saturating_add(start)
    } else {
        start
    }
    .max(0);
    let stop = if stop < 0 {
        length.saturating_add(stop)
    } else {
        stop
    }
    .min(length - 1);
    if start > stop || start >= length || stop < 0 {
        None
    } else {
        Some((usize::try_from(start).ok()?, usize::try_from(stop).ok()?))
    }
}

fn unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn temporary_snapshot_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("veltrix.snapshot");
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        unix_time_ms()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn strings_are_binary_safe_and_expire() {
        let engine = Engine::new();
        engine.set(b"k\0".to_vec(), b"v\0".to_vec(), None);
        assert_eq!(engine.get(b"k\0"), Ok(Some(b"v\0".to_vec())));
        engine.set(
            b"soon".to_vec(),
            b"gone".to_vec(),
            Some(Duration::from_millis(10)),
        );
        thread::sleep(Duration::from_millis(20));
        assert_eq!(engine.get(b"soon"), Ok(None));
        assert_eq!(engine.ttl(b"soon"), TimeToLive::Missing);
    }

    #[test]
    fn collections_enforce_types_and_ranges() {
        let engine = Engine::new();
        assert_eq!(
            engine.hash_set(b"hash", &[(b"field".to_vec(), b"value".to_vec())]),
            Ok(1)
        );
        assert_eq!(
            engine.hash_get(b"hash", b"field"),
            Ok(Some(b"value".to_vec()))
        );
        assert_eq!(engine.list_len(b"hash"), Err(EngineError::WrongType));

        assert_eq!(
            engine.list_push(b"list", &[b"a".to_vec(), b"b".to_vec()], false),
            Ok(2)
        );
        assert_eq!(
            engine.list_range(b"list", 0, -1),
            Ok(vec![b"a".to_vec(), b"b".to_vec()])
        );
        assert_eq!(engine.list_pop(b"list", true), Ok(Some(b"a".to_vec())));

        assert_eq!(
            engine.set_add(b"set", &[b"a".to_vec(), b"a".to_vec()]),
            Ok(1)
        );
        assert_eq!(engine.set_contains(b"set", b"a"), Ok(true));

        assert_eq!(
            engine.sorted_set_add(b"zset", &[(2.0, b"b".to_vec()), (1.0, b"a".to_vec())]),
            Ok(2)
        );
        assert_eq!(
            engine.sorted_set_range(b"zset", 0, -1),
            Ok(vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), 2.0)])
        );
    }

    #[test]
    fn atomic_snapshot_recovers_types_and_expiry() {
        let directory = std::env::temp_dir().join(format!(
            "veltrix-engine-test-{}-{}",
            std::process::id(),
            unix_time_ms()
        ));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("database.snapshot");

        let engine = Engine::new();
        engine.set(b"string".to_vec(), b"value".to_vec(), None);
        engine
            .hash_set(b"hash", &[(b"field".to_vec(), b"value".to_vec())])
            .unwrap();
        engine.set(
            b"expires".to_vec(),
            b"value".to_vec(),
            Some(Duration::from_secs(2)),
        );
        engine.save_snapshot(&path).unwrap();

        let recovered = Engine::load_snapshot(&path).unwrap();
        assert_eq!(recovered.get(b"string"), Ok(Some(b"value".to_vec())));
        assert_eq!(
            recovered.hash_get(b"hash", b"field"),
            Ok(Some(b"value".to_vec()))
        );
        assert!(matches!(
            recovered.ttl(b"expires"),
            TimeToLive::ExpiresIn(_)
        ));

        let expiring = Engine::new();
        expiring.set(
            b"must-not-resurrect".to_vec(),
            b"value".to_vec(),
            Some(Duration::from_millis(10)),
        );
        expiring.save_snapshot(&path).unwrap();
        thread::sleep(Duration::from_millis(20));
        let recovered = Engine::load_snapshot(&path).unwrap();
        assert_eq!(recovered.get(b"must-not-resurrect"), Ok(None));

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn incomplete_temporary_snapshot_does_not_replace_last_good_snapshot() {
        let directory = std::env::temp_dir().join(format!(
            "veltrix-engine-crash-test-{}-{}",
            std::process::id(),
            unix_time_ms()
        ));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("database.snapshot");
        let engine = Engine::new();
        engine.set(b"safe".to_vec(), b"value".to_vec(), None);
        engine.save_snapshot(&path).unwrap();

        // Simulate a process dying while a new same-directory temp file is
        // only partially written. The atomically renamed snapshot remains valid.
        fs::write(
            directory.join(".database.snapshot.interrupted.tmp"),
            b"{\"version\":",
        )
        .unwrap();
        let recovered = Engine::load_snapshot(&path).unwrap();
        assert_eq!(recovered.get(b"safe"), Ok(Some(b"value".to_vec())));

        fs::remove_dir_all(directory).unwrap();
    }
}
