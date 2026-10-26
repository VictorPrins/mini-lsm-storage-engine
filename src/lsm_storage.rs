// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, KeyVec};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

// cache key: (sst_id, block_idx)
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Anything sent over the notifier channel triggers the ending of the flush thread.
        // send() may return an error if the channel is already disconnected. We don't return this error but silence it.
        let _ = self.flush_notifier.send(());
        match self.flush_thread.lock().take() {
            None => Ok(()),
            Some(joinhandle) => joinhandle
                // join waits for this thread to finish
                .join()
                .map_err(|e| anyhow!("Flush thread panicked.")),
        }
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();

        // create the dir if it does not yet exist
        fs::create_dir_all(path)?;

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state_snapshot = Arc::clone(&self.state.read());

        // iterate over all memtable in order from newest to oldest
        for memtable in
            std::iter::once(&state_snapshot.memtable).chain(&state_snapshot.imm_memtables)
        {
            let val = memtable.get(key);
            if let Some(val) = val {
                if val.is_empty() {
                    // this key is deleted
                    return Ok(None);
                } else {
                    // key found
                    return Ok(Some(val));
                }
            }
        }

        let target_key = KeySlice::from_slice(key);

        for sst_id in &state_snapshot.l0_sstables {
            let iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(&state_snapshot.sstables[sst_id]),
                target_key,
            )?;

            if iter.is_valid() && iter.key() == target_key {
                if iter.value().is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                }
            }
        }

        // key not found in any memtable or sst
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let memtable_size = {
            // acquire lock on the state
            let state_guard = self.state.read();
            let memtable = &state_guard.memtable;
            memtable.put(_key, _value)?;
            memtable.approximate_size()
        };

        // Check if memtable capacity is reached
        if memtable_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();

            // Confirm capacity is still reached after locking to prevent race condition
            let current_memtable = Arc::clone(&self.state.read().memtable);
            if current_memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // get write access to the state
        let mut state_guard = self.state.write();

        // copy on write: clone the state so we can safely make changes
        let mut snapshot = LsmStorageState::clone(&state_guard);

        // create new memtable
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        // move new memtable in position and get the old one out
        let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);

        // prepend the old memtable to imm_memtables
        snapshot.imm_memtables.insert(0, old_memtable);

        *state_guard = Arc::new(snapshot);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // mutate the state:
        // acquire state lock
        // the state lock prevents any other structural changes for the whole duration it is held. We hold this lock during IO. However, get()/put() on the current memtable can proceed whilst state_lock is held.

        // acquire read lock to access the state
        // clone state Arc (snapshot) so we don't interfere with concurrent readers
        // release read lock

        // check if imm_memtables has entries. If not, return OK

        // clone LsmStorageState so we can mutate it
        // do the slow work:
        // pop last MemTable of imm_memtables
        // convert into sst using ::flush()
        // build sst (this incurs disk IO)
        // push sst at index 0 of l0_sstables
        // add sst to hashmap

        // acquire write lock on state. This lock is exclusive: whilst it is held, no other thread can concurrently read the state.
        // assign the new version of the state
        // release write lock
        // release state_lock

        let struct_change_guard = self.state_lock.lock();

        let state_snapshot = Arc::clone(&self.state.read());

        if state_snapshot.imm_memtables.is_empty() {
            return Ok(());
        };

        let mut new_state = LsmStorageState::clone(&state_snapshot);

        let mt_to_flush = new_state.imm_memtables.pop().unwrap();

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        mt_to_flush.flush(&mut sst_builder)?;
        // build the sst: this incurs IO
        let sst_id = mt_to_flush.id();
        let sst = sst_builder.build(
            sst_id,
            Some(Arc::clone(&self.block_cache)),
            &self.path_of_sst(sst_id),
        )?;

        new_state.l0_sstables.insert(0, sst_id);
        new_state.sstables.insert(sst_id, Arc::new(sst));

        // Apply the structural change on the state
        *self.state.write() = Arc::new(new_state);

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = Arc::clone(&self.state.read());

        let memtable_iters = std::iter::once(&snapshot.memtable)
            .chain(&snapshot.imm_memtables)
            .map(|mt| Box::new(mt.scan(lower, upper)))
            .collect();

        let start_key = match lower {
            Bound::Unbounded => KeyVec::new(),
            Bound::Included(key) => KeySlice::from_slice(key).to_key_vec(),
            Bound::Excluded(key) => {
                // in this case we need to set the start key to the bytestring that is exactly 1 step greater than key according to lexicographic ordering
                // This is the key that has the 0 byte padded. i.e. same prefix, 1 increment greater length.
                let mut next_key = Vec::from(key);
                next_key.push(0);
                KeyVec::from_vec(next_key)
            }
        };

        let mut sst_iters = vec![];
        for sst_id in &snapshot.l0_sstables {
            sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                Arc::clone(&snapshot.sstables[sst_id]),
                start_key.as_key_slice(),
            )?));
        }

        let memtable_merge_iter = MergeIterator::create(memtable_iters);
        let sst_merge_iter = MergeIterator::create(sst_iters);

        let upper_bound = upper.map(|x| Bytes::copy_from_slice(x));
        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(memtable_merge_iter, sst_merge_iter)?,
            upper_bound,
        )?))
    }
}
