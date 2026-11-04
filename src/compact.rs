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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};

use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        // get the SsTable instances
        // create a mergeiterator over all ssts
        // iterate over all of them, and build new ssts. Drop tombstones

        let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = task
        else {
            unimplemented!();
        };

        let state_snapshot = Arc::clone(&self.state.read());

        // Create individual iterators over the SSTs of L0
        let l0_sst_iters = l0_sstables
            .iter()
            .map(|id| {
                SsTableIterator::create_and_seek_to_first(Arc::clone(&state_snapshot.sstables[id]))
            })
            .collect::<Result<Vec<SsTableIterator>>>()?;

        // create the iterator that iterates over all of L0
        let l0_iter = MergeIterator::create(l0_sst_iters.into_iter().map(Box::new).collect());

        // create the iterator that iterates over all of L1
        let l1_iter = SstConcatIterator::create_and_seek_to_first(
            l1_sstables
                .iter()
                .map(|id| Arc::clone(&state_snapshot.sstables[id]))
                .collect(),
        )?;

        // create the iterator that iterates over all of L0 and L1 combined
        let mut iterator = TwoMergeIterator::create(l0_iter, l1_iter)?;

        let mut new_l1_sstables = Vec::new();

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);

        // use this bool to track if the sst builder contains at least one (non-tombstone) value
        let mut sst_nonempty = false;

        while iterator.is_valid() {
            // drop tombstones
            if !iterator.value().is_empty() {
                sst_builder.add(iterator.key(), iterator.value());
                sst_nonempty = true;
            }

            if sst_builder.estimated_size() > self.options.target_sst_size {
                let sst = self.finish_sst_builder(sst_builder)?;
                new_l1_sstables.push(Arc::new(sst));
                sst_builder = SsTableBuilder::new(self.options.block_size);
                sst_nonempty = false;
            }

            iterator.next()?;
        }

        if sst_nonempty {
            // Finish the final builder
            let sst = self.finish_sst_builder(sst_builder)?;
            new_l1_sstables.push(Arc::new(sst));
        }

        Ok(new_l1_sstables)
    }

    fn finish_sst_builder(&self, sst_builder: SsTableBuilder) -> Result<SsTable> {
        let id = self.next_sst_id();

        sst_builder.build(
            id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(id),
        )
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // Important assumption: no two compactions will ever run concurrently. We don't lock to preserve this; we rely on this assumption.

        // take a snapshot of the state
        // call compact
        // apply the compaction result to the state

        // snapshot the state
        let snapshot = Arc::clone(&self.state.read());

        let l1_sstables = match snapshot.levels.get(0) {
            Some((level, ids)) => ids.clone(),
            None => Vec::new(),
        };

        // clone l1 sstables one more time, because we need it later and the original variable won't be accessible anymore after it is moved
        let l1_sstables_old = l1_sstables.clone();

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: snapshot.l0_sstables.clone(),
            l1_sstables: l1_sstables,
        };

        let new_ssts = self.compact(&task)?;

        {
            // lock the state for structural changes
            let _state_guard = self.state_lock.lock();
            let mut state = LsmStorageState::clone(&self.state.read());

            // remove all l0 table ids that this compaction function moved out of l0. Keep the l0 table ids that were added to l0 since this compaction function started.
            state
                .l0_sstables
                .retain(|id| !snapshot.l0_sstables.contains(id));

            // install the new L1
            state.levels[0] = (1, new_ssts.iter().map(|sst| sst.sst_id()).collect());

            // remove all SSTs that were compacted away
            for id in snapshot.l0_sstables.iter().chain(&l1_sstables_old) {
                state.sstables.remove(id);
            }

            // update hashmap with new SSTs
            for sst in &new_ssts {
                state.sstables.insert(sst.sst_id(), Arc::clone(sst));
            }

            *self.state.write() = Arc::new(state);
        }

        for id in snapshot.l0_sstables.iter().chain(&l1_sstables_old) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            return self.force_flush_next_imm_memtable();
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
