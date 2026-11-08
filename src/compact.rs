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
use crate::key::KeySlice;
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

        let snapshot = Arc::clone(&self.state.read());

        // Because L0 is not a sorted run unlike the lower levels, it must be iterated over using MergeIterator rather than the ConcatIterator that can handle L1 .. Ln. This means that any task that compacts L0 has differently typed iterators from any compaction task that compacts L1 .. Ln

        // In Rust's strict typing system, any variable in the code must have a exactly 1 type. Variables that hold generic structs are no exception to this. They also have 1 type, due to monomorphization. Hence two variables, holding the same generic struct, each with a different generic type (eg Vec<u8> vs Vec<i32> ; both using generic struct Vec<T>) do NOT have the same type.
        // The consequence of this is that we can't abstract over the two iterators, even though we use them in identical ways. Hence we can't apply DRY and need to repeat some code. This is because the type of the special iterator for L0 penetrates. i.e. any code using that special iterator is differently typed than code using the other iterator, and thus we need to both cases in separate branches until both branches converge again to identical types. In our case this happens on method return and no earlier.

        // Some more background on this language feature:
        // The benefit of this strict typing is that it allows static dispatch. If instead you want dynamic dispatch, we can use Box<dyn Trait>. Variables typed like this are called 'Trait objects' in Rust. This can sometimes make code easier and reduce boilerplate, at the cost of chasing a heap pointer to the vtable at runtime any time a method is called on this type.
        // These threads contain good discussions:
        // https://www.reddit.com/r/rust/comments/1etyqtl/on_using_dynamic_dispatch/
        // https://www.reddit.com/r/rust/comments/ta2cei/why_exactly_cant_you_always_do_static_dispatch_in

        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // Create individual iterators over the SSTs of L0
                let l0_sst_iters = l0_sstables
                    .iter()
                    .map(|id| {
                        SsTableIterator::create_and_seek_to_first(Arc::clone(
                            &snapshot.sstables[id],
                        ))
                    })
                    .collect::<Result<Vec<SsTableIterator>>>()?;

                // create the iterator that iterates over all of L0
                let l0_iter =
                    MergeIterator::create(l0_sst_iters.into_iter().map(Box::new).collect());

                // create the iterator that iterates over all of L1
                let l1_iter = SstConcatIterator::create_and_seek_to_first(
                    l1_sstables
                        .iter()
                        .map(|id| Arc::clone(&snapshot.sstables[id]))
                        .collect(),
                )?;

                // create the iterator that iterates over all of L0 and L1 combined
                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;

                self.compact_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(task_details) => {
                match task_details.upper_level {
                    // None means the upper level is L0
                    None => {
                        // Note: the code in this arm is virtually identical to the code in arm 'CompactionTask::ForceFullCompaction'

                        let l0_sst_iters = task_details
                            .upper_level_sst_ids
                            .iter()
                            .map(|id| {
                                SsTableIterator::create_and_seek_to_first(Arc::clone(
                                    &snapshot.sstables[id],
                                ))
                            })
                            .collect::<Result<Vec<SsTableIterator>>>()?;

                        // create the iterator that iterates over all of L0
                        let l0_iter =
                            MergeIterator::create(l0_sst_iters.into_iter().map(Box::new).collect());

                        // create the iterator that iterates over all of L1
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(
                            task_details
                                .lower_level_sst_ids
                                .iter()
                                .map(|id| Arc::clone(&snapshot.sstables[id]))
                                .collect(),
                        )?;

                        // create the iterator that iterates over all of L0 and L1 combined
                        let iter = TwoMergeIterator::create(l0_iter, lower_iter)?;
                        self.compact_from_iter(iter, task.compact_to_bottom_level())
                    }
                    // Some means the upper level is a level below L0.
                    Some(lvl) => {
                        // create the iterator that iterates over all of L1
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(
                            task_details
                                .upper_level_sst_ids
                                .iter()
                                .map(|id| Arc::clone(&snapshot.sstables[id]))
                                .collect(),
                        )?;

                        // create the iterator that iterates over all of L1
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(
                            task_details
                                .lower_level_sst_ids
                                .iter()
                                .map(|id| Arc::clone(&snapshot.sstables[id]))
                                .collect(),
                        )?;

                        // create the iterator that iterates over all of L0 and L1 combined
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_from_iter(iter, task.compact_to_bottom_level())
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    fn compact_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_l1_sstables = Vec::new();

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);

        // use this bool to track if the sst builder contains at least one (non-tombstone) value
        let mut sst_nonempty = false;

        while iter.is_valid() {
            // Only drop tombstones when we are compacting to the bottom level. If we are compacting to any level higher than that, we must preserve tombstones. This is because a lower level could still contain a value for this deleted key, so if we drop the tombstone we resurrect a deleted value.
            if !iter.value().is_empty() || !compact_to_bottom_level {
                sst_builder.add(iter.key(), iter.value());
                sst_nonempty = true;
            }

            if sst_builder.estimated_size() > self.options.target_sst_size {
                let sst = self.finish_sst_builder(sst_builder)?;
                new_l1_sstables.push(Arc::new(sst));
                sst_builder = SsTableBuilder::new(self.options.block_size);
                sst_nonempty = false;
            }

            iter.next()?;
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
        let CompactionOptions::Simple(options) = &self.options.compaction_options else {
            unimplemented!();
        };

        let controller =
            CompactionController::Simple(SimpleLeveledCompactionController::new(options.clone()));

        let Some(task) = controller.generate_compaction_task(&self.state.read()) else {
            // no compaction needed if this method returns None
            return Ok(());
        };

        let new_ssts = self.compact(&task)?;

        let ssts_to_remove = {
            // lock for structural changes to the state
            let _state_guard = self.state_lock.lock();

            let snapshot = Arc::clone(&self.state.read());

            let new_sst_ids: Vec<usize> = new_ssts.iter().map(|sst| sst.sst_id()).collect();

            // apply_compaction_result does NOT update the hashmap. This is a slightly odd design, but that's how it works. apply_compaction_result updates state.l0_sstables and state.levels, but updating the hashmap state.sstables remains the responsibility of this method.
            let (mut new_state, ssts_to_remove) =
                controller.apply_compaction_result(&snapshot, &task, &new_sst_ids, false);

            // remove old SSTs from the hashmap
            for id in &ssts_to_remove {
                new_state.sstables.remove(id);
            }

            // add new SSTs to the hashmap
            for sst in new_ssts {
                new_state.sstables.insert(sst.sst_id(), sst);
            }

            // install the new state
            *self.state.write() = Arc::new(new_state);
            ssts_to_remove
        };

        // delete old ssts from disk
        for id in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
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
