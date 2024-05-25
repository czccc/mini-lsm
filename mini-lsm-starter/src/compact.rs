#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

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

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
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
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
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
    fn two_level_compact(&self, upper: &[usize], lower: &[usize]) -> Result<Vec<Arc<SsTable>>> {
        let state = self.state.read();
        let mut iters = Vec::new();
        for sst_id in upper.iter().chain(lower.iter()) {
            let sstable = state.sstables[sst_id].clone();
            let iter = SsTableIterator::create_and_seek_to_first(sstable)?;
            iters.push(Box::new(iter));
        }
        let mut merge_iter = MergeIterator::create(iters);
        let mut builders = vec![SsTableBuilder::new(self.options.block_size)];
        while merge_iter.is_valid() {
            if builders.last_mut().unwrap().estimated_size() > self.options.target_sst_size {
                builders.push(SsTableBuilder::new(self.options.block_size));
            }
            if !merge_iter.value().is_empty() {
                builders
                    .last_mut()
                    .unwrap()
                    .add(merge_iter.key(), merge_iter.value());
            }
            merge_iter.next()?;
        }
        let mut new_sstables = Vec::new();
        for builder in builders {
            let sst_id = self.next_sst_id();
            let sstable = builder.build(sst_id, None, self.path_of_sst(sst_id))?;
            new_sstables.push(Arc::new(sstable));
        }

        Ok(new_sstables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        if let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = task
        {
            return self.two_level_compact(l0_sstables, l1_sstables);
        }

        if let CompactionTask::Simple(task) = task {
            return self.two_level_compact(&task.upper_level_sst_ids, &task.lower_level_sst_ids);
        }

        unimplemented!()
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].clone().1)
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let new_ssts = self.compact(&task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.l0_sstables = state
                .l0_sstables
                .clone()
                .into_iter()
                .skip(l0_sstables.len())
                .collect();
            state.levels[0] = (1, new_ssts.iter().map(|x| x.sst_id()).collect());
            for sst_id in l0_sstables.iter().chain(&l1_sstables) {
                if let Some(sst) = state.sstables.remove(sst_id) {
                    std::fs::remove_file(self.path_of_sst(sst.sst_id()))?
                }
            }
            for sst in new_ssts {
                state.sstables.insert(sst.sst_id(), sst);
            }
            *guard = Arc::new(state);
        };
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        match &self.options.compaction_options {
            CompactionOptions::Leveled(_) => todo!(),
            CompactionOptions::Tiered(_) => todo!(),
            CompactionOptions::Simple(options) => {
                let controller = CompactionController::Simple(
                    SimpleLeveledCompactionController::new(options.clone()),
                );
                let snapshot = (*self.state.read()).clone();
                if let Some(task) = controller.generate_compaction_task(&snapshot) {
                    println!("running compaction task: {task:?}");

                    let new_sstables = self.compact(&task)?;
                    let new_sst_ids: Vec<usize> = new_sstables.iter().map(|x| x.sst_id()).collect();

                    let _state_lock = self.state_lock.lock();
                    let mut guard = self.state.write();
                    let snapshot = (*guard).clone();

                    let (mut new_state, old_sst_ids) =
                        controller.apply_compaction_result(&snapshot, &task, &new_sst_ids);

                    println!(
                        "compaction finished: {} files removed, {} files added",
                        old_sst_ids.len(),
                        new_sstables.len(),
                    );

                    for sst_id in &old_sst_ids {
                        if let Some(sst) = new_state.sstables.remove(sst_id) {
                            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?
                        }
                    }
                    for sst in new_sstables {
                        new_state.sstables.insert(sst.sst_id(), sst);
                    }
                    *guard = Arc::new(new_state);
                }
            }
            CompactionOptions::NoCompaction => {}
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
        if self.state.read().imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
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
