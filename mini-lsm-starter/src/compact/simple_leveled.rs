use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut level_sst_ids = Vec::new();
        level_sst_ids.push(snapshot.l0_sstables.clone());
        for i in 0..self.options.max_levels {
            level_sst_ids.push(snapshot.levels.get(i).unwrap_or(&(0, Vec::new())).1.clone());
        }

        for level in 0..self.options.max_levels {
            let upper_sst_ids = &level_sst_ids[level];
            let lower_sst_ids = &level_sst_ids[level + 1];

            if level == 0 && upper_sst_ids.len() >= self.options.level0_file_num_compaction_trigger
            {
                println!(
                    "compaction triggered at level {} and {} with level0 size {}, level_sst_ids {:?}",
                    level,
                    level + 1,
                    self.options.level0_file_num_compaction_trigger,
                    level_sst_ids
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids: upper_sst_ids.clone(),
                    lower_level: level + 1,
                    lower_level_sst_ids: lower_sst_ids.clone(),
                    is_lower_level_bottom_level: (level + 1) == self.options.max_levels,
                });
            }

            if level > 0
                && 100.0 * (lower_sst_ids.len() as f64) / (upper_sst_ids.len() as f64)
                    < (self.options.size_ratio_percent as f64)
            {
                println!(
                    "compaction triggered at level {} and {} with ratio {}, level_sst_ids {:?}",
                    level,
                    level + 1,
                    100.0 * (lower_sst_ids.len() as f64) / (upper_sst_ids.len() as f64),
                    level_sst_ids
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: upper_sst_ids.clone(),
                    lower_level: level + 1,
                    lower_level_sst_ids: lower_sst_ids.clone(),
                    is_lower_level_bottom_level: (level + 1) == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();

        match task.upper_level {
            Some(level) => {
                state.levels[level - 1] = (level, Vec::new());
            }
            None => {
                state.l0_sstables = state.l0_sstables
                    [..(state.l0_sstables.len() - task.upper_level_sst_ids.len())]
                    .to_vec();
            }
        }
        state.levels[task.lower_level - 1] = (task.lower_level, output.to_vec());
        (
            state,
            task.upper_level_sst_ids
                .clone()
                .into_iter()
                .chain(task.lower_level_sst_ids.clone())
                .collect(),
        )
    }
}
