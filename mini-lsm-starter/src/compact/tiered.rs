use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // 1. Triggered by Space Amplification Ratio
        let total_size: f64 = snapshot.levels.iter().map(|x| x.1.len() as f64).sum();
        let last_tier_size = snapshot.levels.last().unwrap().1.len() as f64;
        if (total_size - last_tier_size) / last_tier_size * 100.0
            >= (self.options.max_size_amplification_percent as f64)
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // 2. Triggered by Size Ratio
        let mut prev_tire_size: f64 = 0.;
        for i in 0..snapshot.levels.len() {
            let curr_tier_size = snapshot.levels[i].1.len() as f64;
            if i != 0
                && prev_tire_size / curr_tier_size
                    >= (100.0 + self.options.size_ratio as f64) / 100.0
                && i + 1 >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.iter().take(i + 1).cloned().collect(),
                    bottom_tier_included: i == snapshot.levels.len() - 1,
                });
            }
            prev_tire_size += curr_tier_size;
        }

        // 3. Reduce Sorted Runs
        let num = snapshot.levels.len() - self.options.num_tiers + 2;
        if num > 0 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.iter().take(num).cloned().collect(),
                bottom_tier_included: false,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();

        let compacted_tier_ids: BTreeSet<usize> = task.tiers.iter().map(|x| x.0).collect();

        state.levels.clear();
        let mut new_tier_inserted = false;
        for i in 0..snapshot.levels.len() {
            let id = snapshot.levels[i].0;
            if compacted_tier_ids.get(&id).is_none() {
                state.levels.push(snapshot.levels[i].clone());
            } else if !new_tier_inserted {
                state.levels.push((output[0], output.to_vec()));
                new_tier_inserted = true;
            }
        }

        (
            state,
            task.tiers.clone().into_iter().flat_map(|x| x.1).collect(),
        )
    }
}
