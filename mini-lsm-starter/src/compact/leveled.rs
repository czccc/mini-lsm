use std::{collections::HashSet, ops::Bound};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let (first_key, last_key) = sst_ids
            .iter()
            .map(|sst_id| &snapshot.sstables[sst_id])
            .map(|sstable| (sstable.first_key(), sstable.last_key()))
            .reduce(|(first_key1, last_key1), (first_key2, last_key2)| {
                (first_key1.min(first_key2), last_key1.max(last_key2))
            })
            .unwrap();
        snapshot.levels[in_level - 1]
            .1
            .iter()
            .filter(|sst_id| {
                snapshot.sstables[sst_id].contains_range(
                    Bound::Included(first_key.key_ref()),
                    Bound::Included(last_key.key_ref()),
                )
            })
            .copied()
            .collect()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // 1. Compute Target Sizes and Decide Base Level
        let real_sizes: Vec<usize> = snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                sst_ids
                    .iter()
                    .map(|sst_id| snapshot.sstables[sst_id].table_size() as usize)
                    .sum()
            })
            .collect();

        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        let mut target_size: Vec<usize> = vec![0; self.options.max_levels];
        target_size[self.options.max_levels - 1] =
            *real_sizes.last().unwrap().max(&base_level_size_bytes);
        let mut base_level = target_size.len();
        for i in (0..(target_size.len() - 1)).rev() {
            let next_level_size = target_size[i + 1];
            if next_level_size > base_level_size_bytes {
                target_size[i] = next_level_size / self.options.level_size_multiplier;
            }
            if target_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // println!(
        //     "base_level: {base_level}, target_size: {target_size:?}, real_size: {real_sizes:?}"
        // );

        // 2. Decide Level Priorities
        // 2.1 check l0_sstables size with highest priorities
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == snapshot.levels.len(),
            });
        }

        // 2.2 compare other level priorities
        let mut priorities = Vec::with_capacity(snapshot.levels.len());
        for level in 0..(snapshot.levels.len() - 1) {
            if real_sizes[level] > 0 && real_sizes[level] > target_size[level] {
                priorities.push((
                    level + 1,
                    real_sizes[level] as f64 / target_size[level] as f64,
                ));
            }
        }

        // 3. Select SST to Compact
        if !priorities.is_empty() {
            priorities.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().reverse());

            let level = priorities.first().unwrap().0;
            let sst_id = snapshot.levels[level - 1].1.iter().min().copied().unwrap();

            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![sst_id],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[sst_id], level + 1),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();
        let removed_sstable: HashSet<usize> = task
            .upper_level_sst_ids
            .iter()
            .chain(&task.lower_level_sst_ids)
            .copied()
            .collect();
        match task.upper_level {
            Some(level) => {
                state.levels[level - 1].1 = snapshot.levels[level - 1]
                    .1
                    .iter()
                    .filter(|sst_id| removed_sstable.get(sst_id).is_none())
                    .copied()
                    .collect();
            }
            None => {
                state.l0_sstables = snapshot
                    .l0_sstables
                    .iter()
                    .filter(|sst_id| removed_sstable.get(sst_id).is_none())
                    .copied()
                    .collect();
            }
        };

        let mut new_sstable_inserted = false;
        let lower_level = &mut state.levels[task.lower_level - 1].1;
        lower_level.clear();
        for sst_id in &snapshot.levels[task.lower_level - 1].1 {
            if removed_sstable.get(sst_id).is_none() {
                lower_level.push(*sst_id);
            } else if !new_sstable_inserted {
                lower_level.extend(output);
                new_sstable_inserted = true;
            }
        }
        if !new_sstable_inserted {
            lower_level.extend(output);
        }
        lower_level.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });

        println!("state {:?} -> {:?}", snapshot.levels, state.levels);

        (state, removed_sstable.into_iter().collect())
    }
}
