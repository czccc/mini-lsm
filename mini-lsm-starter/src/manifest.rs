#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Seek};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.as_ref())?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = File::options().read(true).write(true).open(path.as_ref())?;
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;

        let mut records = Vec::new();

        for record in serde_json::Deserializer::from_slice(buf.as_ref()).into_iter() {
            records.push(record?);
        }

        let manifest = Self {
            file: Arc::new(Mutex::new(file)),
        };
        Ok((manifest, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        file.sync_all()?;
        file.seek(std::io::SeekFrom::End(0))?;
        let buf = serde_json::to_string(&record)?;
        file.write_all(buf.as_ref())?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
}
