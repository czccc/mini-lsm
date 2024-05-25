#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Seek};

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut};
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
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf: &[u8] = buf.as_ref();

        let mut records = Vec::new();
        while buf.has_remaining() {
            let record_len = buf.get_u32();

            let record_buf = buf.get(..record_len as usize).unwrap();
            buf.advance(record_len as usize);
            let checksum = buf.get_u32();
            if checksum != crc32fast::hash(record_buf) {
                return Err(anyhow!("checksum invalid!"));
            }

            records.push(serde_json::from_slice(record_buf)?);
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

        let record_buf = serde_json::to_vec(&record)?;

        let mut buf = Vec::new();
        buf.put_u32(record_buf.len() as u32);
        buf.put(record_buf.as_ref());
        buf.put_u32(crc32fast::hash(&record_buf));

        file.write_all(buf.as_ref())?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
}
