#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use bytes::{Buf, BufMut, BytesMut};

use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.as_ref())?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = File::options().read(true).write(true).open(path.as_ref())?;
        file.seek(std::io::SeekFrom::Start(0))?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = Bytes::from(buf);

        while buf.has_remaining() {
            let key_len = buf.get_u32();
            let key = Bytes::copy_from_slice(buf.get(..(key_len as usize)).unwrap());
            buf.advance(key_len as usize);
            let value_len = buf.get_u32();
            let value = Bytes::copy_from_slice(buf.get(..(value_len as usize)).unwrap());
            buf.advance(value_len as usize);
            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u32(key.len() as u32);
        buf.put(key);
        buf.put_u32(value.len() as u32);
        buf.put(value);
        self.file.lock().write_all(buf.freeze().as_ref())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().flush()?;
        self.file.lock().get_mut().sync_all()?;
        Ok(())
    }
}
