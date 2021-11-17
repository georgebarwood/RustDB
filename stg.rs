/// Storage interface for CompactFile and VersionStorage.
pub trait Storage {
    // Get the current size of the underlying storage.
    fn size(&mut self) -> u64;
    // Read from the underlying storage.
    fn read(&mut self, off: u64, bytes: &mut [u8]);
    // Write to the underlying storage.
    fn write(&mut self, off: u64, bytes: &[u8]);
    // Finish write transaction, size is new size of underlying storage.
    fn commit(&mut self, size: u64);
}

use crate::cache::Cache;
use std::cmp::min;
use std::sync::{Arc, Mutex};

pub struct SharedStorageInner {
    pub stg: SimpleFileStorage,
    pub cache: Cache<Vec<u8>>,
}

/// Multiple versioned views on underlying SimpleFileStorage.
pub struct SharedStorage {
    pub x: Mutex<SharedStorageInner>,
}

impl SharedStorage {
    const PAGE_SIZE: usize = 1024;

    pub fn end_read(&self, time: u64) {
        let mut x = self.x.lock().unwrap();
        x.cache.end_read(time);
    }

    pub fn read(&self, time: u64, mut off: u64, bytes: &mut [u8]) {
        let mut x = self.x.lock().unwrap();
        let mut done: usize = 0;
        let len: usize = bytes.len();
        while done < len {
            let page = off / Self::PAGE_SIZE as u64;
            let poff = off as usize % Self::PAGE_SIZE;
            let amount: usize = min(Self::PAGE_SIZE - poff, len - done);
            if let Some(cp) = x.cache.get(page, time) {
                // Copy bytes from the cached page.
                // println!("Using cache page {} time={}", page, time);
                bytes[done..done + amount].copy_from_slice(&cp[poff..poff + amount]);
            } else {
                // Get bytes from the file.
                x.stg.read(off, &mut bytes[done..done + amount]);
            }
            done += amount;
            off += amount as u64;
        }
    }

    pub fn write(&self, woff: u64, bytes: &[u8]) {
        let mut x = self.x.lock().unwrap();
        // Save copies of affected pages in cache for readers.
        let mut done: usize = 0;
        let mut off = woff;
        let len: usize = bytes.len();
        while done < len {
            let page = off / Self::PAGE_SIZE as u64;
            let poff = off as usize % Self::PAGE_SIZE;
            let amount: usize = min(Self::PAGE_SIZE - poff, len - done);

            if !x.cache.saved(page) {
                let mut buffer = vec![0; Self::PAGE_SIZE];
                x.stg.read(page * Self::PAGE_SIZE as u64, &mut buffer);
                // println!("Setting cache page {}", page);
                x.cache.set(page, buffer);
            }
            done += amount;
            off += amount as u64;
        }
        // Write to underlying file.
        x.stg.write(woff, bytes);
    }

    pub fn direct_read(&self, off: u64, bytes: &mut [u8]) {
        let mut x = self.x.lock().unwrap();
        x.stg.read(off, bytes);
    }

    pub fn commit(&self, size: u64) {
        let mut x = self.x.lock().unwrap();
        x.stg.commit(size);
        x.cache.tick();
    }

    pub fn new(stg: SimpleFileStorage) -> Self {
        Self {
            x: Mutex::new(SharedStorageInner {
                stg,
                cache: Cache::new(),
            }),
        }
    }

    pub fn open_read(self: &Arc<SharedStorage>) -> VersionStorage {
        let mut x = self.x.lock().unwrap();
        VersionStorage {
            writer: false,
            time: x.cache.begin_read(),
            ss: self.clone(),
            size: x.stg.size(),
        }
    }
    pub fn open_write(self: &Arc<SharedStorage>) -> VersionStorage {
        let mut x = self.x.lock().unwrap();
        VersionStorage {
            writer: true,
            time: 0,
            ss: self.clone(),
            size: x.stg.size(),
        }
    }
}

/// Versioned Storage for concurrent reads.
pub struct VersionStorage {
    writer: bool,
    size: u64,
    time: u64,
    ss: Arc<SharedStorage>,
}

impl Storage for VersionStorage {
    fn size(&mut self) -> u64 {
        self.size
    }
    fn read(&mut self, off: u64, bytes: &mut [u8]) {
        if self.writer {
            self.ss.direct_read(off, bytes);
        } else {
            self.ss.read(self.time, off, bytes);
        }
    }
    fn write(&mut self, off: u64, bytes: &[u8]) {
        debug_assert!(self.writer);
        self.ss.write(off, bytes);
    }
    fn commit(&mut self, size: u64) {
        debug_assert!(self.writer);
        self.ss.commit(size);
    }
}

impl Drop for VersionStorage {
    fn drop(&mut self) {
        if !self.writer {
            self.ss.end_read(self.time);
        }
    }
}

use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// Simple implementation of Storage using std::fs::File.
pub struct SimpleFileStorage {
    pub file: fs::File,
}

impl SimpleFileStorage {
    pub fn new(filename: &str) -> Self {
        Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(filename)
                .unwrap(),
        }
    }
}

impl Storage for SimpleFileStorage {
    fn read(&mut self, off: u64, bytes: &mut [u8]) {
        self.file.seek(SeekFrom::Start(off)).unwrap();
        let _x = self.file.read_exact(bytes);
    }
    fn write(&mut self, off: u64, bytes: &[u8]) {
        self.file.seek(SeekFrom::Start(off)).unwrap();
        let _x = self.file.write(bytes);
    }
    fn size(&mut self) -> u64 {
        self.file.seek(SeekFrom::End(0)).unwrap()
    }
    fn commit(&mut self, size: u64) {
        self.file.set_len(size).unwrap();
    }
}
