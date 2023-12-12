use crate::{Arc, Data};

/// Interface for database storage.
pub trait Storage: Send + Sync {
    /// Get the size of the underlying storage.
    /// Note : this is valid initially and after a commit but is not defined after write is called.
    fn size(&self) -> u64;

    /// Read data from storage.
    fn read(&self, start: u64, data: &mut [u8]);

    /// Write byte slice to storage.
    fn write(&self, start: u64, data: &[u8]);

    /// Write byte Vec to storage.
    fn write_vec(&self, start: u64, data: Vec<u8>) {
        let len = data.len();
        let d = Arc::new(data);
        self.write_data(start, d, 0, len);
    }

    /// Write Data slice to storage.
    fn write_data(&self, start: u64, data: Data, off: usize, len: usize) {
        self.write(start, &data[off..off + len]);
    }

    /// Finish write transaction, size is new size of underlying storage.
    fn commit(&self, size: u64);

    /// Write u64 to storage.
    fn write_u64(&self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }

    /// Read u64 from storage.
    fn read_u64(&self, start: u64) -> u64 {
        let mut bytes = [0; 8];
        self.read(start, &mut bytes);
        u64::from_le_bytes(bytes)
    }
}

/// Simple implementation of storage using `Vec<u8>`.
#[derive(Default)]
pub struct MemFile {
    v: Mutex<Vec<u8>>,
}

impl MemFile {
    /// Get a new (boxed) MemFile.
    pub fn new() -> Box<Self> {
        Box::<Self>::default()
    }
}

impl Storage for MemFile {
    fn size(&self) -> u64 {
        let v = self.v.lock().unwrap();
        v.len() as u64
    }

    fn read(&self, off: u64, bytes: &mut [u8]) {
        let off = off as usize;
        let len = bytes.len();
        let mut v = self.v.lock().unwrap();
        if off + len > v.len() {
            v.resize(off + len, 0);
        }
        bytes.copy_from_slice(&v[off..off + len]);
    }

    fn write(&self, off: u64, bytes: &[u8]) {
        let off = off as usize;
        let len = bytes.len();
        let mut v = self.v.lock().unwrap();
        if off + len > v.len() {
            v.resize(off + len, 0);
        }
        v[off..off + len].copy_from_slice(bytes);
    }

    fn commit(&self, size: u64) {
        let mut v = self.v.lock().unwrap();
        v.resize(size as usize, 0);
    }
}

use crate::Mutex;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// Simple implementation of Storage using `std::fs::File`.
pub struct SimpleFileStorage {
    file: Mutex<fs::File>,
}

impl SimpleFileStorage {
    /// Construct from filename.
    pub fn new(filename: &str) -> Box<Self> {
        Box::new(Self {
            file: Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(filename)
                    .unwrap(),
            ),
        })
    }
}

impl Storage for SimpleFileStorage {
    fn size(&self) -> u64 {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::End(0)).unwrap()
    }

    fn read(&self, off: u64, bytes: &mut [u8]) {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        let _x = f.read_exact(bytes);
    }

    fn write(&self, off: u64, bytes: &[u8]) {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        if let Err(e) = f.write(bytes) {
            std::panic::panic_any(e);
        }
    }

    fn commit(&self, size: u64) {
        let f = self.file.lock().unwrap();
        f.set_len(size).unwrap();
        if let Err(e) = f.sync_all() {
            std::panic::panic_any(e);
        }
    }
}
