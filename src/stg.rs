use crate::{Arc, Data};

/// Interface for database storage.
pub trait Storage: Send + Sync {
    /// Get the current size of the underlying storage.
    fn size(&self) -> u64;

    /// Read from the underlying storage.
    fn read(&self, start: u64, bytes: &mut [u8]);

    /// Write to the underlying storage.
    fn write(&mut self, start: u64, bytes: &[u8]);

    /// Write Vec to underlying storage.
    fn write_vec(&mut self, start: u64, data: Vec<u8>) {
        let len = data.len();
        let d = Arc::new(data);
        self.write_data(start, d, 0, len);
    }

    /// Write Data slice to the underlying storage.
    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.write(start, &data[off..off + len]);
    }

    /// Finish write transaction, size is new size of underlying storage.
    fn commit(&mut self, size: u64);
}

use crate::Mutex;
use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// Simple implementation of Storage using std::fs::File.
pub struct SimpleFileStorage {
    pub file: Mutex<fs::File>,
}

impl SimpleFileStorage {
    pub fn new(filename: &str) -> Self {
        Self {
            file: Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(filename)
                    .unwrap(),
            ),
        }
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
    fn write(&mut self, off: u64, bytes: &[u8]) {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        let _x = f.write(bytes);
    }
    fn commit(&mut self, size: u64) {
        let f = self.file.lock().unwrap();
        f.set_len(size).unwrap();
    }
}
