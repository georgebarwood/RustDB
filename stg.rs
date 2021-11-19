/// Interface for database storage.
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
