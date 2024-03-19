use crate::{Arc, Data, Mutex};

/// Interface for database storage.
pub trait Storage: Send + Sync {
    /// Get the size of the underlying storage.
    /// Note : this is valid initially and after a commit but is not defined after write is called.
    fn size(&self) -> u64;

    /// Read data from storage.
    fn read(&self, start: u64, data: &mut [u8]);

    /// Write byte slice to storage.
    fn write(&mut self, start: u64, data: &[u8]);

    /// Write byte Vec to storage.
    fn write_vec(&mut self, start: u64, data: Vec<u8>) {
        let len = data.len();
        let d = Arc::new(data);
        self.write_data(start, d, 0, len);
    }

    /// Write Data slice to storage.
    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.write(start, &data[off..off + len]);
    }

    /// Finish write transaction, size is new size of underlying storage.
    fn commit(&mut self, size: u64);

    /// Write u64 to storage.
    fn write_u64(&mut self, start: u64, value: u64) {
        self.write(start, &value.to_le_bytes());
    }

    /// Read u64 from storage.
    fn read_u64(&self, start: u64) -> u64 {
        let mut bytes = [0; 8];
        self.read(start, &mut bytes);
        u64::from_le_bytes(bytes)
    }

    /// Clone.
    fn clone(&self) -> Box<dyn Storage> {
        panic!()
    }

    /// Wait until current writes are complete.
    fn wait_complete(&self) {}

    /// Reset buffer
    fn reset(&mut self) {
        panic!()
    }
}

/// Interface for page storage.
pub trait PageStorage: Send + Sync {
    /// Is the underlying storage new?
    fn is_new(&self) -> bool;
    /// Information about page sizes.
    fn info(&self) -> Box<dyn PageStorageInfo>;
    /// Make a new page, result is page number.
    fn new_page(&mut self) -> u64;
    /// Drop page number.
    fn drop_page(&mut self, pn: u64);
    /// Set contents of page.
    fn set_page(&mut self, pn: u64, data: Data);
    /// Get contents of page.
    fn get_page(&self, pn: u64) -> Data;
    /// Get page size (for repacking).
    fn size(&self, pn: u64) -> usize;
    /// Save pages to underlying storage.
    fn save(&mut self);
    /// Undo changes since last save ( but set_page/renumber cannot be undone, only new_page and drop_page can be undone ).
    fn rollback(&mut self);
    /// Wait until save is complete.
    fn wait_complete(&self);
    #[cfg(feature = "verify")]
    /// Get set of free pages and number of pages ever allocated ( for VERIFY builtin function ).
    fn get_free(&mut self) -> (crate::HashSet<u64>, u64);
    #[cfg(feature = "renumber")]
    /// Renumber page.
    fn renumber(&mut self, pn: u64) -> u64;
    #[cfg(feature = "renumber")]
    /// Load free pages in preparation for page renumbering. Returns number of used pages or None if there are no free pages.
    fn load_free_pages(&mut self) -> Option<u64>;
    #[cfg(feature = "renumber")]
    /// Final part of page renumber operation.
    fn set_alloc_pn(&mut self, target: u64);
}

/// Information about page sizes.
pub trait PageStorageInfo: Send + Sync {
    /// Number of different page sizes.
    fn sizes(&self) -> usize;
    /// Size index for given page size.
    fn index(&self, size: usize) -> usize;
    /// Page size for ix ( 1-based ix must be <= sizes() ).
    fn size(&self, ix: usize) -> usize;
    /// Maximum size page.
    fn max_size_page(&self) -> usize {
        self.size(self.sizes())
    }
    /// Half size page.
    fn half_size_page(&self) -> usize {
        self.size(self.index(self.max_size_page() / 2 - 50))
    }
    /// Is it worth compressing a page of given size by saving.
    fn compress(&self, size: usize, saving: usize) -> bool {
        self.index(size - saving) < self.index(size)
    }
}

/// Simple implementation of [Storage] using `Vec<u8>`.
#[derive(Default)]
pub struct MemFile {
    v: Arc<Mutex<Vec<u8>>>,
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

    fn write(&mut self, off: u64, bytes: &[u8]) {
        let off = off as usize;
        let len = bytes.len();
        let mut v = self.v.lock().unwrap();
        if off + len > v.len() {
            v.resize(off + len, 0);
        }
        v[off..off + len].copy_from_slice(bytes);
    }

    fn commit(&mut self, size: u64) {
        let mut v = self.v.lock().unwrap();
        v.resize(size as usize, 0);
    }

    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self { v: self.v.clone() })
    }
}

use std::{fs, fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write};

/// Simple implementation of [Storage] using `std::fs::File`.
pub struct SimpleFileStorage {
    file: Arc<Mutex<fs::File>>,
}

impl SimpleFileStorage {
    /// Construct from filename.
    pub fn new(filename: &str) -> Box<Self> {
        Box::new(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(filename)
                    .unwrap(),
            )),
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
        let _ = f.read(bytes).unwrap();
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        let mut f = self.file.lock().unwrap();
        // The list of operating systems which auto-zero is likely more than this...research is todo.
        #[cfg(not(any(target_os = "windows", target_os = "linux")))]
        {
            let size = f.seek(SeekFrom::End(0)).unwrap();
            if off > size {
                f.set_len(off).unwrap();
                /*
                let len = (off - size) as usize;
                let zb = vec![0; len];
                f.seek(SeekFrom::Start(size)).unwrap();
                let _ = f.write(&zb).unwrap();
                */
            }
        }
        f.seek(SeekFrom::Start(off)).unwrap();
        let _ = f.write(bytes).unwrap();
    }

    fn commit(&mut self, size: u64) {
        let f = self.file.lock().unwrap();
        f.set_len(size).unwrap();
        f.sync_all().unwrap();
    }

    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self {
            file: self.file.clone(),
        })
    }
}

/// Alternative to SimpleFileStorage that uses multiple [SimpleFileStorage]s to allow parallel reads/writes by different threads.
#[allow(clippy::vec_box)]
pub struct MultiFileStorage {
    filename: String,
    files: Arc<Mutex<Vec<Box<SimpleFileStorage>>>>,
}

impl MultiFileStorage {
    /// Create new MultiFileStorage.
    pub fn new(filename: &str) -> Box<Self> {
        Box::new(Self {
            filename: filename.to_string(),
            files: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn get_file(&self) -> Box<SimpleFileStorage> {
        if let Some(f) = self.files.lock().unwrap().pop() {
            f
        } else {
            SimpleFileStorage::new(&self.filename)
        }
    }

    fn put_file(&self, f: Box<SimpleFileStorage>) {
        self.files.lock().unwrap().push(f);
    }
}

impl Storage for MultiFileStorage {
    fn size(&self) -> u64 {
        let f = self.get_file();
        let result = f.size();
        self.put_file(f);
        result
    }

    fn read(&self, off: u64, bytes: &mut [u8]) {
        let f = self.get_file();
        f.read(off, bytes);
        self.put_file(f);
    }

    fn write(&mut self, off: u64, bytes: &[u8]) {
        let mut f = self.get_file();
        f.write(off, bytes);
        self.put_file(f);
    }

    fn commit(&mut self, size: u64) {
        let mut f = self.get_file();
        f.commit(size);
        self.put_file(f);
    }

    fn clone(&self) -> Box<dyn Storage> {
        Box::new(Self {
            filename: self.filename.clone(),
            files: self.files.clone(),
        })
    }
}

/// Dummy Stg that can be used for Atomic upd file if "reliable" atomic commits are not required.
pub struct DummyFile {}
impl DummyFile {
    /// Construct.
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Storage for DummyFile {
    fn size(&self) -> u64 {
        0
    }

    fn read(&self, _off: u64, _bytes: &mut [u8]) {}

    fn write(&mut self, _off: u64, _bytes: &[u8]) {}

    fn commit(&mut self, _size: u64) {}

    fn clone(&self) -> Box<dyn Storage> {
        Self::new()
    }
}
