use crate::{util, wmap::DataSlice, wmap::WMap, Arc, Data, RwLock, Storage};

/// AtomicFile makes sure that database updates are all-or-nothing.
/// Keeps a map of outstanding writes which have not yet been written to the underlying file.
pub struct AtomicFile {
    /// Map of existing outstanding writes. Note the key is the file address of the last byte written.
    map: WMap,
    cf: Arc<RwLock<CommitFile>>,
    size: u64,
    tx: std::sync::mpsc::Sender<(u64, WMap)>,
    stopping: bool,
}

impl AtomicFile {
    /// Construct a new AtomicFle. stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn Storage>, upd: Box<dyn Storage>) -> Box<Self> {
        let size = stg.size();
        let mut baf = BasicAtomicFile::new(stg.clone(), upd);
        let (tx, rx) = std::sync::mpsc::channel::<(u64, WMap)>();
        let cf1 = Arc::new(RwLock::new(CommitFile {
            stg,
            map: WMap::default(),
            todo: 0,
            done: 0,
            waiting_client: None,
        }));
        let cf = cf1.clone();
        std::thread::spawn(move || {
            while let Ok((size, map)) = rx.recv() {
                baf.map = map;
                baf.commit(size);
                cf1.write().unwrap().done_one();
            }
        });
        Box::new(Self {
            map: WMap::default(),
            cf,
            size,
            tx,
            stopping: false,
        })
    }
}

impl Storage for AtomicFile {
    fn commit(&mut self, size: u64) {
        self.size = size;
        if self.map.map.is_empty() {
            return;
        }
        while {
            let cf = &mut self.cf.write().unwrap();
            // If the CommitFile map has got "large" wait for the commit process to finish (so the map is reset).
            if !self.stopping && cf.wait(3000) {
                true
            } else {
                let map = std::mem::take(&mut self.map);
                cf.todo += 1;
                for (k, v) in map.map.iter() {
                    let start = k + 1 - v.len as u64;
                    cf.write_data(start, v.data.clone(), v.off, v.len);
                }
                self.tx.send((size, map)).unwrap();
                false
            }
        } {
            std::thread::park();
        }
        if self.stopping {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.stopping = true;
        while self.cf.write().unwrap().wait(0) {
            std::thread::park();
        }
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &*self.cf.read().unwrap());
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.map.write(start, data, off, len);
    }

    fn write(&mut self, start: u64, data: &[u8]) {
        let len = data.len();
        let d = Arc::new(data.to_vec());
        self.write_data(start, d, 0, len);
    }

    fn complete(&self, done: u64) -> (bool, u64) {
        self.cf.read().unwrap().complete(done)
    }
}

struct CommitFile {
    stg: Box<dyn Storage>,
    map: WMap,
    todo: usize,
    done: u64,
    waiting_client: Option<std::thread::Thread>,
}

impl CommitFile {
    fn complete(&self, done: u64) -> (bool, u64) {
        let result = self.todo == 0 || self.done > done;
        (result, self.done)
    }

    fn done_one(&mut self) {
        self.todo -= 1;
        if self.todo == 0 {
            self.done += 1;
            self.map = WMap::default();
            if let Some(client) = std::mem::take(&mut self.waiting_client) {
                client.unpark();
            }
        }
    }

    fn wait(&mut self, n: usize) -> bool {
        let len = self.map.map.len();
        if len > n {
            // println!("client waiting n={} len={} todo={}", n, len, self.todo );
            self.waiting_client = Some(std::thread::current());
            true
        } else {
            false
        }
    }
}

impl Storage for CommitFile {
    fn commit(&mut self, _size: u64) {
        panic!()
    }

    fn size(&self) -> u64 {
        panic!()
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &*self.stg);
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.map.write(start, data, off, len);
    }

    fn write(&mut self, _start: u64, _data: &[u8]) {
        panic!()
    }
}

/// Non-buffered alternative to AtomicFile.
pub struct BasicAtomicFile {
    /// The main underlying storage.
    pub stg: Box<dyn Storage>,
    /// Temporary storage for updates during commit.
    pub upd: Box<dyn Storage>,
    /// Map of writes. Note the key is the file address of the last byte written.
    map: WMap,
    ///
    list: Vec<(u64, DataSlice)>,
}

impl BasicAtomicFile {
    /// stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn Storage>, upd: Box<dyn Storage>) -> Box<Self> {
        let mut result = Self {
            map: WMap::default(),
            list: Vec::new(),
            stg,
            upd,
        };
        result.init();
        Box::new(result)
    }

    /// Apply outstanding updates.
    fn init(&mut self) {
        let end = self.upd.read_u64(0);
        let size = self.upd.read_u64(8);
        if end == 0 {
            return;
        }
        assert!(end == self.upd.size());
        let mut pos = 16;
        while pos < end {
            let start = self.upd.read_u64(pos);
            pos += 8;
            let len = self.upd.read_u64(pos);
            pos += 8;
            let mut buf = vec![0; len as usize];
            self.upd.read(pos, &mut buf);
            pos += len;
            self.stg.write(start, &buf);
        }
        self.stg.commit(size);
        self.upd.commit(0);
    }

    /// Perform the specified phase ( 1 or 2 ) of a two-phase commit.
    pub fn commit_phase(&mut self, size: u64, phase: u8) {
        if self.map.map.is_empty() && self.list.is_empty() {
            return;
        }
        if phase == 1 {
            /* Get list of updates, compare with old data to reduce the size of upd file */
            if false {
                let mut buf = Vec::new();
                for (k, v) in self.map.map.iter() {
                    let start = k + 1 - v.len as u64;
                    let len = v.len;
                    if buf.len() < len {
                        buf.resize(len, 0);
                    }
                    self.stg.read(start, &mut buf[0..len]);
                    util::diff(&v.data[v.off..v.off + len], &buf, 17, |off, len| {
                        self.list.push((
                            start + off as u64,
                            DataSlice {
                                off: v.off + off,
                                len,
                                data: v.data.clone(),
                            },
                        ));
                    });
                }
            } else {
                for (k, v) in self.map.map.iter() {
                    let start = k + 1 - v.len as u64;
                    let len = v.len;
                    self.list.push((
                        start,
                        DataSlice {
                            off: v.off,
                            len,
                            data: v.data.clone(),
                        },
                    ));
                }
            }
            // println!("Commit # writes={}", self.list.len());

            self.map.map.clear();

            // Write the updates to upd.
            // First set the end position to zero.
            self.upd.write_u64(0, 0);
            self.upd.write_u64(8, size);
            self.upd.commit(16); // Not clear if this is necessary.

            // Write the update records.
            let mut pos: u64 = 16;
            for (start, v) in self.list.iter() {
                let len = v.len as u64;
                self.upd.write_u64(pos, *start);
                pos += 8;
                self.upd.write_u64(pos, len);
                pos += 8;
                self.upd.write(pos, &v.data[v.off..v.off + v.len]);
                pos += len;
            }
            self.upd.commit(pos); // Not clear if this is necessary.

            // Set the end position.
            self.upd.write_u64(0, pos);
            self.upd.write_u64(8, size);
            self.upd.commit(pos);
        } else {
            for (start, v) in self.list.iter() {
                self.stg.write(*start, &v.data[v.off..v.off + v.len]);
            }
            self.list.clear();
            self.stg.commit(size);
            self.upd.commit(0);
        }
    }
}

impl Storage for BasicAtomicFile {
    fn commit(&mut self, size: u64) {
        self.commit_phase(size, 1);
        self.commit_phase(size, 2);
    }

    fn size(&self) -> u64 {
        self.stg.size()
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &*self.stg);
    }

    fn write_data(&mut self, start: u64, data: Data, off: usize, len: usize) {
        self.map.write(start, data, off, len);
    }

    fn write(&mut self, start: u64, data: &[u8]) {
        let len = data.len();
        let d = Arc::new(data.to_vec());
        self.write_data(start, d, 0, len);
    }
}

#[test]
pub fn test() {
    use crate::stg::MemFile;
    use rand::Rng;
    /* Idea of test is to check AtomicFile and MemFile behave the same */

    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        let mut s1 = AtomicFile::new(MemFile::new(), MemFile::new());
        let mut s2 = MemFile::new();

        for _ in 0..1000 {
            let off: usize = rng.gen::<usize>() % 100;
            let mut len = 1 + rng.gen::<usize>() % 20;
            let w: bool = rng.gen();
            if w {
                let mut bytes = Vec::new();
                while len > 0 {
                    len -= 1;
                    let b: u8 = rng.gen::<u8>();
                    bytes.push(b);
                }
                s1.write(off as u64, &bytes);
                s2.write(off as u64, &bytes);
            } else {
                let mut b2 = vec![0; len];
                let mut b3 = vec![0; len];
                s1.read(off as u64, &mut b2);
                s2.read(off as u64, &mut b3);
                assert!(b2 == b3);
            }
        }
    }
}
