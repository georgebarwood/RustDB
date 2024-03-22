use crate::{
    buf::ReadBufStg, wmap::WMap, Arc, BasicAtomicFile, Data, Limits, Mutex, RwLock, Storage,
};

/// Based on [BasicAtomicFile] which makes sure that database updates are all-or-nothing.
/// Provides read buffering for small reads, and a thread to perform commit asyncronously.
pub struct AtomicFile {
    map: WMap,
    cf: Arc<RwLock<CommitFile>>,
    size: u64,
    tx: std::sync::mpsc::Sender<(u64, WMap)>,
    busy: Arc<Mutex<()>>,
    map_lim: usize,
}

impl AtomicFile {
    /// Construct AtomicFile with default limits. stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn Storage>, upd: Box<dyn Storage>) -> Box<Self> {
        Self::new_with_limits(stg, upd, &Limits::default())
    }

    /// Construct Atomic file with specified limits.
    pub fn new_with_limits(
        stg: Box<dyn Storage>,
        upd: Box<dyn Storage>,
        lim: &Limits,
    ) -> Box<Self> {
        let size = stg.size();
        let mut baf = BasicAtomicFile::new(stg.clone(), upd, lim);
        let (tx, rx) = std::sync::mpsc::channel::<(u64, WMap)>();
        let cf = Arc::new(RwLock::new(CommitFile::new(stg, lim.rbuf_mem)));
        let busy = Arc::new(Mutex::new(())); // Lock held while async save thread is active.

        // Start the thread which does save asyncronously.
        let (cf1, busy1) = (cf.clone(), busy.clone());
        std::thread::spawn(move || {
            while let Ok((size, map)) = rx.recv() {
                let _lock = busy1.lock();
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
            busy,
            map_lim: lim.map_lim,
        })
    }
}

impl Storage for AtomicFile {
    fn commit(&mut self, size: u64) {
        self.size = size;
        if self.map.is_empty() {
            return;
        }
        if self.cf.read().unwrap().map.len() > self.map_lim {
            self.wait_complete();
        }
        let map = std::mem::take(&mut self.map);
        let cf = &mut *self.cf.write().unwrap();
        cf.todo += 1;
        map.to_storage(cf);
        self.tx.send((size, map)).unwrap();
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

    fn wait_complete(&self) {
        while self.cf.read().unwrap().todo != 0 {
            #[cfg(feature = "log")]
            println!("AtomicFile::wait_complete - waiting for writer process");
            let _x = self.busy.lock();
        }
    }
}

struct CommitFile {
    stg: Box<dyn Storage>,
    map: WMap,
    todo: usize,
}

impl CommitFile {
    fn new(stg: Box<dyn Storage>, buf_mem: usize) -> Self {
        Self {
            stg: ReadBufStg::<256>::new(stg, 50, buf_mem / 256),
            map: WMap::default(),
            todo: 0,
        }
    }

    fn done_one(&mut self) {
        self.todo -= 1;
        if self.todo == 0 {
            self.map = WMap::default();
            self.stg.reset();
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
