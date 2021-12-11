use crate::{Arc, BTreeMap, Data, Mutex, Storage};
use std::cmp::min;

/// Slice of Data to be written to storage.
pub struct DataSlice {
    pub off: usize,
    pub len: usize,
    pub data: Data,
}

/// AtomicFile makes sure that database updates are all-or-nothing.
/// Keeps a map of outstanding writes which have not yet been written to the underlying file.
pub struct AtomicFile {
    /// Map of existing outstanding writes. Note the key is the file address of the last byte written.
    pub map: Mutex<BTreeMap<u64, DataSlice>>,
    /// The main underlying storage.
    pub stg: Box<dyn Storage>,
    /// Temporary storage for updates during commit.
    pub upd: Box<dyn Storage>,
}

impl AtomicFile {
    /// Construct a new AtomicFle. stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn Storage>, upd: Box<dyn Storage>) -> Self {
        let result = Self {
            map: Mutex::new(BTreeMap::new()),
            stg,
            upd,
        };
        result.init();
        result
    }

    /// Apply outstanding updates.
    fn init(&self) {
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
}

impl Storage for AtomicFile {
    fn commit(&self, size: u64) {
        let mut map = self.map.lock().unwrap();
        if map.is_empty() {
            return;
        }

        // Write the updates to upd.
        // First set the end position to zero.
        self.upd.write_u64(0, 0);
        self.upd.write_u64(8, size);
        self.upd.commit(16); // Not clear if this is necessary.

        // Write the update records.
        let mut pos: u64 = 16;
        for (k, v) in map.iter() {
            let start = k + 1 - v.len as u64;
            let len = v.len as u64;
            self.upd.write_u64(pos, start);
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

        // Hopefully updates are now securely stored in upd file.

        for (k, v) in map.iter() {
            let start = k + 1 - v.len as u64;
            self.stg.write(start, &v.data[v.off..v.off + v.len]);
        }
        map.clear();
        self.stg.commit(size);
        self.upd.commit(0);
    }

    fn size(&self) -> u64 {
        self.stg.size()
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        let mut todo: usize = data.len();
        if todo == 0 {
            return;
        }
        let mut done: usize = 0;

        let map = self.map.lock().unwrap();
        for (&k, v) in map.range(start..) {
            let estart = k + 1 - v.len as u64;
            if estart > start + done as u64 {
                let lim = (estart - (start + done as u64)) as usize;
                let amount = min(todo, lim) as usize;
                self.stg
                    .read(start + done as u64, &mut data[done..done + amount]);
                done += amount;
                todo -= amount;
            }
            if estart > start + data.len() as u64 {
                break;
            } else {
                let skip = (start + done as u64 - estart) as usize;
                let amount = min(todo, v.len - skip);
                data[done..done + amount]
                    .copy_from_slice(&v.data[v.off + skip..v.off + skip + amount]);
                done += amount;
                todo -= amount;
            }
            if todo == 0 {
                break;
            }
        }
        if todo > 0 {
            self.stg
                .read(start + done as u64, &mut data[done..done + todo]);
        }
    }

    fn write_data(&self, start: u64, data: Data, off: usize, len: usize) {
        if len == 0 {
            return;
        }

        // Existing writes which overlap with new write need to be trimmed or removed.
        let mut remove = Vec::new();
        let mut add = Vec::new();
        let end = start + len as u64;

        let mut map = self.map.lock().unwrap();
        for (&k, v) in map.range_mut(start..) {
            let eend = k + 1; // end of existing write.
            let estart = eend - v.len as u64; // start of existing write.

            // (a) New write ends before existing write.
            if end <= estart {
                break;
            }
            // (b) New write subsumes existing write entirely, remove existing write.
            else if start <= estart && end >= eend {
                remove.push(eend - 1);
            }
            // (c) New write starts before existing write, but doesn't subsume it. Trim existing write.
            else if start <= estart {
                let trim = (end - estart) as usize;
                v.len -= trim;
                v.off += trim;
            }
            // (d) New write starts in middle of existing write, ends before end of existing write...
            // .. put start of existing write in add list, trim existing write.
            else if start > estart && end < eend {
                let remain = (start - estart) as usize;
                add.push((estart, v.data.clone(), v.off, remain));

                let trim = (end - estart) as usize;
                v.len -= trim;
                v.off += trim;
            }
            // (e) New write starts in middle of existing write, ends after existing write...
            // ... put start of existing write in add list, remove existing write,
            else {
                let remain = (start - estart) as usize;
                add.push((estart, v.data.clone(), v.off, remain));

                remove.push(eend - 1);
            }
        }
        for k in remove {
            map.remove(&k);
        }
        for (start, data, off, len) in add {
            map.insert(start + len as u64 - 1, DataSlice { data, off, len });
        }

        map.insert(start + len as u64 - 1, DataSlice { data, off, len });
    }

    fn write(&self, start: u64, data: &[u8]) {
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

    for _ in 0..1000 {
        let s0 = Box::new(MemFile::default());
        let s1 = Box::new(MemFile::default());
        let s2 = AtomicFile::new(s0, s1);
        let s3 = MemFile::default();

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
                s2.write(off as u64, &bytes);
                s3.write(off as u64, &bytes);
            } else {
                let mut b2 = vec![0; len];
                let mut b3 = vec![0; len];
                s2.read(off as u64, &mut b2);
                s3.read(off as u64, &mut b3);
                assert!(b2 == b3);
            }
        }
    }
}
