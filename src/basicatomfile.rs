use crate::{buf::WriteBuffer, wmap::DataSlice, wmap::WMap, Arc, Data, Storage};

/// Alternative to AtomicFile.
pub struct BasicAtomicFile {
    /// The main underlying storage.
    pub stg: WriteBuffer,
    /// Temporary storage for updates during commit.
    pub upd: WriteBuffer,
    /// Map of writes. Note the key is the file address of the last byte written.
    pub map: WMap,
    ///
    list: Vec<(u64, DataSlice)>,
    ///
    size: u64,
}

impl BasicAtomicFile {
    /// stg is the main underlying storage, upd is temporary storage for updates during commit.
    pub fn new(stg: Box<dyn Storage>, upd: Box<dyn Storage>) -> Box<Self> {
        let size = stg.size();
        let mut result = Box::new(Self {
            map: WMap::default(),
            list: Vec::new(),
            stg: WriteBuffer::new(stg),
            upd: WriteBuffer::new(upd),
            size,
        });
        result.init();
        result
    }

    /// Apply outstanding updates.
    fn init(&mut self) {
        let end = self.upd.stg.read_u64(0);
        let size = self.upd.stg.read_u64(8);
        if end == 0 {
            return;
        }
        assert!(end == self.upd.stg.size());
        let mut pos = 16;
        while pos < end {
            let start = self.upd.stg.read_u64(pos);
            pos += 8;
            let len = self.upd.stg.read_u64(pos);
            pos += 8;
            let mut buf = vec![0; len as usize];
            self.upd.stg.read(pos, &mut buf);
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
                /*
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
                */
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
            let mut stg_written = false;
            let mut pos: u64 = 16;
            for (start, v) in self.list.iter() {
                let len = v.len as u64;
                let start = *start;
                let data = &v.data[v.off..v.off + v.len];
                if start >= self.size {
                    // Writes beyond current stg size can be written directly.
                    stg_written = true;
                    self.stg.write(start, data);
                } else {
                    self.upd.write_u64(pos, start);
                    pos += 8;
                    self.upd.write_u64(pos, len);
                    pos += 8;
                    self.upd.write(pos, data);
                    pos += len;
                }
            }
            if stg_written {
                self.stg.commit(size);
            }
            self.upd.commit(pos); // Not clear if this is necessary.

            // Set the end position.
            self.upd.write_u64(0, pos);
            self.upd.write_u64(8, size);
            self.upd.commit(pos);
        } else {
            for (start, v) in self.list.iter() {
                let start = *start;
                if start < self.size {
                    // Writes beyond current stg size have already been written.
                    self.stg.write(start, &v.data[v.off..v.off + v.len]);
                }
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
        self.size = size;
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, start: u64, data: &mut [u8]) {
        self.map.read(start, data, &*self.stg.stg);
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
