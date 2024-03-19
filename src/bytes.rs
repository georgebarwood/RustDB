use crate::{util, Cell, Ordering, Rc, Record, SaveOp, SortedFile, DB};

/// Number of fragment types.
pub const NFT: usize = 4;

/// Total bytes used taking into account all overhead ( 3 + 1 + 8 = 12 bytes, per fragment ).
fn tot(len: usize, bpf: usize) -> usize {
    let nf = (len + bpf - 1) / bpf;
    nf * (bpf + 12)
}

/// Calculate best fragment type from byte length.
pub fn fragment_type(len: usize, bpf: &[usize]) -> usize {
    let mut best = usize::MAX;
    let mut result = 0;
    for (ft, bpf) in bpf.iter().enumerate() {
        let t = tot(len, *bpf);
        if t <= best {
            best = t;
            result = ft;
        }
    }
    result
}

/// Calculate fragment sizes.
pub fn bpf(hp: usize) -> [usize; NFT] {
    let hp = hp - 8; // 8 is to account for page header.
    let pp = hp / 1000;
    let max_bpf = hp / pp - 12;
    [40, 127, 333, max_bpf]
}

/// Storage of variable size values.
pub struct ByteStorage {
    /// File for storing fragments.
    pub file: Rc<SortedFile>,
    id_gen: Cell<u64>,
    /// Bytes per fragment.
    bpf: usize,
}

impl ByteStorage {
    /// Construct new ByteStorage with specified root page and fragment type.
    pub fn new(root_page: u64, bpf: usize) -> Self {
        let file = Rc::new(SortedFile::new(9 + bpf, 8, root_page));
        ByteStorage {
            file,
            id_gen: Cell::new(u64::MAX),
            bpf,
        }
    }

    /// Get fragment Id value.
    fn get_id(&self, db: &DB) -> u64 {
        let mut result = self.id_gen.get();
        if result == u64::MAX {
            result = 0;
            // Initialise id_gen to id of last record.
            let start = Fragment::new(u64::MAX, self.bpf);
            if let Some((pp, off)) = self.file.clone().dsc(db, Box::new(start)).next() {
                let p = pp.borrow();
                result = 1 + util::getu64(&p.data, off);
            }
            self.id_gen.set(result);
        }
        result
    }

    /// Check whether there are changes to underlying file.
    pub fn changed(&self) -> bool {
        self.file.changed()
    }

    /// Save to underlying file.
    pub fn save(&self, db: &DB, op: SaveOp) {
        self.file.save(db, op);
    }

    /// Encode bytes.
    pub fn encode(&self, db: &DB, bytes: &[u8]) -> u64 {
        let result = self.get_id(db);
        let mut r = Fragment::new(0, self.bpf);
        let n = bytes.len();
        let mut done = 0;
        loop {
            r.id = self.id_gen.get();
            self.id_gen.set(r.id + 1);
            let mut len = n - done;
            if len > self.bpf {
                r.last = false;
                len = self.bpf;
            } else {
                r.last = true;
            }
            r.len = len;
            r.bytes[..len].copy_from_slice(&bytes[done..(len + done)]);
            done += len;
            self.file.insert(db, &r);
            if done == n {
                break;
            }
        }
        result
    }

    /// Decode bytes, inline bytes are reserved.
    pub fn decode(&self, db: &DB, mut id: u64, inline: usize) -> Vec<u8> {
        let mut result = vec![0_u8; inline];
        let start = Fragment::new(id, self.bpf);
        for (pp, off) in self.file.asc(db, Box::new(start)) {
            let p = pp.borrow();
            let data = &p.data;
            debug_assert!(util::getu64(data, off) == id);
            id += 1;
            let off = off + 8;
            let (len, last) = decode(&data[off..], self.bpf);
            result.extend_from_slice(&data[off..off + len]);
            if last {
                break;
            }
        }
        result
    }

    /// Delete a code.
    pub fn delcode(&self, db: &DB, id: u64) {
        let start = Fragment::new(id, self.bpf);
        let mut n = 0;
        for (pp, off) in self.file.asc(db, Box::new(start)) {
            let p = pp.borrow();
            debug_assert!(util::getu64(&p.data, off) == id + n);
            n += 1;
            let off = off + 8;
            let (_len, last) = decode(&p.data[off..], self.bpf);
            if last {
                break;
            }
        }
        let mut r = Fragment::new(0, self.bpf);
        for xid in id..id + n {
            r.id = xid;
            self.file.remove(db, &r);
        }
    }

    /// Pack underlying file.
    #[cfg(feature = "pack")]
    pub fn repack_file(&self, db: &DB) -> i64 {
        let r = Fragment::new(0, self.bpf);
        self.file.repack(db, &r)
    }
}

/// Values are split into fragments.
struct Fragment {
    id: u64,
    len: usize,
    last: bool,
    bytes: Vec<u8>,
}

impl Fragment {
    pub fn new(id: u64, bpf: usize) -> Self {
        Fragment {
            id,
            len: 0,
            last: false,
            bytes: vec![0; bpf],
        }
    }
}

impl Record for Fragment {
    fn compare(&self, _db: &DB, data: &[u8]) -> Ordering {
        let val = util::getu64(data, 0);
        self.id.cmp(&val)
    }

    fn save(&self, data: &mut [u8]) {
        util::setu64(data, self.id);
        let bpf = self.bytes.len();
        data[8..8 + self.len].copy_from_slice(&self.bytes[..self.len]);

        // Maybe should zero unused bytes.

        let unused = bpf - self.len;
        data[8 + bpf] = (unused % 64) as u8
            + if self.last { 64 } else { 0 }
            + if unused >= 64 { 128 } else { 0 };
        if unused >= 64 {
            data[8 + bpf - 1] = (unused / 64) as u8;
        }
    }
}

/// Result is data length and last flag.
fn decode(data: &[u8], bpf: usize) -> (usize, bool) {
    let b = data[bpf];
    let unused = (b % 64) as usize
        + if b >= 128 {
            data[bpf - 1] as usize * 64
        } else {
            0
        };
    (bpf - unused, b & 64 != 0)
}
