use crate::{util, Cell, Ordering, Rc, Record, SaveOp, SortedFile, DB};

/// =4. Number of fragment types.
pub const NFT: usize = 4;
/// =40. Min fragment size.
pub const MINF: usize = 40;
/// =29. Fragment increment.
pub const FI: usize = 29;

/// Total bytes used taking into account all overhead ( 3 + 1 + 8 = 12 bytes, per fragment ).
fn total(len: usize, ft: usize) -> usize {
    let bpf = FI * ft + MINF;
    let nf = (len + bpf - 1) / bpf;
    nf * (bpf + 12)
}

/// Calculate best fragment type from byte length.
pub fn fragment_type(len: usize) -> usize {
    let mut best = usize::MAX;
    let mut result = 0;
    for ft in 0..NFT {
        let t = total(len, ft);
        if t <= best {
            best = t;
            result = ft;
        }
    }
    result
}

/// Storage of variable size values.
pub struct ByteStorage {
    ///
    pub file: Rc<SortedFile>,
    id_gen: Cell<u64>,
    /// Bytes per fragment. One of 40, 69, 98, 127.
    bpf: usize,
}

impl ByteStorage {
    /// Construct new ByteStorage with specified root page and fragment type.
    pub fn new(root_page: u64, ft: usize) -> Self {
        let bpf = FI * ft + MINF;
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
                r.len = (self.bpf << 1) as u8;
                len = self.bpf
            } else {
                r.len = 1 + ((len as u8) << 1);
            }
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
            let xid = util::getu64(&p.data, off);
            debug_assert!(xid == id);
            id += 1;
            let len = p.data[off + 8] as usize;
            let off = off + 9;
            result.extend_from_slice(&p.data[off..off + (len >> 1)]);
            if len & 1 == 1 {
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
            let xid = util::getu64(&p.data, off);
            debug_assert!(xid == id + n);
            n += 1;
            let len = &p.data[off + 8];
            if len & 1 == 1 {
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
    /// Bit 0 encodes whether this is the last fragment.
    len: u8,
    bytes: Vec<u8>,
}

impl Fragment {
    pub fn new(id: u64, bpf: usize) -> Self {
        Fragment {
            id,
            len: 0,
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
        data[8] = self.len;
        let bpf = self.bytes.len();
        data[9..9 + bpf].copy_from_slice(&self.bytes[..bpf]);
    }
}
