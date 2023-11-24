use crate::{
    nd, Arc, BTreeMap, CompactFile, Data, HashMap, HashSet, Mutex, RwLock, SaveOp, Storage,
};

/// ```Arc<PageInfo>```
pub type PageInfoPtr = Arc<PageInfo>;

/// Page data and usage information.
pub struct PageInfo {
    /// Page Data
    pub d: Mutex<PageData>,
    /// Page Usage
    pub u: Mutex<u64>,
}

impl PageInfo {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            d: Mutex::new(PageData {
                current: None,
                history: BTreeMap::new(),
            }),
            u: Mutex::new(0),
        })
    }
    fn inc_usage(self: &Arc<Self>) -> u64 {
        let mut pu = self.u.lock().unwrap();
        *pu += 1;
        *pu
    }
}

/// Data for a logical page, including historic data.
pub struct PageData {
    /// Current data for the page.
    pub current: Option<Data>,
    /// Historic data for the page. Has data for page at specified time.
    /// A copy is made prior to an update, so get looks forward from access time.
    pub history: BTreeMap<u64, Data>,
}

impl PageData {
    /// Get the Data for the page, checking history if not a writer.
    /// Reads Data from file if necessary.
    /// Result is Data and flag indicating whether data was read from file.
    fn get_data(&mut self, lpnum: u64, a: &AccessPagedData) -> (Data, bool) {
        if !a.writer {
            if let Some((_k, v)) = self.history.range(a.time..).next() {
                return (v.clone(), false);
            }
        }

        if let Some(p) = &self.current {
            return (p.clone(), false);
        }

        // Get data from file.
        let file = a.spd.file.read().unwrap();
        let data = file.get_page(lpnum);
        self.current = Some(data.clone());
        (data, true)
    }

    /// Set the page data, updating the history using the specified time and current data.
    /// result is size of previously loaded data.
    fn set_data(&mut self, time: u64, data: Data, do_history: bool) -> usize {
        let mut result = 0;
        if let Some(old) = self.current.take() {
            result = old.len();
            if do_history {
                self.history.insert(time, old);
            }
        }
        self.current = Some(data);
        result
    }

    /// Trim entry for time t that no longer need to be retained, returning whether entry was retained.
    /// start is start of range for which no readers exist.
    fn trim(&mut self, t: u64, start: u64) -> bool {
        let first = self.history_start(t);
        if first >= start {
            // There is no reader that can read copy for time t, so copy can be removed.
            self.history.remove(&t);
            false
        } else {
            true
        }
    }

    /// Returns the earliest time that would return the page for the specified time.
    fn history_start(&self, t: u64) -> u64 {
        if let Some((k, _)) = self.history.range(..t).next_back() {
            *k + 1
        } else {
            0
        }
    }
}

/// Central store of data.
#[derive(Default)]
pub struct Stash {
    /// Write time - number of writes.
    pub time: u64,
    /// Page number -> page info.
    pub pages: HashMap<u64, PageInfoPtr>,
    /// Time -> reader count. Number of readers for given time.
    pub rdrs: BTreeMap<u64, usize>,
    /// Time -> set of page numbers. Page copies held for given time.
    pub vers: BTreeMap<u64, HashSet<u64>>,
    /// Total size of current pages.
    pub total: usize,
    /// trim_cache reduces total to mem_limit (or below).
    pub mem_limit: usize,
    /// Tracks loaded page with smallest usage.
    pub min: Min,
    /// Total number of page accesses.
    pub read: u64,
    /// Total number of misses ( data was not already loaded ).
    pub miss: u64,
}

impl Stash {
    /// Set the value of the specified page for the current time.
    fn set(&mut self, lpnum: u64, data: Data, apd: &AccessPagedData) {
        let time = self.time;
        let u = self.vers.entry(time).or_default();
        let do_history = u.insert(lpnum);
        let p = self
            .pages
            .entry(lpnum)
            .or_insert_with(PageInfo::new)
            .clone();
        self.min.set(lpnum, p.inc_usage());
        self.total += data.len();
        let mut pd = p.d.lock().unwrap();
        // Make sure page is in cache ( since trimming could mean it has been discarded ).
        let (old, loaded) = pd.get_data(lpnum, apd);
        if loaded {
            self.total += old.len();
        }
        self.total -= pd.set_data(time, data, do_history);
    }

    /// Get the PageInfoPtr for the specified page and note the page as used.
    fn get_pinfo(&mut self, lpnum: u64) -> PageInfoPtr {
        let p = self
            .pages
            .entry(lpnum)
            .or_insert_with(PageInfo::new)
            .clone();
        self.min.set(lpnum, p.inc_usage());
        self.read += 1;
        p
    }

    /// Register that there is a client reading the database. The result is the current time.
    fn begin_read(&mut self) -> u64 {
        let time = self.time;
        let n = self.rdrs.entry(time).or_insert(0);
        *n += 1;
        time
    }

    /// Register that the read at the specified time has ended. Stashed pages may be freed.
    fn end_read(&mut self, time: u64) {
        let n = self.rdrs.get_mut(&time).unwrap();
        *n -= 1;
        if *n == 0 {
            self.rdrs.remove(&time);
            self.trim(time);
        }
    }

    /// Register that an update operation has completed. Time is incremented.
    /// Stashed pages may be freed. Returns number of pages updated.
    fn end_write(&mut self) -> usize {
        let result = if let Some(u) = self.vers.get(&self.time) {
            u.len()
        } else {
            0
        };
        let t = self.time;
        self.time = t + 1;
        self.trim(t);
        result
    }

    /// Trim historic data that is no longer required.
    fn trim(&mut self, time: u64) {
        let (s, r) = (self.start(time), self.retain(time));
        if s != r {
            let mut empty = Vec::<u64>::new();
            for (t, pl) in self.vers.range_mut(s..r) {
                pl.retain(|pnum| {
                    let p = self.pages.get(pnum).unwrap();
                    let mut p = p.d.lock().unwrap();
                    p.trim(*t, s)
                });
                if pl.is_empty() {
                    empty.push(*t);
                }
            }
            for t in empty {
                self.vers.remove(&t);
            }
        }
    }

    /// Calculate the start of the range of times for which there are no readers.
    fn start(&self, time: u64) -> u64 {
        if let Some((t, _n)) = self.rdrs.range(..time).next_back() {
            1 + *t
        } else {
            0
        }
    }

    /// Calculate the end of the range of times for which there are no readers.
    fn retain(&self, time: u64) -> u64 {
        if let Some((t, _n)) = self.rdrs.range(time..).next() {
            *t
        } else {
            self.time
        }
    }

    /// Increase total memory used by stash.
    fn more(&mut self, amount: usize) {
        self.miss += 1;
        self.total += amount;
        self.trim_cache();
    }

    /// Trim cached data ( to reduce memory usage ).
    fn trim_cache(&mut self) {
        while self.total >= self.mem_limit {
            if let Some(lpnum) = self.min.pop() {
                let p = self.pages.get(&lpnum).unwrap();
                let mut d = p.d.lock().unwrap();
                let mut freed = 0;
                if let Some(data) = &d.current {
                    freed = data.len();
                    d.current = None;
                }
                self.total -= freed;
            } else {
                break;
            }
        }
    }

    /// Return the number of pages currently cached.
    pub fn cached(&self) -> usize {
        self.min.v.len()
    }
}

/// Allows logical database pages to be shared to allow concurrent readers.
pub struct SharedPagedData {
    /// Underlying file.
    pub file: RwLock<CompactFile>,
    /// Starter page size.
    pub sp_size: usize,
    /// Extension page size.
    pub ep_size: usize,
    /// Stash of pages.
    pub stash: Mutex<Stash>,
}

/// =1024. Size of an extension page.
const EP_SIZE: usize = 1024;
/// =16. Maximum number of extension pages.
const EP_MAX: usize = 16;
/// =136. Starter page size.
const SP_SIZE: usize = (EP_MAX + 1) * 8;

impl SharedPagedData {
    /// Construct SharedPageData based on specified underlying storage.
    pub fn new(file: Box<dyn Storage>) -> Self {
        let file = CompactFile::new(file, SP_SIZE, EP_SIZE);
        // Note : if it's not a new file, sp_size and ep_size are read from file header.
        let sp_size = file.sp_size;
        let ep_size = file.ep_size;
        // Set a default stash memory limit of 10 MB.
        let stash = Stash {
            mem_limit: 10 * 1024 * 1024,
            ..Default::default()
        };
        Self {
            stash: Mutex::new(stash),
            file: RwLock::new(file),
            sp_size,
            ep_size,
        }
    }

    /// Calculate the maximum size of a logical page. This value is stored in the Database struct.
    pub fn page_size_max(&self) -> usize {
        let ep_max = (self.sp_size - 2) / 8;
        (self.ep_size - 16) * ep_max + (self.sp_size - 2)
    }

    /// Trim cache.
    pub fn trim_cache(&self) {
        self.stash.lock().unwrap().trim_cache();
    }
}

/// Access to shared paged data.
pub struct AccessPagedData {
    writer: bool,
    time: u64,
    ///
    pub spd: Arc<SharedPagedData>,
}

impl AccessPagedData {
    /// Construct access to a virtual read-only copy of the database logical pages.
    pub fn new_reader(spd: Arc<SharedPagedData>) -> Self {
        let time = spd.stash.lock().unwrap().begin_read();
        AccessPagedData {
            writer: false,
            time,
            spd,
        }
    }

    /// Construct access to the database logical pages.
    pub fn new_writer(spd: Arc<SharedPagedData>) -> Self {
        AccessPagedData {
            writer: true,
            time: 0,
            spd,
        }
    }

    /// Get the Data for the specified page.
    pub fn get_data(&self, lpnum: u64) -> Data {
        // Get page info.
        let pinfo = self.spd.stash.lock().unwrap().get_pinfo(lpnum);

        // Read the page data.
        let (data, loaded) = pinfo.d.lock().unwrap().get_data(lpnum, self);

        // If data was read from underlying file, adjust the total data stashed, and trim the stash if appropriate.
        if loaded {
            self.spd.stash.lock().unwrap().more(data.len());
        }
        data
    }

    /// Set the data of the specified page.
    pub fn set_page(&self, lpnum: u64, data: Data) {
        debug_assert!(self.writer);

        // First update the stash ( ensures any readers will not attempt to read the file ).
        self.spd
            .stash
            .lock()
            .unwrap()
            .set(lpnum, data.clone(), self);

        // Write data to underlying file.
        self.spd.file.write().unwrap().set_page(lpnum, data);
    }

    /// Is the underlying file new (so needs to be initialised ).
    pub fn is_new(&self) -> bool {
        self.writer && self.spd.file.read().unwrap().is_new()
    }

    /// Check whether compressing a page is worthwhile.
    pub fn compress(&self, size: usize, saving: usize) -> bool {
        debug_assert!(self.writer);
        CompactFile::compress(self.spd.sp_size, self.spd.ep_size, size, saving)
    }

    /// Allocate a logical page.
    pub fn alloc_page(&self) -> u64 {
        debug_assert!(self.writer);
        self.spd.file.write().unwrap().alloc_page()
    }

    /// Free a logical page.
    pub fn free_page(&self, lpnum: u64) {
        debug_assert!(self.writer);
        self.spd.stash.lock().unwrap().set(lpnum, nd(), self);
        self.spd.file.write().unwrap().free_page(lpnum);
    }

    /// Commit changes to underlying file ( or rollback logical page allocations ).
    pub fn save(&self, op: SaveOp) -> usize {
        debug_assert!(self.writer);
        match op {
            SaveOp::Save => {
                self.spd.file.write().unwrap().save();
                self.spd.stash.lock().unwrap().end_write()
            }
            SaveOp::RollBack => {
                // Note: rollback happens before any pages are updated.
                // However logical page allocations need to be rolled back.
                self.spd.file.write().unwrap().rollback();
                0
            }
        }
    }
}

impl Drop for AccessPagedData {
    fn drop(&mut self) {
        if !self.writer {
            self.spd.stash.lock().unwrap().end_read(self.time);
        }
    }
}

use std::collections::hash_map::Entry;

#[derive(Default)]
/// Used to efficiently track least used cached page.
pub struct Min {
    /// Vector of id,value pairs.
    pub v: Vec<(u64, u64)>,
    /// Position in v of given id. 
    pub pos: HashMap<u64, usize>,
}

impl Min {
    /// Sets the value associated with the specified id.
    pub fn set(&mut self, id: u64, val: u64) {
        match self.pos.entry(id) {
            Entry::Occupied(e) => {
                let i: usize = *e.get();
                self.v[i] = (id, val);
                self.check(i);
            }
            Entry::Vacant(e) => {
                let i = self.v.len();
                e.insert(i);
                self.v.push((id, val));
                self.check(i);
            }
        };
    }

    /// Adjusts position of item at specified position.
    fn check(&mut self, i: usize) {
        self.check_up(i);
        self.check_up(i * 2 + 1);
        self.check_up(i * 2 + 2);
    }

    fn check_up(&mut self, i: usize) {
        if i > 0 && i < self.v.len() {
            let pi = (i - 1) / 2;
            let pv = self.v[pi];
            let v = self.v[i];
            if v.1 < pv.1 {
                self.v[pi] = v;
                self.v[i] = pv;
                self.pos.insert(v.0, pi);
                self.pos.insert(pv.0, i);
            }
        }
    }

    /// Removes id with smallest associated value.
    pub fn pop(&mut self) -> Option<u64> {
        if self.v.is_empty() {
            return None;
        }
        let result = self.v[0].0;
        self.pos.remove(&result);
        let last = self.v.pop().unwrap();
        if !self.v.is_empty() {
            self.pos.insert(last.0, 0);
            self.v[0] = last;
            self.check(0);
        }
        Some(result)
    }
}

#[test]
pub fn test() {
    let mut h = Min::default();
    h.set(5, 10);
    h.set(8, 1);
    h.set(13, 2);
    h.set(8, 15);
    assert!(h.get().unwrap() == 13);
    assert!(h.get().unwrap() == 5);
    assert!(h.get().unwrap() == 8);
    assert!(h.get() == None);
}
