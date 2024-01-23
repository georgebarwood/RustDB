use crate::{
    heap::GHeap, nd, Arc, AtomicFile, BTreeMap, CompactFile, Data, HashMap, HashSet, Mutex, RwLock,
    SaveOp,
};

type HX = u32; // Typical 8M cache will have 1K x 8KB pages, so 10 bits is typical, 32 should be plenty.
type Heap = GHeap<u64, u64, HX>;

/// ```Arc<Mutex<PageInfo>>```
pub type PageInfoPtr = Arc<Mutex<PageInfo>>;

/// Information for a logical page, including historic data.
pub struct PageInfo {
    /// Current data for the page( None implies it is stored in underlying file ).
    pub current: Option<Data>,
    /// Historic data for the page. Has data for page at specified time.
    /// A copy is made prior to an update, so get looks forward from access time.
    pub history: BTreeMap<u64, Data>,
    /// How many times has the page been used.
    pub usage: u64,
    /// Heap index.
    pub hx: HX,
}

impl PageInfo {
    fn new() -> PageInfoPtr {
        Arc::new(Mutex::new(PageInfo {
            current: None,
            history: BTreeMap::new(),
            usage: 0,
            hx: HX::MAX,
        }))
    }

    /// Increase usage.
    fn inc_usage(&mut self, lpnum: u64, ah: &mut Heap) {
        self.usage += 1;
        if self.hx == HX::MAX {
            self.hx = ah.insert(lpnum, self.usage);
        } else {
            ah.modify(self.hx, self.usage);
        }
    }

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

    /// Set the page data, updating the history using the specified time and old data.
    /// Result is size of current data.
    fn set_data(&mut self, time: u64, old: Data, data: Data, do_history: bool) -> usize {
        if do_history {
            self.history.insert(time, old);
        }
        let result = if let Some(x) = &self.current {
            x.len()
        } else {
            0
        };
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
    pub min: Heap,
    /// Total number of page accesses.
    pub read: u64,
    /// Total number of misses ( data was not already loaded ).
    pub miss: u64,
}

impl Stash {
    /// Set the value of the specified page for the current time.
    fn set(&mut self, lpnum: u64, old: Data, data: Data) -> usize {
        let time = self.time;
        let u = self.vers.entry(time).or_default();
        let do_history = u.insert(lpnum);
        let p = self.get_pinfo(lpnum);
        let result = p.lock().unwrap().set_data(time, old, data, do_history);
        result
    }

    /// Get the PageInfoPtr for the specified page and note the page as used.
    fn get_pinfo(&mut self, lpnum: u64) -> PageInfoPtr {
        let p = self
            .pages
            .entry(lpnum)
            .or_insert_with(PageInfo::new)
            .clone();
        p.lock().unwrap().inc_usage(lpnum, &mut self.min);
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
                    p.lock().unwrap().trim(*t, s)
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

    /// Adjust total memory used by stash.
    fn delta(&mut self, amount: usize, old: usize) {
        if old == 0 {
            self.miss += 1;
        }
        if amount > old {
            self.total += amount - old;
            self.trim_cache();
        } else {
            self.total -= old - amount;
        }
    }

    /// Trim cached data to configured limit.
    fn trim_cache(&mut self) {
        while self.total > self.mem_limit && self.min.n > 0 {
            let lpnum = self.min.pop();
            let mut p = self.pages.get(&lpnum).unwrap().lock().unwrap();
            p.hx = HX::MAX;
            if let Some(data) = &p.current {
                self.total -= data.len();
                p.current = None;
            }
        }
    }

    /// Return the number of pages currently cached.
    pub fn cached(&self) -> usize {
        self.min.n as usize
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
    pub fn new(file: AtomicFile) -> Arc<Self> {
        let file = CompactFile::new(file, SP_SIZE, EP_SIZE);
        // Note : if it's not a new file, sp_size and ep_size are read from file header.
        let sp_size = file.sp_size;
        let ep_size = file.ep_size;
        // Set a default stash memory limit of 10 MB.
        let stash = Stash {
            mem_limit: 10 * 1024 * 1024,
            ..Default::default()
        };
        Arc::new(Self {
            stash: Mutex::new(stash),
            file: RwLock::new(file),
            sp_size,
            ep_size,
        })
    }

    /// Calculate the maximum size of a logical page. This value is stored in the Database struct.
    pub fn page_size_max(&self) -> usize {
        let ep_max = (self.sp_size - 2) / 8;
        (self.ep_size - 16) * ep_max + (self.sp_size - 2)
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

    /// Get locked guard of stash.
    pub fn stash(&self) -> std::sync::MutexGuard<'_, Stash> {
        self.spd.stash.lock().unwrap()
    }

    /// Get the Data for the specified page.
    pub fn get_data(&self, lpnum: u64) -> Data {
        // Get page info.
        let pinfo = self.stash().get_pinfo(lpnum);

        // Read the page data.
        let (data, loaded) = pinfo.lock().unwrap().get_data(lpnum, self);

        // If data was read from underlying file, adjust the total data stashed, and trim the stash if appropriate.
        if loaded {
            self.stash().delta(data.len(), 0);
        }
        data
    }

    /// Set the data of the specified page.
    pub fn set_data(&self, lpnum: u64, data: Data) {
        debug_assert!(self.writer);

        // Get copy of current data.
        let old = self.get_data(lpnum);
        let new_len = data.len();

        // Update the stash ( ensures any readers will not attempt to read the file ).
        let old_len = self.stash().set(lpnum, old, data.clone());

        // Write data to underlying file.
        if data.len() > 0 {
            self.spd.file.write().unwrap().set_page(lpnum, data);
        } else {
            self.spd.file.write().unwrap().free_page(lpnum);
        }

        // Adjust the total data stashed, and trim the stash if appropriate.
        self.stash().delta(new_len, old_len);
    }

    /// Allocate a logical page.
    pub fn alloc_page(&self) -> u64 {
        debug_assert!(self.writer);
        self.spd.file.write().unwrap().alloc_page()
    }

    /// Free a logical page.
    pub fn free_page(&self, lpnum: u64) {
        self.set_data(lpnum, nd());
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

    /// Commit changes to underlying file ( or rollback logical page allocations ).
    pub fn save(&self, op: SaveOp) -> usize {
        debug_assert!(self.writer);
        match op {
            SaveOp::Save => {
                self.spd.file.write().unwrap().save();
                self.stash().end_write()
            }
            SaveOp::RollBack => {
                // Note: rollback happens before any pages are updated.
                // However logical page allocations need to be rolled back.
                self.spd.file.write().unwrap().rollback();
                0
            }
        }
    }

    /// Renumber a page.
    #[cfg(feature = "renumber")]
    pub fn renumber_page(&self, lpnum: u64) -> u64 {
        assert!(self.writer);

        let data = self.get_data(lpnum);
        self.stash().set(lpnum, data.clone(), nd());

        let lpnum2 = self.spd.file.write().unwrap().renumber(lpnum);
        let old2 = self.get_data(lpnum2);

        self.stash().set(lpnum2, old2, data);
        lpnum2
    }
}

impl Drop for AccessPagedData {
    fn drop(&mut self) {
        if !self.writer {
            self.stash().end_read(self.time);
        }
    }
}
