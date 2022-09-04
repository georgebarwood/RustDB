use crate::{
    nd, Arc, BTreeMap, CompactFile, Data, HashMap, HashSet, Mutex, RwLock, SaveOp, Storage,
};

/// ```Arc<PageInfo>```
type PageInfoPtr = Arc<PageInfo>;

/// Page data and usage information.
struct PageInfo {
    d: Mutex<PageData>,
    u: Mutex<PageUsage>,
}

impl PageInfo {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            d: Mutex::new(PageData {
                current: None,
                history: BTreeMap::new(),
            }),
            u: Mutex::new(PageUsage {
                counter: 0,
                heap_pos: usize::MAX,
            }),
        })
    }
}

/// Data for a logical page, including historic data.
struct PageData {
    /// Current data for the page.
    current: Option<Data>,
    /// Historic data for the page. Has data for page at specified time.
    /// A copy is made prior to an update, so get looks forward from access time.
    history: BTreeMap<u64, Data>,
}

/// Information about logical page usage.
struct PageUsage {
    /// Count of how many times the page has been used.
    counter: usize,
    /// Position of the page in stash heap.
    heap_pos: usize,
}

impl PageData {
    /// Get the Data for the page, checking history if not a writer.
    /// Reads Data from file if necessary.
    /// Result is Data and flag indicating whether data was read from file.
    fn get(&mut self, lpnum: u64, a: &AccessPagedData) -> (Data, bool) {
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
    /// result is size of old data (if any).
    fn set(&mut self, time: u64, data: Data) -> usize {
        let mut result = 0;
        if let Some(old) = self.current.take() {
            result = old.len();
            self.history.insert(time, old);
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
        if let Some((k, _)) = self.history.range(..t).rev().next() {
            *k + 1
        } else {
            0
        }
    }
}

/// Heap keeps track of the page with the smallest usage counter.
#[derive(Default)]
struct Heap {
    v: Vec<PageInfoPtr>,
}

impl Heap {
    /// Increases usage counter for p and adjusts the heap to match.
    fn used(&mut self, p: &PageInfoPtr) {
        let (mut pos, counter) = {
            let mut p = p.u.lock().unwrap();
            p.counter += 1;
            (p.heap_pos, p.counter)
        };
        if pos == usize::MAX {
            pos = self.v.len();
            self.v.push(p.clone());
            self.move_up(pos, counter);
        } else {
            self.move_down(pos, counter);
        }
    }

    /// Free the least used page and remove it from the heap.
    /// Returns the amount of memory freed.
    fn free(&mut self) -> usize {
        let mut result = 0;
        {
            let p = &self.v[0];
            let mut d = p.d.lock().unwrap();
            if let Some(data) = &d.current {
                result = data.len();
                d.current = None;
            }
            let mut u = p.u.lock().unwrap();
            u.heap_pos = usize::MAX;
        }
        // Pop the last element of the vector, save in position zero.
        let last = self.v.pop().unwrap();
        let counter = last.u.lock().unwrap().counter;
        self.v[0] = last;
        // Restore heap invariant.
        self.move_down(0, counter);
        result
    }

    /// Called when page at pos may be too low in the heap.
    fn move_up(&mut self, mut pos: usize, counter: usize) {
        loop {
            if pos == 0 {
                break;
            }
            let ppos = (pos - 1) / 2;
            {
                let mut pl = self.v[ppos].u.lock().unwrap();
                if pl.counter <= counter {
                    break;
                }
                pl.heap_pos = pos;
            }
            self.v.swap(ppos, pos);
            pos = ppos;
        }
        self.v[pos].u.lock().unwrap().heap_pos = pos;
    }

    /// Called when page at pos may be too high in the heap.
    fn move_down(&mut self, mut pos: usize, counter: usize) {
        let n = self.v.len();
        loop {
            let mut cpos = pos * 2 + 1;
            if cpos >= n {
                break;
            } else {
                let mut c1 = self.v[cpos].u.lock().unwrap().counter;
                if cpos + 1 < n {
                    let c2 = self.v[cpos + 1].u.lock().unwrap().counter;
                    if c2 < c1 {
                        cpos += 1;
                        c1 = c2;
                    }
                }
                if counter <= c1 {
                    break;
                }
            }
            self.v.swap(pos, cpos);
            self.v[pos].u.lock().unwrap().heap_pos = pos;
            pos = cpos;
        }
        self.v[pos].u.lock().unwrap().heap_pos = pos;
    }
}

/// Central store of data.
#[derive(Default)]
pub struct Stash {
    /// Write time - number of writes.
    time: u64,
    /// Page number -> page info.
    pages: HashMap<u64, PageInfoPtr>,
    /// Time -> reader count. Number of readers for given time.
    rdrs: BTreeMap<u64, usize>,
    /// Time -> set of page numbers. Page copies held for given time.
    vers: BTreeMap<u64, HashSet<u64>>,
    /// Total size of current pages.
    pub total: usize,
    /// trim_cache reduces total to mem_limit (or below).
    pub mem_limit: usize,
    /// Heap of pages, page with smallest counter in position 0.
    heap: Heap,
    /// Trace cache trimming etc.
    pub trace: bool,
}

impl Stash {
    /// Set the value of the specified page for the current time.
    fn set(&mut self, lpnum: u64, data: Data) {
        let time = self.time;
        let u = self.vers.entry(time).or_insert_with(HashSet::default);
        if u.insert(lpnum) {
            let p = self
                .pages
                .entry(lpnum)
                .or_insert_with(PageInfo::new)
                .clone();
            self.heap.used(&p);
            self.total += data.len();
            self.total -= p.d.lock().unwrap().set(time, data);
        }
    }

    /// Get the PageInfoPtr for the specified page and note as used.
    fn get(&mut self, lpnum: u64) -> PageInfoPtr {
        let p = self
            .pages
            .entry(lpnum)
            .or_insert_with(PageInfo::new)
            .clone();
        self.heap.used(&p);
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
        if let Some((t, _n)) = self.rdrs.range(..time).rev().next() {
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

    /// Trim cached data ( to reduce memory usage ).
    fn trim_cache(&mut self) {
        let (old_total, old_len) = (self.total, self.heap.v.len());
        while !self.heap.v.is_empty() && self.total >= self.mem_limit {
            self.total -= self.heap.free();
        }
        if self.trace {
            let (new_total, new_len) = (self.total, self.heap.v.len());
            if new_len < old_len {
                println!(
                    "trimmed cache mem_limit={} total={}(-{}) heap len={}(-{})",
                    self.mem_limit,
                    new_total,
                    old_total - new_total,
                    new_len,
                    old_len - new_len
                );
            }
        }
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
        Self {
            stash: Mutex::new(Stash::default()),
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
    pub fn get_page(&self, lpnum: u64) -> Data {
        // Get page info.
        let pinfo = self.spd.stash.lock().unwrap().get(lpnum);

        // Read the page data.
        let (data, loaded) = pinfo.d.lock().unwrap().get(lpnum, self);

        if loaded {
            self.spd.stash.lock().unwrap().total += data.len();
        }
        data
    }

    /// Set the data of the specified page.
    pub fn set_page(&self, lpnum: u64, data: Data) {
        debug_assert!(self.writer);

        // First update the stash ( ensures any readers will not attempt to read the file ).
        self.spd.stash.lock().unwrap().set(lpnum, data.clone());

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
        self.spd.stash.lock().unwrap().set(lpnum, nd());
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
