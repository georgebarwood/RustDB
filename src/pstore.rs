use crate::{
    nd, Arc, BTreeMap, CompactFile, Data, HashMap, HashSet, Mutex, RwLock, SaveOp, Storage,
};
use std::ops::Bound::Included;

/// ```Arc<Mutex<PageInfo>>```
type PageInfoPtr = Arc<Mutex<PageInfo>>;

/// Cached information about a logical page.
struct PageInfo {
    current: Option<Data>,
    history: BTreeMap<u64, Data>,
    next: Option<PageInfoPtr>,
    prev: Option<PageInfoPtr>,
    in_chain: bool,
}

impl PageInfo {
    /// Construct a new PageInfo.
    fn new() -> PageInfoPtr {
        Arc::new(Mutex::new(Self {
            current: None,
            history: BTreeMap::new(),
            next: None,
            prev: None,
            in_chain: false,
        }))
    }

    /// Get the Data for the page, checking history if not a writer.
    /// Reads Data from file if necessary.
    /// Result is Data and flag indicating that data was read from file.
    fn get(&mut self, lpnum: u64, a: &AccessPagedData) -> (Data, bool) {
        if !a.writer {
            if let Some((_k, v)) = self
                .history
                .range((Included(&a.time), Included(&u64::MAX)))
                .next()
            {
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

    /// Reduce the history to the specified cache time.
    fn trim(&mut self, to: u64) {
        while let Some(&f) = self.history.keys().next() {
            if f >= to {
                break;
            }
            self.history.remove(&f);
        }
    }
}

/// Central store of data.
#[derive(Default)]
pub struct Stash {
    /// Write time - number of writes.
    time: u64,
    /// Page number -> page info.
    pages: HashMap<u64, PageInfoPtr>,
    /// Time -> reader count.
    readers: BTreeMap<u64, usize>,
    /// Time -> set of page numbers.
    updates: BTreeMap<u64, HashSet<u64>>,
    /// Least recently used current page.
    lru: Option<PageInfoPtr>,
    /// Most recently used current page.
    mru: Option<PageInfoPtr>,
    /// Total size of current pages.
    total: usize,
}

impl Stash {
    /// Insert p into mru/lru chain.
    fn insert(&mut self, mut p: PageInfoPtr) -> PageInfoPtr {
        if p.lock().unwrap().in_chain {
            p = self.remove(p);
        }
        {
            let mut lp = p.lock().unwrap();
            lp.next = self.mru.clone();
            lp.prev = None;
            lp.in_chain = true;
        }
        if let Some(m) = &self.mru {
            m.lock().unwrap().prev = Some(p.clone());
        }
        self.mru = Some(p.clone());
        if self.lru.is_none() {
            self.lru = Some(p.clone());
        }
        p
    }

    /// Remove p from mru/lru chain.
    fn remove(&mut self, p: PageInfoPtr) -> PageInfoPtr {
        let (next, prev) = {
            let mut p = p.lock().unwrap();
            p.in_chain = false;
            let next = p.next.take();
            let prev = p.prev.take();
            (next, prev)
        };

        if let Some(prev) = &prev {
            prev.lock().unwrap().next = next.clone();
        } else {
            self.mru = next.clone();
        }

        if let Some(next) = &next {
            next.lock().unwrap().prev = prev;
        } else {
            self.lru = prev;
        }
        p
    }

    /// Trim cached data ( to reduce memory usage ).
    fn trim_cache(&mut self, to: usize) {
        let mut x = self.lru.clone();
        while let Some(p) = x {
            if self.total <= to {
                break;
            }
            let mut lp = p.lock().unwrap();
            if let Some(d) = &lp.current {
                self.total -= d.len();
                lp.current = None;
            }
            lp.next = None;
            x = lp.prev.take();
            lp.in_chain = false;
            if let Some(p) = &x {
                p.lock().unwrap().next = None;
                self.lru = x.clone();
            } else {
                self.mru = None;
                self.lru = None;
            }
        }
    }

    /// Set the value of the specified page for the current time.
    fn set(&mut self, lpnum: u64, data: Data) {
        let time = self.time;
        let u = self.updates.entry(time).or_insert_with(HashSet::default);
        if u.insert(lpnum) {
            let p = self.pages.entry(lpnum).or_insert_with(PageInfo::new);
            self.total += data.len();
            self.total -= p.lock().unwrap().set(time, data);
        }
    }

    /// Get the PageInfoPtr for the specified page and insert into lru chain.
    fn get(&mut self, lpnum: u64) -> PageInfoPtr {
        let p = {
            let p = self.pages.entry(lpnum).or_insert_with(PageInfo::new);
            p.clone()
        };
        self.insert(p)
    }

    /// Register that there is a client reading the database. The result is the current time.
    fn begin_read(&mut self) -> u64 {
        let time = self.time;
        let n = self.readers.entry(time).or_insert(0);
        *n += 1;
        time
    }

    /// Register that the read at the specified time has ended. Stashed pages may be freed.
    fn end_read(&mut self, time: u64) {
        let n = self.readers.get_mut(&time).unwrap();
        *n -= 1;
        if *n == 0 {
            self.readers.remove(&time);
            self.trim();
        }
    }

    /// Register that an update operation has completed. Time is incremented.
    /// Stashed pages may be freed.
    fn end_write(&mut self) -> usize {
        let result = if let Some(u) = self.updates.get(&self.time) {
            u.len()
        } else {
            0
        };
        self.time += 1;
        self.trim();
        result
    }

    /// Trim due to a read or write ending.
    fn trim(&mut self) {
        // rt is time of first remaining reader.
        let rt = *self.readers.keys().next().unwrap_or(&self.time);
        // wt is time of first remaining update.
        while let Some(&wt) = self.updates.keys().next() {
            if wt >= rt {
                break;
            }
            for lpnum in self.updates.remove(&wt).unwrap() {
                let p = self.pages.get(&lpnum).unwrap();
                p.lock().unwrap().trim(rt);
            }
        }
    }
}

/// Allows logical database pages to be shared to allow concurrent readers.
pub struct SharedPagedData {
    ///
    pub file: RwLock<CompactFile>,
    ///
    pub sp_size: usize,
    ///
    pub ep_size: usize,
    ///
    pub stash: RwLock<Stash>,
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
            stash: RwLock::new(Stash::default()),
            file: RwLock::new(file),
            sp_size,
            ep_size,
        }
    }

    /// Calculate the maxiumum size of a logical page. This value is stored in the Database struct.
    pub fn page_size_max(&self) -> usize {
        let ep_max = (self.sp_size - 2) / 8;
        (self.ep_size - 16) * ep_max + (self.sp_size - 2)
    }

    /// Trim cache.
    pub fn trim_cache(&self, to: usize) {
        self.stash.write().unwrap().trim_cache(to);
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
        let time = spd.stash.write().unwrap().begin_read();
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
        let mut stash = self.spd.stash.write().unwrap();

        // Get PageInfoPtr for the specified page.
        let pinfo = stash.get(lpnum);

        // Lock the Mutex for the page.
        let mut pinfo = pinfo.lock().unwrap();

        // Read the page data.
        let (data, loaded) = pinfo.get(lpnum, self);
        if loaded {
            stash.total += data.len();
        }
        data
    }

    /// Set the data of the specified page.
    pub fn set_page(&self, lpnum: u64, data: Data) {
        debug_assert!(self.writer);

        // First update the stash ( ensures any readers will not attempt to read the file ).
        self.spd.stash.write().unwrap().set(lpnum, data.clone());

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
        self.spd.stash.write().unwrap().set(lpnum, nd());
        self.spd.file.write().unwrap().free_page(lpnum);
    }

    /// Commit changes to underlying file ( or rollback logical page allocations ).
    pub fn save(&self, op: SaveOp) -> usize {
        debug_assert!(self.writer);
        match op {
            SaveOp::Save => {
                self.spd.file.write().unwrap().save();
                self.spd.stash.write().unwrap().end_write()
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
            self.spd.stash.write().unwrap().end_read(self.time);
        }
    }
}
