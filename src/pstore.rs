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
}

impl PageInfo {
    /// Construct a new PageInfo.
    fn new() -> PageInfoPtr {
        Arc::new(Mutex::new(Self {
            current: None,
            history: BTreeMap::new(),
        }))
    }

    /// Get the Data for the page, checking history if not a writer.
    /// Reads Data from file if necessary.
    fn get(&mut self, lpnum: u64, a: &AccessPagedData) -> Data {
        if !a.writer {
            if let Some((_k, v)) = self
                .history
                .range((Included(&a.time), Included(&u64::MAX)))
                .next()
            {
                return v.clone();
            }
        }

        if let Some(p) = &self.current {
            return p.clone();
        }

        // Get data from file.
        let file = a.spd.file.read().unwrap();
        let data = file.get_page(lpnum);
        self.current = Some(data.clone());
        data
    }

    /// Set the page data, updating the history using the specified time and current data.
    fn set(&mut self, time: u64, data: Data) {
        if let Some(old) = self.current.take() {
            self.history.insert(time, old);
        }
        self.current = Some(data);
    }

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
    /// Number of times cache has been used since it was cleared.
    pub cache_used: usize ,
    /// Cache is cleared when this limit is reached.
    pub cache_limit: usize,
}

impl Stash {

    /// Construct new Stash with specified clear limit.
    fn new(limit: usize) -> Self
    {
       let mut result = Self::default();
       result.cache_limit = limit;
       result
    }

    /// Clear cached data ( to reduce memory usage ).
    pub fn clear_cache(&mut self)
    {
       let mut total = 0;
       for (_pnum,pinfo) in &self.pages
       {
          let mut pinfo = pinfo.lock().unwrap();
          if let Some(d) = &pinfo.current
          {
            total += d.len() as u64;
            pinfo.current = None;
          }
       } 
       if total > 0 { println!("clear_cache total={total}"); }
    }

    /// Set the value of the specified page for the current time.
    fn set(&mut self, lpnum: u64, data: Data) {
        let time = self.time;
        let u = self.updates.entry(time).or_insert_with(HashSet::default);
        if u.insert(lpnum) {
            let p = self.pages.entry(lpnum).or_insert_with(PageInfo::new);
            p.lock().unwrap().set(time, data);
        }
    }

    /// Get the PageInfoPtr for the specified page.  
    fn get(&mut self, lpnum: u64) -> PageInfoPtr {
        let p = self.pages.entry(lpnum).or_insert_with(PageInfo::new);
        p.clone()
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

        if self.readers.len() == 0 && self.updates.len() == 0 && self.cache_used >= self.cache_limit
        {
           self.cache_used = 0;
           self.clear_cache();
        }
        else
        {
           self.cache_used += 1;
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
            stash: RwLock::new(Stash::new(10)),
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

    /// Free cached pages.
    pub fn clear_cache(&self)
    {
       self.stash.write().unwrap().clear_cache();
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
        // Get PageInfoPtr for the specified page.
        let pinfo = self.spd.stash.write().unwrap().get(lpnum);

        // Lock the Mutex for the page.
        let mut pinfo = pinfo.lock().unwrap();

        // Read the page data.
        pinfo.get(lpnum, self)
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
