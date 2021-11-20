use crate::{BTreeMap, HashMap, HashSet};
use std::ops::Bound::Included;

/// u64
pub type Time = u64;
/// u64
pub type PageId = u64;

/// Purpose of the cache is to retain page values at different times.
/// A database is assumed to be a large collection of pages.
/// There can be multiple readers, and one writer.
/// Writers save current page values in the cache before modifying them in the main database.
/// Readers see a consistent version of the database by checking the cache.
pub struct Cache<T> {
    time: Time,
    pages: HashMap<PageId, CachePage<T>>, // Page for specific PageId.
    readers: BTreeMap<Time, usize>,       // Count of readers at specified Time.
    updates: BTreeMap<Time, HashSet<PageId>>, // Set of PageIds updated at specified Time.
}

impl<T> Default for Cache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Cache<T> {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Cache {
            time: 0,
            pages: HashMap::new(),
            readers: BTreeMap::new(),
            updates: BTreeMap::new(),
        }
    }

    /// Set the value of the specified page for the current time.
    pub fn set(&mut self, pageid: PageId, value: T) {
        let time = self.time;
        let u = self.updates.entry(time).or_insert_with(HashSet::new);
        if u.insert(pageid) {
            let p = self.pages.entry(pageid).or_insert_with(CachePage::new);
            p.set(time, value);
        }
    }

    /// Get the value of the specified page for the specified (registered) time.  
    pub fn get(&mut self, pageid: PageId, time: Time) -> Option<&T> {
        if let Some(p) = self.pages.get(&pageid) {
            p.get(time)
        } else {
            Option::None
        }
    }

    /// Register that an update operation has completed. The cache time is incremented.
    /// Cached pages may be freed.
    pub fn tick(&mut self) {
        // println!("Cache tick time={}", self.time);
        self.time += 1;
        self.trim();
    }

    /// Register that there is a client reading the database. The result is the cache time.
    pub fn begin_read(&mut self) -> Time {
        let time = self.time;
        // println!("Cache begin read time={}", time);
        let n = self.readers.entry(time).or_insert(0);
        *n += 1;
        time
    }

    /// Register that the read at the specified time has ended. Cached pages may be freed.
    pub fn end_read(&mut self, time: Time) {
        // println!("Cache end read time={}", time);
        let n = self.readers.get_mut(&time).unwrap();
        *n -= 1;
        if *n == 0 {
            self.readers.remove(&time);
            self.trim();
        }
    }

    fn trim(&mut self) {
        // rt is time of first remaining reader.
        let rt = *self.readers.keys().next().unwrap_or(&self.time);
        // wt is time of first remaining update.
        while let Some(&wt) = self.updates.keys().next() {
            if wt >= rt {
                break;
            }
            for pid in self.updates.remove(&wt).unwrap() {
                let p = self.pages.get_mut(&pid).unwrap();
                // println!("Cache trim page {}", pid);
                p.trim(rt);
            }
        }
    }
} // end impl Cache

struct CachePage<T> {
    values: BTreeMap<Time, T>, // values at different times
}

impl<T> CachePage<T> {
    fn new() -> Self {
        CachePage {
            values: BTreeMap::new(),
        }
    }

    fn set(&mut self, time: Time, value: T) {
        self.values.insert(time, value);
    }

    fn get(&self, time: Time) -> Option<&T> {
        match self
            .values
            .range((Included(&time), Included(&Time::MAX)))
            .next()
        {
            Some((_k, v)) => Some(v),
            None => None,
        }
    }

    fn trim(&mut self, to: Time) {
        while let Some(&f) = self.values.keys().next() {
            if f >= to {
                break;
            }
            self.values.remove(&f);
        }
    }
} // end impl CachePage

fn _cache_test() {
    let mut c = Cache::new();

    for rt in 0..10 {
        assert_eq!(rt, c.begin_read());
        c.set(rt % 3, rt * 10);
        c.tick();
    }

    for rt in 0..10 {
        if let Some(v) = c.get(rt % 3, rt) {
            println!("At time {} value is {}", rt, v);
        }
        c.end_read(rt);
    }

    c.set(0, 0);
    c.tick();
}
