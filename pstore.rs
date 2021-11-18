use crate::cache::Cache;
use crate::*;

/* Idea behind paged storage is:
(a) Caching, shared access to page in memory.
(b) Access to old pages.
*/

pub struct SPSInner {
    pub file: CompactFile,
    pub stash: Cache<Arc<Vec<u8>>>,
    pub cache: HashMap<u64, Arc<Vec<u8>>>,
}

pub struct SharedPagedStorage {
    pub x: Mutex<SPSInner>,
}

impl SharedPagedStorage {
    pub fn new( file: Box<dyn Storage+Send> ) -> Self
    {
      Self{ x: Mutex::new( SPSInner{ file: CompactFile::new(file,400,1024), stash: Cache::new(), cache: HashMap::new() } ) }
    }
    pub fn open_read(self: &Arc<SharedPagedStorage>) -> AccessPagedStorage {
        let mut x = self.x.lock().unwrap();
        AccessPagedStorage {
            writer: false,
            time: x.stash.begin_read(),
            sps: self.clone(),
        }
    }
    pub fn open_write(self: &Arc<SharedPagedStorage>) -> AccessPagedStorage {
        AccessPagedStorage {
            writer: true,
            time: 0,
            sps: self.clone(),
        }
    }
    fn end_read(self: &Arc<SharedPagedStorage>, time: u64) {
        let mut x = self.x.lock().unwrap();
        x.stash.end_read(time);
    }

    fn set_page(&self, lpnum: u64, p: Data) {
        let mut x = self.x.lock().unwrap();
        x.file.set_page(lpnum, &p, p.len());
        let old = 
        {
          if let Some(old) = x.cache.get(&lpnum)
          { old.clone() }
          else 
          { Arc::new( Vec::new() ) }
        };
        x.stash.set(lpnum, old);
        x.cache.insert(lpnum, p);
    }

    fn get_page(&self, lpnum: u64, time: u64) -> Arc<Vec<u8>> {
        // println!("get_page lpnum={} time={}", lpnum, time );
        let mut x = self.x.lock().unwrap();
        if let Some(p) = x.stash.get(lpnum, time) {
            // println!("got page from stash, lpnum={}", lpnum );
            p.clone()
        } else if let Some(p) = x.cache.get(&lpnum) {
            // println!("got page from cache, lpnum={}", lpnum );
            p.clone()
        } else {
            // println!("got page from file, lpnum={}", lpnum );
            let n = x.file.page_size(lpnum);
            let mut v = vec![0; n];
            x.file.get_page(lpnum, &mut v);
            let p = Arc::new(v);
            x.cache.insert(lpnum, p.clone());
            p
        }
    }

    fn direct_get_page(&self, lpnum: u64) -> Arc<Vec<u8>> {
        let mut x = self.x.lock().unwrap();
        if let Some(p) = x.cache.get(&lpnum) {
            p.clone()
        } else {
            let n = x.file.page_size(lpnum);
            let mut v = vec![0; n];
            x.file.get_page(lpnum, &mut v);
            Arc::new(v)
        }
    }
}

pub struct AccessPagedStorage {
    pub writer: bool,
    pub time: u64,
    pub sps: Arc<SharedPagedStorage>,
}

impl AccessPagedStorage {
    pub fn get_page(&self, lpnum: u64) -> Arc<Vec<u8>> {
        if self.writer {
            self.sps.direct_get_page(lpnum)
        } else {
            self.sps.get_page(lpnum, self.time)
        }
    }
    pub fn set_page(&self, lpnum: u64, p: Arc<Vec<u8>>) {
        debug_assert!(self.writer);
        self.sps.set_page(lpnum, p);
    }

    pub fn compress(&self, size: usize, saving: usize) -> bool {
        self.sps.x.lock().unwrap().file.compress(size, saving)
    }
    pub fn is_new(&self) -> bool {
        self.writer && self.sps.x.lock().unwrap().file.is_new()
    }
    pub fn save(&self) {
        let mut x = self.sps.x.lock().unwrap();
        x.file.save();
        x.stash.tick();
    }
    pub fn alloc_page(&self) -> u64 {
        self.sps.x.lock().unwrap().file.alloc_page()
    }
    pub fn free_page(&self, pnum: u64) {
        self.sps.x.lock().unwrap().file.free_page(pnum)
    }
}

impl Drop for AccessPagedStorage {
    fn drop(&mut self) {
        if !self.writer {
            self.sps.end_read(self.time);
        }
    }
}
