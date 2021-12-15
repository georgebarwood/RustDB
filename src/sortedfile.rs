use crate::*;
use page::PAGE_SIZE;
use std::cmp::min;
use std::collections::hash_map::Entry;

/// Sorted Record storage.
///
/// SortedFile is a tree of pages.
///
/// Each page is either a parent page with links to child pages, or a leaf page.
pub struct SortedFile {
    /// Cached pages.
    pub pages: RefCell<HashMap<u64, PagePtr>>,
    /// List of pages that have changed.
    pub dirty_pages: RefCell<Vec<PagePtr>>,
    /// Size of a record.
    pub rec_size: usize,
    /// Size of a key.
    pub key_size: usize,
    /// The root page.
    pub root_page: u64,
    /// Status
    pub ok: Cell<bool>,
}

impl SortedFile {
    /// Create File with specified record size, key size, root page.
    pub fn new(rec_size: usize, key_size: usize, root_page: u64) -> Self {
        SortedFile {
            pages: newmap(),
            dirty_pages: RefCell::new(Vec::new()),
            rec_size,
            key_size,
            root_page,
            ok: Cell::new(true),
        }
    }

    /// Save changes to underlying storage.
    pub fn save(&self, db: &DB, op: SaveOp) {
        if op == SaveOp::RollBack {
            self.rollback();
            return;
        }
        let dp = &mut *self.dirty_pages.borrow_mut();
        while let Some(pp) = dp.pop() {
            let p = &mut *pp.borrow_mut();
            if p.pnum != u64::MAX {
                p.compress(db);
                if false {
                    println!(
                        "Saving page {} root={} count={} node_size={} size={}",
                        p.pnum,
                        self.root_page,
                        p.count,
                        p.node_size,
                        p.size()
                    );
                }
                p.write_header();
                p.is_dirty = false;
                db.file.set_page(p.pnum, p.data.clone());
            }
        }
    }

    /// Clear the cache, changes are discarded instead of being saved.
    pub fn rollback(&self) {
        self.pages.borrow_mut().clear();
        self.dirty_pages.borrow_mut().clear();
    }

    /// Free the underlying storage. File is not useable after this.
    pub fn free_pages(&self, db: &DB, r: &dyn Record) {
        self.free_page(db, self.root_page, r);
        self.rollback();
        self.ok.set(false);
    }

    /// Get the set of used logical pages.
    pub fn get_used(&self, db: &DB, to: &mut HashSet<u64>) {
        self.get_used_page(db, to, self.root_page);
    }

    fn get_used_page(&self, db: &DB, to: &mut HashSet<u64>, pnum: u64) {
        assert!(to.insert(pnum));
        let pp = self.load_page(db, pnum);
        let p = &pp.borrow();
        if p.level != 0 {
            self.get_used_page(db, to, p.first_page);
            self.get_used_pages(db, to, p, p.root);
        }
    }

    fn get_used_pages(&self, db: &DB, to: &mut HashSet<u64>, p: &Page, x: usize) {
        if x != 0 {
            self.get_used_pages(db, to, p, p.left(x));
            self.get_used_pages(db, to, p, p.right(x));
            if p.level > 0 {
                self.get_used_page(db, to, p.child_page(x));
            }
        }
    }

    /// Insert a Record. If the key is a duplicate, the record is not saved.
    pub fn insert(&self, db: &DB, r: &dyn Record) {
        while !self.insert_leaf(db, self.root_page, r, None) {
            // We get here if a child page needed to be split.
        }
    }

    /// Remove a Record.
    pub fn remove(&self, db: &DB, r: &dyn Record) {
        let mut pp = self.load_page(db, self.root_page);
        loop {
            let cpnum = {
                let p = &mut *pp.borrow_mut();
                if p.level == 0 {
                    self.set_dirty(p, &pp);
                    p.remove(db, r);
                    break;
                }
                p.find_child(db, r)
            };
            pp = self.load_page(db, cpnum);
        }
    }

    /// Free a page and any child pages if this is a parent page.
    fn free_page(&self, db: &DB, pnum: u64, r: &dyn Record) {
        let pp = self.load_page(db, pnum);
        let p = &pp.borrow();
        if p.level != 0 {
            if p.level > 1 {
                self.free_page(db, p.first_page, r);
            } else {
                db.free_page(p.first_page);
            }
            self.free_parent_node(db, p, p.root, r);
        }
        db.free_page(pnum);
    }

    /// Free a parent node.
    fn free_parent_node(&self, db: &DB, p: &Page, x: usize, r: &dyn Record) {
        if x != 0 {
            self.free_parent_node(db, p, p.left(x), r);
            self.free_parent_node(db, p, p.right(x), r);
            p.drop_key(db, x, r);
            let cp = p.child_page(x);
            if p.level > 1 {
                self.free_page(db, cp, r);
            } else {
                db.free_page(cp);
            }
        }
    }

    /// Locate a record with matching key. Result is PagePtr and offset of data.
    pub fn get(&self, db: &DB, r: &dyn Record) -> Option<(PagePtr, usize)> {
        let mut pp = self.load_page(db, self.root_page);
        let off;
        loop {
            let cpnum = {
                let p = pp.borrow();
                if p.level == 0 {
                    let x = p.find_equal(db, r);
                    if x == 0 {
                        return None;
                    }
                    off = p.rec_offset(x);
                    break;
                }
                p.find_child(db, r)
            };
            pp = self.load_page(db, cpnum);
        }
        Some((pp, off))
    }

    /// For iteration in ascending order from start.
    pub fn asc(self: &Rc<Self>, db: &DB, start: Box<dyn Record>) -> Asc {
        Asc::new(db, start, self)
    }

    /// For iteration in descending order from start.
    pub fn dsc(self: &Rc<Self>, db: &DB, start: Box<dyn Record>) -> Dsc {
        Dsc::new(db, start, self)
    }

    /// Insert a record into a leaf page.
    fn insert_leaf(&self, db: &DB, pnum: u64, r: &dyn Record, pi: Option<&ParentInfo>) -> bool {
        let pp = self.load_page(db, pnum);
        let cpnum = {
            // new block to ensure pp borrow is released before recursing.
            let p = &mut *pp.borrow_mut();
            if p.level != 0 {
                p.find_child(db, r)
            } else if !p.full() {
                self.set_dirty(p, &pp);
                p.insert(db, r);
                return true;
            } else {
                // Page is full, divide it into left and right.
                let sp = Split::new(p);
                let sk = &*p.get_key(db, sp.split_node, r);
                // Could insert r into left or right here.
                // sp.right is allocated a new page number.
                let pnum2 = self.alloc_page(db, sp.right);
                match pi {
                    None => {
                        // New root page needed.
                        // New root re-uses the root page number.
                        let mut new_root = self.new_page(p.level + 1);
                        // sp.left is allocated a new page number, which is first page of new root.
                        new_root.first_page = self.alloc_page(db, sp.left);
                        self.publish_page(self.root_page, new_root);
                        self.append_page(db, self.root_page, sk, pnum2);
                    }
                    Some(pi) => {
                        self.publish_page(pnum, sp.left);
                        self.insert_page(db, pi, sk, pnum2);
                    }
                }
                return false; // r has not yet been inserted.
            }
        };
        self.insert_leaf(db, cpnum, r, Some(&ParentInfo { pnum, parent: pi }))
    }

    /// Insert child into a non-leaf page.
    fn insert_page(&self, db: &DB, into: &ParentInfo, r: &dyn Record, cpnum: u64) {
        let pp = self.load_page(db, into.pnum);
        let p = &mut *pp.borrow_mut();
        // Need to check if page is full.
        if !p.full() {
            self.set_dirty(p, &pp);
            p.insert_page(db, r, cpnum);
        } else {
            // Split the parent page.
            let mut sp = Split::new(p);
            let sk = &*p.get_key(db, sp.split_node, r);
            // Insert into either left or right.
            let c = p.compare(db, r, sp.split_node);
            if c == Ordering::Greater {
                sp.left.insert_page(db, r, cpnum);
            } else {
                sp.right.insert_page(db, r, cpnum);
            }
            let pnum2 = self.alloc_page(db, sp.right);
            match into.parent {
                None => {
                    // New root page needed.
                    let mut new_root = self.new_page(p.level + 1);
                    new_root.first_page = self.alloc_page(db, sp.left);
                    self.publish_page(self.root_page, new_root);
                    self.append_page(db, self.root_page, sk, pnum2);
                }
                Some(pi) => {
                    self.publish_page(into.pnum, sp.left);
                    self.insert_page(db, pi, sk, pnum2);
                }
            }
        }
    }

    /// Append child to a non-leaf page. Used when a new root page has just been created.
    fn append_page(&self, db: &DB, into: u64, k: &dyn Record, cpnum: u64) {
        let pp = self.load_page(db, into);
        let p = &mut *pp.borrow_mut();
        self.set_dirty(p, &pp);
        p.append_page(k, cpnum);
    }

    /// Construct a new empty page.
    fn new_page(&self, level: u8) -> Page {
        Page::new(
            if level != 0 {
                self.key_size
            } else {
                self.rec_size
            },
            level,
            nd(),
            u64::MAX,
        )
    }

    /// Allocate a page number, publish the page in the cache.
    fn alloc_page(&self, db: &DB, p: Page) -> u64 {
        let pnum = db.alloc_page();
        self.publish_page(pnum, p);
        pnum
    }

    /// Publish a page in the cache with specified page number.
    fn publish_page(&self, pnum: u64, p: Page) {
        let pp = util::new(p);
        {
            let p = &mut *pp.borrow_mut();
            p.pnum = pnum;
            self.set_dirty(p, &pp);
        }
        self.pages.borrow_mut().insert(pnum, pp);
    }

    fn remove_page(&self, pnum: u64) {
        self.pages.borrow_mut().remove(&pnum);
    }

    /// Get a page from the cache, or if it is not in the cache, load it from external storage.
    fn load_page(&self, db: &DB, pnum: u64) -> PagePtr {
        if !self.ok.get() {
            panic!()
        }
        match self.pages.borrow_mut().entry(pnum) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let data = db.file.get_page(pnum);
                let level = if data.len() == 0 { 0 } else { data[0] };
                let p = util::new(Page::new(
                    if level != 0 {
                        self.key_size
                    } else {
                        self.rec_size
                    },
                    level,
                    data,
                    pnum,
                ));
                e.insert(p.clone());

                if false {
                    let p = p.borrow();
                    println!(
                        "Loaded page {} root={} count={} node_size={} size={}",
                        p.pnum,
                        self.root_page,
                        p.count,
                        p.node_size,
                        p.size()
                    );
                }

                p
            }
        }
    }

    /// Mark a page as changed.
    pub fn set_dirty(&self, p: &mut Page, pp: &PagePtr) {
        if !p.is_dirty {
            p.is_dirty = true;
            self.dirty_pages.borrow_mut().push(pp.clone());
        }
    }

    /// Attempt to free up logical pages by re-packing child pages.
    pub fn repack(&self, db: &DB, r: &dyn Record) -> i64 {
        let mut freed = 0;
        self.repack_page(db, self.root_page, r, &mut freed);
        freed
    }

    /* Notes on repacking.
       When repacking a page of level 2 or more, no keys are dropped or created.
       Instead keys move from the level 1 pages to the level 2 page or vice versa.
       When repacking a level 1 page, parent keys are dropped, and new keys may be created.
    */

    /// Repack a page. Result is number of pages freed.
    fn repack_page(&self, db: &DB, pnum: u64, r: &dyn Record, freed: &mut i64) {
        let pp = self.load_page(db, pnum);
        let p = &mut pp.borrow_mut();

        if p.level == 0 {
            return;
        }

        if p.level > 1 {
            self.repack_page(db, p.first_page, r, freed);
            if *freed >= REPACK_LIMIT {
                return;
            }
            self.repack_children(db, p, p.root, r, freed);
            if *freed >= REPACK_LIMIT {
                return;
            }
        }

        let (x, y) = self.page_total(db, p, p.root, r);
        let n = 1 + x;
        if n < 2 {
            return;
        }
        let total = y + db.page_size(p.first_page);
        let full = (n * PAGE_SIZE) as u64;
        let space = full - total;

        let div = min(10, n as u64);
        if space < full / div {
            return;
        }

        // Iterate over the page child records, appnding them into a PageList of new pages.
        let mut plist = PageList::default();
        plist.add(db, p.first_page, r, self, None, 0);
        self.move_children(db, p, p.root, r, &mut plist);
        let n1 = plist.list.len();

        if TRACE_PACK {
            println!(
                "pnum={} level={} #children={} total={} full={} space={}",
                pnum,
                p.level,
                n,
                total,
                full,
                full - total
            );
            println!(
                "new #children={} diff={} record count={}",
                n1,
                n - n1,
                plist.count
            );
        }
        *freed += plist.store_to(db, p, self);
    }

    /// Count number child pages and their total size, to decide whether to repack a parent page.
    fn page_total(&self, db: &DB, p: &Page, x: usize, r: &dyn Record) -> (usize, u64) {
        if x == 0 {
            return (0, 0);
        }
        let cp = p.child_page(x);
        let cp_size = db.page_size(cp);
        let (n1, t1) = self.page_total(db, p, p.left(x), r);
        let (n2, t2) = self.page_total(db, p, p.right(x), r);
        (1 + n1 + n2, cp_size + t1 + t2)
    }

    /// Move child records into PageList.
    fn move_children(&self, db: &DB, p: &Page, x: usize, r: &dyn Record, plist: &mut PageList) {
        if x != 0 {
            self.move_children(db, p, p.left(x), r, plist);
            let cp = p.child_page(x);
            plist.add(db, cp, r, self, Some(p), x);
            if p.level == 1 {
                p.drop_key(db, x, r);
            }
            self.move_children(db, p, p.right(x), r, plist);
        }
    }

    fn repack_children(&self, db: &DB, p: &Page, x: usize, r: &dyn Record, freed: &mut i64) {
        if x != 0 {
            self.repack_page(db, p.child_page(x), r, freed);
            if *freed >= REPACK_LIMIT {
                return;
            }
            self.repack_children(db, p, p.left(x), r, freed);
            if *freed >= REPACK_LIMIT {
                return;
            }
            self.repack_children(db, p, p.right(x), r, freed);
        }
    }
} // end impl SortedFile

/// Used to pass parent page number for insert operations.
struct ParentInfo<'a> {
    pnum: u64,
    parent: Option<&'a ParentInfo<'a>>,
}

/// For dividing full pages into two.
struct Split {
    count: usize,
    half: usize,
    split_node: usize,
    left: Page,
    right: Page,
}

impl Split {
    /// Split the records of p into two new pages.
    fn new(p: &mut Page) -> Self {
        p.pnum = u64::MAX; // Invalidate old pnum to prevent old page being saved.
        let mut result = Split {
            count: 0,
            half: p.count / 2,
            split_node: 0,
            left: p.new_page(),
            right: p.new_page(),
        };
        result.left.first_page = p.first_page;
        result.split(p, p.root);
        result
    }
    fn split(&mut self, p: &Page, x: usize) {
        if x != 0 {
            self.split(p, p.left(x));
            if self.count < self.half {
                self.left.append_from(p, x);
            } else {
                if self.count == self.half {
                    self.split_node = x;
                }
                self.right.append_from(p, x);
            }
            self.count += 1;
            self.split(p, p.right(x));
        }
    }
} // end impl Split

/// A record to be stored in a SortedFile.
pub trait Record {
    /// Compare record with stored bytes.
    fn compare(&self, db: &DB, data: &[u8]) -> Ordering;
    /// Save record as bytes.
    fn save(&self, _data: &mut [u8]) {}
    /// Load key from bytes ( to store in parent page ).
    fn key(&self, _db: &DB, data: &[u8]) -> Box<dyn Record> {
        Box::new(Id {
            id: util::getu64(data, 0),
        })
    }
    /// Drop parent key ( may need to delete codes ).
    fn drop_key(&self, _db: &DB, _data: &[u8]) {}
}

/// Id record.
pub struct Id {
    pub id: u64,
}

impl Record for Id {
    fn compare(&self, _db: &DB, data: &[u8]) -> Ordering {
        let id = util::getu64(data, 0);
        self.id.cmp(&id)
    }

    fn save(&self, data: &mut [u8]) {
        util::setu64(data, self.id);
    }
}

/// Fetch records from SortedFile in ascending order. The iterator result is a PagePtr and offset of the data.
pub struct Asc {
    stk: Stack,
    file: Rc<SortedFile>,
}

impl Asc {
    fn new(db: &DB, start: Box<dyn Record>, file: &Rc<SortedFile>) -> Self {
        let root_page = file.root_page;
        let mut result = Asc {
            stk: Stack::new(db, start),
            file: file.clone(),
        };
        let pp = file.load_page(db, root_page);
        result.stk.push(&pp, 0);
        result
    }
}

impl Iterator for Asc {
    type Item = (PagePtr, usize);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.stk.next(&self.file)
    }
}

/// Fetch records from SortedFile in descending order.
pub struct Dsc {
    stk: Stack,
    file: Rc<SortedFile>,
}

impl Dsc {
    fn new(db: &DB, start: Box<dyn Record>, file: &Rc<SortedFile>) -> Self {
        let root_page = file.root_page;
        let mut result = Dsc {
            stk: Stack::new(db, start),
            file: file.clone(),
        };
        result.stk.add_page_dsc(file, root_page);
        result
    }
}

impl Iterator for Dsc {
    type Item = (PagePtr, usize);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.stk.prev(&self.file)
    }
}

/// Stack for implementing iteration.
struct Stack {
    v: Vec<(PagePtr, usize)>,
    start: Box<dyn Record>,
    seeking: bool,
    db: DB,
}

impl Stack {
    /// Create a new Stack with specified start key.
    fn new(db: &DB, start: Box<dyn Record>) -> Self {
        Stack {
            v: Vec::new(),
            start,
            seeking: true,
            db: db.clone(),
        }
    }

    /// Push page ptr and offset onto stack.
    fn push(&mut self, pp: &PagePtr, off: usize) {
        self.v.push((pp.clone(), off));
    }

    /// Fetch the next record.
    fn next(&mut self, file: &SortedFile) -> Option<(PagePtr, usize)> {
        while let Some((pp, x)) = self.v.pop() {
            if x == 0 {
                self.add_page_asc(file, pp);
            } else {
                let p = &pp.borrow();
                self.add_asc(p, &pp, p.left(x));
                if p.level != 0 {
                    let cpnum = p.child_page(x);
                    let cpp = file.load_page(&self.db, cpnum);
                    self.add_page_asc(file, cpp);
                } else {
                    self.seeking = false;
                    return Some((pp.clone(), p.rec_offset(x)));
                }
            }
        }
        None
    }

    /// Fetch the previous record.
    fn prev(&mut self, file: &SortedFile) -> Option<(PagePtr, usize)> {
        while let Some((pp, x)) = self.v.pop() {
            let p = &pp.borrow();
            self.add_dsc(p, &pp, p.right(x));
            if p.level != 0 {
                let cpnum = p.child_page(x);
                self.add_page_dsc(file, cpnum);
            } else {
                self.seeking = false;
                return Some((pp.clone(), p.rec_offset(x)));
            }
        }
        None
    }

    /// Seek ascending order. Note that smaller keys are in the right sub-tree.
    fn seek_asc(&mut self, p: &Page, pp: &PagePtr, mut x: usize) {
        while x != 0 {
            match p.compare(&self.db, &*self.start, x) {
                Ordering::Less => {
                    // Start is less than node key. node needs to be visited, so push it onto stack.
                    self.push(pp, x);
                    x = p.right(x);
                }
                Ordering::Equal => {
                    self.push(pp, x);
                    break;
                }
                Ordering::Greater => x = p.left(x),
            }
        }
    }

    /// Returns true if a node is found which is <= start.
    /// This is used to decide whether the the preceding child page is added.
    fn seek_dsc(&mut self, p: &Page, pp: &PagePtr, mut x: usize) -> bool {
        while x != 0 {
            match p.compare(&self.db, &*self.start, x) {
                Ordering::Less => {
                    if !self.seek_dsc(p, pp, p.right(x)) && p.level != 0 {
                        self.push(pp, x);
                    }
                    return true;
                }
                Ordering::Equal => {
                    self.push(pp, x);
                    return true;
                }
                Ordering::Greater => {
                    self.push(pp, x);
                    x = p.left(x);
                }
            }
        }
        false
    }

    fn add_asc(&mut self, p: &Page, pp: &PagePtr, mut x: usize) {
        while x != 0 {
            self.push(pp, x);
            x = p.right(x);
        }
    }

    fn add_dsc(&mut self, p: &Page, pp: &PagePtr, mut x: usize) {
        while x != 0 {
            self.push(pp, x);
            x = p.left(x);
        }
    }

    fn add_page_asc(&mut self, file: &SortedFile, pp: PagePtr) {
        let p = &pp.borrow();
        if p.level != 0 {
            let fp = file.load_page(&self.db, p.first_page);
            self.push(&fp, 0);
        }
        let root = p.root;
        if self.seeking {
            self.seek_asc(p, &pp, root);
        } else {
            self.add_asc(p, &pp, root);
        }
    }

    fn add_page_dsc(&mut self, file: &SortedFile, mut pnum: u64) {
        loop {
            let pp = file.load_page(&self.db, pnum);
            let p = &pp.borrow();
            let root = p.root;
            if self.seeking {
                if self.seek_dsc(p, &pp, root) {
                    return;
                }
            } else {
                self.add_dsc(p, &pp, root);
            }
            if p.level == 0 {
                return;
            }
            pnum = p.first_page;
        }
    }
} // end impl Stack

enum PKey {
    None,
    Dyn(Box<dyn Record>),
    Copy(Vec<u8>),
}

/// PageList is used to implement repacking of child pages ( REPACKFILE builtin function ).
#[derive(Default)]
struct PageList {
    cur: usize,
    list: Vec<(Page, PKey)>,
    pnums: Vec<u64>,
    count: usize,
}

const TRACE_PACK: bool = true;

/// Limit on how many pages to free in one transaction.
const REPACK_LIMIT: i64 = 100;

impl PageList {
    /// Build new parent page.
    fn store_to(&mut self, db: &DB, p: &mut Page, file: &SortedFile) -> i64 {
        let mut pnums = std::mem::take(&mut self.pnums);
        let list = std::mem::take(&mut self.list);
        let mut np = p.new_page();

        for (cp, key) in list {
            let cpnum = pnums.pop().unwrap();
            file.publish_page(cpnum, cp);
            match key {
                PKey::Copy(b) => np.append_page_copy(&b, cpnum),
                PKey::Dyn(key) => np.append_page(&*key, cpnum),
                PKey::None => np.first_page = cpnum,
            }
        }

        let pnum = p.pnum;
        p.pnum = u64::MAX;

        file.remove_page(p.pnum);
        file.publish_page(pnum, np);

        if TRACE_PACK {
            println!("Free pages from pack={:?}", pnums);
        }
        let result = pnums.len();
        while let Some(pnum) = pnums.pop() {
            db.free_page(pnum);
        }
        result as i64
    }

    /// Add a page to the PageList.
    fn add(
        &mut self,
        db: &DB,
        pnum: u64,
        r: &dyn Record,
        file: &SortedFile,
        par: Option<&Page>,
        px: usize,
    ) {
        let pp = file.load_page(db, pnum);
        let p = &mut pp.borrow_mut();

        if self.list.is_empty() {
            self.list.push((p.new_page(), PKey::None));
        }
        self.pnums.push(pnum);
        file.remove_page(pnum);
        p.pnum = u64::MAX;

        if p.level > 0
        // Need to save p.first_page. Append the parent key, then fixup the page number.
        {
            if let Some(pp) = par {
                self.append_one(db, pp, px, r);
            }
            let ap = &mut self.list[self.cur].0;
            let x = ap.count;
            if x == 0 {
                ap.first_page = p.first_page;
            } else {
                ap.set_child_page(x, p.first_page);
            }
        }
        // Append the page records by recursing from p.root.
        self.append(db, p, p.root, r);
    }

    /// Append page records.
    fn append(&mut self, db: &DB, p: &Page, x: usize, r: &dyn Record) {
        if x != 0 {
            self.append(db, p, p.left(x), r);
            self.append_one(db, p, x, r);
            self.append(db, p, p.right(x), r);
        }
    }

    /// Append a single page record.
    fn append_one(&mut self, db: &DB, p: &Page, x: usize, r: &dyn Record) {
        self.count += 1;
        let mut ap = &mut self.list[self.cur].0;
        if ap.full() {
            // Start a new page.
            let key = if ap.level == 0 {
                PKey::Dyn(p.get_key(db, x, r))
            } else {
                PKey::Copy(p.copy(x))
            };
            self.list.push((p.new_page(), key));
            self.cur += 1;
            ap = &mut self.list[self.cur].0;
        }
        ap.append_from(p, x);
    }
}
