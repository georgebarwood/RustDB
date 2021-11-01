use crate::*;
use std::collections::hash_map::Entry;

/// Sorted Record storage.
///
/// SortedFile is a tree of pages.
///
/// Each page is either a parent page with links to child pages, or a leaf page.
pub struct SortedFile
{
  /// Cached pages.
  pages: RefCell<HashMap<u64, PagePtr>>,
  /// List of pages that have changed.
  dirty_pages: RefCell<Vec<PagePtr>>,
  /// Size of a record.
  rec_size: usize,
  /// Size of a key.
  key_size: usize,
  /// The root page.
  root_page: u64,
}

impl SortedFile
{
  /// Create File with specified record size, key size, root page.
  pub fn new(rec_size: usize, key_size: usize, root_page: u64) -> Self
  {
    SortedFile { pages: util::newmap(), dirty_pages: RefCell::new(Vec::new()), rec_size, key_size, root_page }
  }

  /// Insert a Record. If the key is a duplicate, the record is not saved.
  pub fn insert(&self, db: &DB, r: &dyn Record)
  {
    while !self.insert_leaf(db, self.root_page, r, None)
    {
      // We get here if a child page needed to be split.
    }
  }

  /// Remove a Record.
  pub fn remove(&self, db: &DB, r: &dyn Record)
  {
    let mut pp = self.load_page(db, self.root_page);
    loop
    {
      let cpnum = {
        let p = &mut *pp.borrow_mut();
        if p.level == 0
        {
          self.set_dirty(p, &pp);
          p.remove(db, r);
          break;
        }
        p.find_child(db, r)
      };
      pp = self.load_page(db, cpnum);
    }
  }

  /// Locate a record with matching key. Result is PagePtr and offset of data.
  pub fn get(&self, db: &DB, r: &dyn Record) -> Option<(PagePtr, usize)>
  {
    let mut pp = self.load_page(db, self.root_page);
    let off;
    loop
    {
      let cpnum = {
        let p = &pp.borrow();
        if p.level == 0
        {
          let x = p.find_equal(db, r);
          if x == 0
          {
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
  pub fn asc(self: &Rc<Self>, db: &DB, start: Box<dyn Record>) -> Asc
  {
    Asc::new(db, start, self)
  }

  /// For iteration in descending order from start.
  pub fn dsc(self: &Rc<Self>, db: &DB, start: Box<dyn Record>) -> Dsc
  {
    Dsc::new(db, start, self)
  }

  /// Insert a record into a leaf page.
  fn insert_leaf(&self, db: &DB, pnum: u64, r: &dyn Record, pi: Option<&ParentInfo>) -> bool
  {
    let pp = self.load_page(db, pnum);
    let cpnum = {
      // new block to ensure pp borrow is released before recursing.
      let p = &mut pp.borrow_mut();
      if p.level != 0
      {
        p.find_child(db, r)
      }
      else if !p.full()
      {
        self.set_dirty(p, &pp);
        p.insert(db, r);
        return true;
      }
      else
      {
        // Page is full, divide it into left and right.
        let sp = Split::new(p);
        let sk = &*p.get_key(db, sp.split_node, r);

        // Could insert r into left or right here.

        // sp.right is allocated a new page number.
        let pnum2 = self.alloc_page(db, sp.right);
        match pi
        {
          None =>
          {
            // New root page needed.
            // New root re-uses the root page number.
            let mut new_root = self.new_page(p.level + 1);
            // sp.left is allocated a new page number, which is first page of new root.
            new_root.first_page = self.alloc_page(db, sp.left);
            self.publish_page(self.root_page, new_root);
            self.append_page(db, self.root_page, sk, pnum2);
          }
          Some(pi) =>
          {
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
  fn insert_page(&self, db: &DB, into: &ParentInfo, r: &dyn Record, cpnum: u64)
  {
    let pp = self.load_page(db, into.pnum);
    let p = &mut pp.borrow_mut();

    // Need to check if page is full.
    if !p.full()
    {
      self.set_dirty(p, &pp);
      p.insert_page(db, r, cpnum);
    }
    else
    {
      // Split the parent page.
      let mut sp = Split::new(p);
      let sk = &*p.get_key(db, sp.split_node, r);

      // Insert into either left or right.
      let c = p.compare(db, r, sp.split_node);
      if c == Ordering::Greater
      {
        sp.left.insert_page(db, r, cpnum);
      }
      else
      {
        sp.right.insert_page(db, r, cpnum);
      }

      let pnum2 = self.alloc_page(db, sp.right);
      match into.parent
      {
        None =>
        {
          // New root page needed.
          let mut new_root = self.new_page(p.level + 1);
          new_root.first_page = self.alloc_page(db, sp.left);
          self.publish_page(self.root_page, new_root);
          self.append_page(db, self.root_page, sk, pnum2);
        }
        Some(pi) =>
        {
          self.publish_page(into.pnum, sp.left);
          self.insert_page(db, pi, sk, pnum2);
        }
      }
    }
  }

  /// Append child to a non-leaf page. Used when a new root page has just been created.
  fn append_page(&self, db: &DB, into: u64, k: &dyn Record, cpnum: u64)
  {
    let pp = self.load_page(db, into);
    let p = &mut pp.borrow_mut();
    self.set_dirty(p, &pp);
    p.append_page(k, cpnum);
  }

  /// Construct a new empty page.
  fn new_page(&self, level: u8) -> Page
  {
    Page::new(
      if level != 0 { self.key_size } else { self.rec_size },
      level,
      vec![0; PAGE_SIZE],
      u64::MAX,
    )
  }

  /// Allocate a page number, publish the page in the cache.
  fn alloc_page(&self, db: &DB, p: Page) -> u64
  {
    let pnum = db.alloc_page();
    self.publish_page(pnum, p);
    pnum
  }

  /// Publish a page in the cache with specified page number.
  fn publish_page(&self, pnum: u64, mut p: Page)
  {
    p.pnum = pnum;
    let pp = util::new(p);
    self.pages.borrow_mut().insert(pnum, pp.clone());
    let p = &mut pp.borrow_mut();
    self.set_dirty(p, &pp);
  }

  /// Get a page from the cache, or if it is not in the cache, load it from external storage.
  fn load_page(&self, db: &DB, pnum: u64) -> PagePtr
  {
    match self.pages.borrow_mut().entry(pnum)
    {
      Entry::Occupied(e) => e.get().clone(),
      Entry::Vacant(e) =>
      {
        let mut data = vec![0; PAGE_SIZE];
        db.file.borrow_mut().read_page(pnum, &mut data);
        let level = data[0];
        let p = util::new(Page::new(
          if level != 0 { self.key_size } else { self.rec_size },
          level,
          data,
          pnum,
        ));
        e.insert(p.clone());
        p
      }
    }
  }

  /// Mark a page as changed.
  pub fn set_dirty(&self, p: &mut Page, pp: &PagePtr)
  {
    if !p.is_dirty
    {
      p.is_dirty = true;
      self.dirty_pages.borrow_mut().push(pp.clone());
    }
  }

  /// Save any changed pages.
  pub(crate) fn save(&self, db: &DB)
  {
    let dp = &mut *self.dirty_pages.borrow_mut();
    while let Some(pp) = dp.pop()
    {
      let p = &mut pp.borrow_mut();
      if p.pnum != u64::MAX
      {
        println!(
          "Saving page {} root={} count={} node_size={}",
          p.pnum, self.root_page, p.count, p.node_size
        );
        p.compress();
        p.write_header();
        p.is_dirty = false;
        db.file.borrow_mut().write_page(p.pnum, &p.data);
      }
    }
  }

  /// For debugging, dump a summary of each page of the file.
  pub(crate) fn dump(&self)
  {
    for (pnum, pp) in self.pages.borrow().iter()
    {
      let p = &pp.borrow();
      println!(
        "Cached Page pnum={} count={} level={} size()={}",
        pnum,
        p.count,
        p.level,
        p.size()
      );
    }
  }
} // end impl File

// *********************************************************************

/// Used to pass parent page number for insert operations.
struct ParentInfo<'a>
{
  pnum: u64,
  parent: Option<&'a ParentInfo<'a>>,
}

/// For dividing full pages into two.
struct Split
{
  count: usize,
  half: usize,
  split_node: usize,
  left: Page,
  right: Page,
}

impl Split
{
  /// Split the records of p into two new pages.
  fn new(p: &mut Page) -> Self
  {
    p.pnum = u64::MAX; // Invalidate old pnum to prevent old page being saved.
    let mut result = Split { count: 0, half: p.count / 2, split_node: 0, left: p.new_page(), right: p.new_page() };
    result.left.first_page = p.first_page;
    result.split(p, p.root);
    result
  }

  fn split(&mut self, p: &Page, x: usize)
  {
    if x != 0
    {
      self.split(p, p.left(x));
      if self.count < self.half
      {
        self.left.append_from(p, x);
      }
      else
      {
        if self.count == self.half
        {
          self.split_node = x;
        }
        self.right.append_from(p, x);
      }
      self.count += 1;
      self.split(p, p.right(x));
    }
  }
} // end impl split

// *********************************************************************

/// A record to be stored in a SortedFile.
pub trait Record
{
  /// Save record as bytes.
  fn save(&self, _data: &mut [u8]) {}

  /// Compare record with stored bytes.
  fn compare(&self, db: &DB, data: &[u8]) -> std::cmp::Ordering;

  /// Load key from bytes ( to store in parent page ).
  fn key(&self, _db: &DB, data: &[u8]) -> Box<dyn Record>
  {
    Box::new(Id { id: util::getu64(data, 0) })
  }

  /// Drop parent key ( may need to delete codes ).
  /// Only used when pages are being merged ( not yet implemented ).
  fn dropkey(&self, _db: &DB, _data: &[u8]) {}
}

/// Id record.
pub struct Id
{
  pub id: u64,
}

impl Record for Id
{
  fn compare(&self, _db: &DB, data: &[u8]) -> std::cmp::Ordering
  {
    let id = util::getu64(data, 0);
    self.id.cmp(&id)
  }
  fn save(&self, data: &mut [u8])
  {
    util::setu64(data, self.id);
  }
}

// *********************************************************************

/// Fetch records from SortedFile in ascending order. The iterator result is a PagePtr and offset of the data.
pub struct Asc
{
  stk: Stack,
  file: Rc<SortedFile>,
}

impl Asc
{
  fn new(db: &DB, start: Box<dyn Record>, file: &Rc<SortedFile>) -> Self
  {
    let root_page = file.root_page;
    let mut result = Asc { stk: Stack::new(db, start), file: file.clone() };
    let pp = file.load_page(db, root_page);
    result.stk.push(&pp, 0);
    result
  }
}

impl Iterator for Asc
{
  type Item = (PagePtr, usize);
  fn next(&mut self) -> Option<<Self as Iterator>::Item>
  {
    self.stk.next(&self.file)
  }
}

/// Fetch records from SortedFile in descending order.
pub struct Dsc
{
  stk: Stack,
  file: Rc<SortedFile>,
}

impl Dsc
{
  fn new(db: &DB, start: Box<dyn Record>, file: &Rc<SortedFile>) -> Self
  {
    let root_page = file.root_page;
    let mut result = Dsc { stk: Stack::new(db, start), file: file.clone() };
    result.stk.add_page_left(file, root_page);
    result
  }
}

impl Iterator for Dsc
{
  type Item = (PagePtr, usize);
  fn next(&mut self) -> Option<<Self as Iterator>::Item>
  {
    self.stk.prev(&self.file)
  }
}

/// Stack for implementing iteration.
struct Stack
{
  v: Vec<(PagePtr, usize)>,
  start: Box<dyn Record>,
  seeking: bool,
  db: DB,
}

impl Stack
{
  /// Create a new Stack with specified start key.
  fn new(db: &DB, start: Box<dyn Record>) -> Self
  {
    Stack { v: Vec::new(), start, seeking: true, db: db.clone() }
  }

  fn push(&mut self, pp: &PagePtr, off: usize)
  {
    self.v.push((pp.clone(), off));
  }

  /// Fetch the next record.
  fn next(&mut self, file: &SortedFile) -> Option<(PagePtr, usize)>
  {
    while let Some((pp, x)) = self.v.pop()
    {
      if x == 0
      {
        self.add_page_right(file, pp);
      }
      else
      {
        let p = &pp.borrow();
        self.add_right(p, &pp, p.left(x));
        if p.level != 0
        {
          let cpnum = p.child_page(x);
          let cpp = file.load_page(&self.db, cpnum);
          self.add_page_right(file, cpp);
        }
        else
        {
          self.seeking = false;
          return Some((pp.clone(), p.rec_offset(x)));
        }
      }
    }
    None
  }

  fn prev(&mut self, file: &SortedFile) -> Option<(PagePtr, usize)>
  {
    while let Some((pp, x)) = self.v.pop()
    {
      let p = &pp.borrow();
      self.add_left(p, &pp, p.right(x));
      if p.level != 0
      {
        let cpnum = p.child_page(x);
        self.add_page_left(file, cpnum);
      }
      else
      {
        self.seeking = false;
        return Some((pp.clone(), p.rec_offset(x)));
      }
    }
    None
  }

  fn add_right(&mut self, p: &Page, pp: &PagePtr, mut x: usize)
  {
    while x != 0
    {
      self.push(pp, x);
      x = p.right(x);
    }
  }

  fn add_left(&mut self, p: &Page, pp: &PagePtr, mut x: usize)
  {
    while x != 0
    {
      self.push(pp, x);
      x = p.left(x);
    }
  }

  fn seek_right(&mut self, p: &Page, pp: &PagePtr, mut x: usize)
  {
    while x != 0
    {
      match p.compare(&self.db, &*self.start, x)
      {
        Ordering::Less =>
        {
          self.push(pp, x);
          x = p.right(x);
        }
        Ordering::Equal =>
        {
          self.push(pp, x);
          break;
        }
        Ordering::Greater => x = p.left(x),
      }
    }
  }

  /// Returns true if a node is found which is <= start.
  /// This is used to decide whether the the preceding child page is added.
  fn seek_left(&mut self, p: &Page, pp: &PagePtr, mut x: usize) -> bool
  {
    while x != 0
    {
      match p.compare(&self.db, &*self.start, x)
      {
        Ordering::Less =>
        {
          if !self.seek_left(p, pp, p.right(x)) && p.level != 0
          {
            self.push(pp, x);
          }
          return true;
        }
        Ordering::Equal =>
        {
          self.push(pp, x);
          return true;
        }
        Ordering::Greater =>
        {
          self.push(pp, x);
          x = p.left(x);
        }
      }
    }
    false
  }

  fn add_page_right(&mut self, file: &SortedFile, pp: PagePtr)
  {
    let p = &pp.borrow();
    if p.level != 0
    {
      let fp = file.load_page(&self.db, p.first_page);
      self.push(&fp, 0);
    }
    let root = p.root;
    if self.seeking
    {
      self.seek_right(p, &pp, root);
    }
    else
    {
      self.add_right(p, &pp, root);
    }
  }

  fn add_page_left(&mut self, file: &SortedFile, mut pnum: u64)
  {
    loop
    {
      let pp = file.load_page(&self.db, pnum);
      let p = &pp.borrow();
      let root = p.root;
      if self.seeking
      {
        if self.seek_left(p, &pp, root)
        {
          return;
        }
      }
      else
      {
        self.add_left(p, &pp, root);
      }
      if p.level == 0
      {
        return;
      }
      pnum = p.first_page;
    }
  }
} // end impl Stack
