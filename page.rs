use crate::*;

/// ```Rc<RefCell<Page>>```
pub type PagePtr = Rc<RefCell<Page>>;

/// = 0x4000. The maximum size in bytes of each page.
pub const PAGE_SIZE: usize = 0x4000;

/// = 3. Size of Balance,Left,Right in a Node ( 2 + 2 x 11 = 24 bits = 3 bytes ).
const NODE_OVERHEAD: usize = 3;

/// = 8. 45 bits ( 1 + 4 x 11 ) needs 6 bytes, but use 8.
const NODE_BASE: usize = 8;

/// = 6. Number of bytes used to store a page number.
const PAGE_ID_SIZE: usize = 6;

/// = 11. Node ids are 11 bits.
const NODE_ID_BITS: usize = 11;

/// = 2047. Largest Node id.
const MAX_NODE: usize = bitmask!(0, NODE_ID_BITS);

/// Node balance - indicates which child tree is higher.
#[derive(PartialEq)]
enum Balance
{
  LeftHigher = 0,
  Balanced = 1,
  RightHigher = 2,
}
use Balance::*;

/// A page in a SortedFile.
pub struct Page
{
  /// Data storage.
  pub data: Vec<u8>,
  /// Page number in file where page is saved.
  pub(crate) pnum: u64,
  /// Does page need to be saved to backing storage?
  pub(crate) is_dirty: bool,

  /// Number of records currently stored in the page.
  pub(crate) count: usize,
  /// Page level. 0 means a child page, more than 0 a parent page.
  pub(crate) level: u8,
  /// Number of bytes required for each node.
  pub(crate) node_size: usize,
  /// Root node for the page.
  pub(crate) root: usize,
  /// First Free node.
  free: usize,
  /// Number of Nodes currently allocated.     
  alloc: usize,
  /// First child page ( for a parent page ).    
  pub(crate) first_page: u64,
}

impl Page
{
  /// The size of the page in bytes.
  pub fn size(&self) -> usize
  {
    NODE_BASE + self.alloc * self.node_size + if self.level != 0 { PAGE_ID_SIZE } else { 0 }
  }

  /// Construct a new page.
  pub fn new(rec_size: usize, level: u8, data: Vec<u8>, pnum: u64) -> Page
  {
    let node_size = rec_size + if level != 0 { PAGE_ID_SIZE } else { 0 } + NODE_OVERHEAD;
    // Round up to multiple of 8 bytes.
    // node_size = node_size + 7;
    // node_size = node_size - node_size % 8;

    let u = util::get(&data, 0, NODE_BASE);
    let root = getbits!(u, 8, NODE_ID_BITS) as usize;
    let count = getbits!(u, 8 + NODE_ID_BITS, NODE_ID_BITS) as usize;
    let free = getbits!(u, 8 + NODE_ID_BITS * 2, NODE_ID_BITS) as usize;
    let alloc = getbits!(u, 8 + NODE_ID_BITS * 3, NODE_ID_BITS) as usize;

    let first_page = if level != 0
    {
      util::get(&data, NODE_BASE + alloc * node_size, PAGE_ID_SIZE)
    }
    else
    {
      0
    };

    Page { data, node_size, root, count, free, alloc, first_page, level, is_dirty: false, pnum }
  }

  /// Sets header and trailer data (if parent). Called just before page is saved to file.
  pub fn write_header(&mut self)
  {
    let u = self.level as u64
      | ((self.root as u64) << 8)
      | ((self.count as u64) << (8 + NODE_ID_BITS))
      | ((self.free as u64) << (8 + 2 * NODE_ID_BITS))
      | ((self.alloc as u64) << (8 + 3 * NODE_ID_BITS));

    util::set(&mut self.data, 0, u, NODE_BASE);
    if self.level != 0
    {
      let off = self.size() - PAGE_ID_SIZE;
      util::set(&mut self.data, off, self.first_page, PAGE_ID_SIZE);
    }
  }

  /// Is the page full?
  pub fn full(&self) -> bool
  {
    self.free == 0
      && (self.alloc == MAX_NODE
        || NODE_BASE + (self.alloc + 1) * self.node_size + if self.level != 0 { PAGE_ID_SIZE } else { 0 } >= PAGE_SIZE)
  }

  /// Construct a new empty page inheriting record size and level from self.
  /// Used when splitting a page that is full.
  pub fn new_page(&self) -> Page
  {
    Page::new(self.rec_size(), self.level, vec![0; PAGE_SIZE], u64::MAX)
  }

  /// Find child page number.
  pub fn find_child(&self, db: &DB, r: &dyn Record) -> u64
  {
    let mut x = self.root;
    let mut rx = 0;
    while x != 0
    {
      let c = self.compare(db, r, x);
      match c
      {
        Ordering::Greater => x = self.left(x),
        Ordering::Less =>
        {
          rx = x;
          x = self.right(x);
        }
        Ordering::Equal =>
        {
          rx = x;
          break;
        }
      }
    }
    if rx == 0
    {
      self.first_page
    }
    else
    {
      self.child_page(rx)
    }
  }

  /// Returns node id of Record equal to r, or zero if no such node exists.
  pub fn find_equal(&self, db: &DB, r: &dyn Record) -> usize
  {
    let mut x = self.root;
    while x != 0
    {
      let c = self.compare(db, r, x);
      match c
      {
        Ordering::Greater => x = self.left(x),
        Ordering::Less => x = self.right(x),
        Ordering::Equal =>
        {
          return x;
        }
      }
    }
    0
  }

  /// Insert a record into the page ( if the key is a duplicate, nothing happens, and the record is not saved ).
  pub fn insert(&mut self, db: &DB, r: &dyn Record)
  {
    let inserted = self.next_alloc();
    self.root = self.insert_into(self.root, Some((db, r))).0;
    if inserted != self.next_alloc()
    {
      self.set_record(inserted, r);
    }
  }

  /// Insert a child page with specified key and number.
  pub fn insert_page(&mut self, db: &DB, r: &dyn Record, cp: u64)
  {
    let inserted = self.next_alloc();
    self.root = self.insert_into(self.root, Some((db, r))).0;
    self.set_record(inserted, r);
    self.set_child_page(inserted, cp);
  }

  /// Append a child page with specified key and number.
  pub fn append_page(&mut self, r: &dyn Record, cp: u64)
  {
    let inserted = self.next_alloc();
    self.root = self.insert_into(self.root, None).0;
    self.set_record(inserted, r);
    self.set_child_page(inserted, cp);
  }

  /// Append record x from specified page to this page.
  pub fn append_from(&mut self, from: &Page, x: usize)
  {
    if self.level != 0 && self.first_page == 0
    {
      self.first_page = from.child_page(x);
    }
    else
    {
      let inserted = self.next_alloc();
      self.root = self.insert_into(self.root, None).0;
      let dest_off = self.rec_offset(inserted);
      let src_off = from.rec_offset(x);
      let n = self.node_size - NODE_OVERHEAD;
      self.data[dest_off..dest_off + n].copy_from_slice(&from.data[src_off..src_off + n]);
    }
  }

  /// Remove record from this page.
  pub fn remove(&mut self, db: &DB, r: &dyn Record)
  {
    self.root = self.remove_from(db, self.root, r).0;
  }

  // Node access functions.
  // Layout of a Node is
  // Client data
  // Possibly padding
  // Child page number ( if parent page ) ( 6 bytes )
  // Node overhead ( 3 bytes )

  /// Offset of the 3 byte node overhead  for node x.
  fn over_off(&self, x: usize) -> usize
  {
    (NODE_BASE - NODE_OVERHEAD) + x * self.node_size
  }

  /// Offset of the client data for node x.
  pub fn rec_offset(&self, x: usize) -> usize
  {
    NODE_BASE + (x - 1) * self.node_size
  }

  /// The client data size.
  pub fn rec_size(&self) -> usize
  {
    self.node_size - NODE_OVERHEAD - if self.level != 0 { PAGE_ID_SIZE } else { 0 }
  }

  /// Get balance for node x.
  fn balance(&self, x: usize) -> Balance
  {
    let off = self.over_off(x);
    match getbits!(self.data[off], 0, 2)
    {
      0 => LeftHigher,
      1 => Balanced,
      2 => RightHigher,
      _ => panic!(),
    }
  }

  /// Set balance for node x.
  fn set_balance(&mut self, x: usize, balance: Balance)
  {
    let off = self.over_off(x);
    setbits!(self.data[off], 0, 2, balance as u8);
  }

  /// Get the left child node for node x. Result is zero if there is no child.
  pub fn left(&self, x: usize) -> usize
  {
    let off = self.over_off(x);
    self.data[off + 1] as usize | (getbits!(self.data[off] as usize, 2, NODE_ID_BITS - 8) << 8)
  }

  /// Get the right child node for node x. Result is zero if there is no child.
  pub fn right(&self, x: usize) -> usize
  {
    let off = self.over_off(x);
    self.data[off + 2] as usize | (getbits!(self.data[off] as usize, 2 + NODE_ID_BITS - 8, NODE_ID_BITS - 8) << 8)
  }

  /// Set the left child node for node x.
  fn set_left(&mut self, x: usize, y: usize)
  {
    let off = self.over_off(x);
    self.data[off + 1] = (y & 255) as u8;
    setbits!(self.data[off], 2, NODE_ID_BITS - 8, (y >> 8) as u8);
    debug_assert!(self.left(x) == y);
  }

  /// Set the right child node for node x.
  fn set_right(&mut self, x: usize, y: usize)
  {
    let off = self.over_off(x);
    self.data[off + 2] = (y & 255) as u8;
    setbits!(self.data[off], 2 + NODE_ID_BITS - 8, NODE_ID_BITS - 8, (y >> 8) as u8);
    debug_assert!(self.right(x) == y);
  }

  /// Get the child page number for node x in a parent page.
  pub fn child_page(&self, x: usize) -> u64
  {
    debug_assert!(self.level != 0);
    let off = self.over_off(x) - PAGE_ID_SIZE;
    util::get(&self.data, off, PAGE_ID_SIZE)
  }

  /// Set the child page for node x.
  fn set_child_page(&mut self, x: usize, pnum: u64)
  {
    debug_assert!(self.level != 0);
    let off = self.over_off(x) - PAGE_ID_SIZE;
    util::set(&mut self.data, off, pnum as u64, PAGE_ID_SIZE);
  }

  /// Set the record data for node x.
  fn set_record(&mut self, x: usize, r: &dyn Record)
  {
    let off = self.rec_offset(x);
    let size = self.rec_size();
    r.save(&mut self.data[off..off + size]);
  }

  /// Compare record data for node x with record r.
  pub fn compare(&self, db: &DB, r: &dyn Record, x: usize) -> Ordering
  {
    let off = self.rec_offset(x);
    let size = self.rec_size();
    r.compare(db, &self.data[off..off + size])
  }

  /// Get record key for node x.
  pub fn get_key(&self, db: &DB, x: usize, r: &dyn Record) -> Box<dyn Record>
  {
    let off = self.rec_offset(x);
    let size = self.rec_size();
    r.key(db, &self.data[off..off + size])
  }

  // Node Id Allocation.

  /// Peek alloc_node.
  fn next_alloc(&self) -> usize
  {
    if self.free != 0
    {
      self.free
    }
    else
    {
      self.count + 1
    }
  }

  /// Allocate a node.
  fn alloc_node(&mut self) -> usize
  {
    self.count += 1;
    if self.free == 0
    {
      self.alloc += 1;
      self.count
    }
    else
    {
      let result = self.free;
      self.free = self.left(self.free);
      result
    }
  }

  /// Free node x.
  fn free_node(&mut self, x: usize)
  {
    self.set_left(x, self.free);
    self.free = x;
    self.count -= 1;
  }

  /// Insert into node x. Result is node and whether tree height increased.
  fn insert_into(&mut self, mut x: usize, r: Option<(&DB, &dyn Record)>) -> (usize, bool)
  {
    let mut height_increased: bool;
    if x == 0
    {
      x = self.alloc_node();
      self.set_balance(x, Balanced);
      self.set_left(x, 0);
      self.set_right(x, 0);
      height_increased = true;
    }
    else
    {
      let c = match r
      {
        Some((db, r)) => self.compare(db, r, x),
        None => Ordering::Less,
      };

      if c == Ordering::Greater
      {
        let p = self.insert_into(self.left(x), r);
        self.set_left(x, p.0);
        height_increased = p.1;
        if height_increased
        {
          let bx = self.balance(x);
          if bx == Balanced
          {
            self.set_balance(x, LeftHigher);
          }
          else
          {
            height_increased = false;
            if bx == LeftHigher
            {
              return (self.rotate_right(x).0, false);
            }
            self.set_balance(x, Balanced);
          }
        }
      }
      else if c == Ordering::Less
      {
        let p = self.insert_into(self.right(x), r);
        self.set_right(x, p.0);
        height_increased = p.1;
        if height_increased
        {
          let bx = self.balance(x);
          if bx == Balanced
          {
            self.set_balance(x, RightHigher);
          }
          else
          {
            if bx == RightHigher
            {
              return (self.rotate_left(x).0, false);
            }
            height_increased = false;
            self.set_balance(x, Balanced);
          }
        }
      }
      else
      {
        height_increased = false; // Duplicate key
      }
    }
    (x, height_increased)
  }

  /// Rotate right to rebalance tree.
  fn rotate_right(&mut self, x: usize) -> (usize, bool)
  {
    // Left is 2 levels higher than Right.
    let mut height_decreased = true;
    let z = self.left(x);
    let y = self.right(z);
    let zb = self.balance(z);
    if zb != RightHigher
    // Single rotation.
    {
      self.set_right(z, x);
      self.set_left(x, y);
      if zb == Balanced
      // Can only occur when deleting Records.
      {
        self.set_balance(x, LeftHigher);
        self.set_balance(z, RightHigher);
        height_decreased = false;
      }
      else
      {
        // zb = LeftHigher
        self.set_balance(x, Balanced);
        self.set_balance(z, Balanced);
      }
      (z, height_decreased)
    }
    else
    {
      // Double rotation.
      self.set_left(x, self.right(y));
      self.set_right(z, self.left(y));
      self.set_right(y, x);
      self.set_left(y, z);
      let yb = self.balance(y);
      if yb == LeftHigher
      {
        self.set_balance(x, RightHigher);
        self.set_balance(z, Balanced);
      }
      else if yb == Balanced
      {
        self.set_balance(x, Balanced);
        self.set_balance(z, Balanced);
      }
      else
      {
        // yb == RightHigher
        self.set_balance(x, Balanced);
        self.set_balance(z, LeftHigher);
      }
      self.set_balance(y, Balanced);
      (y, height_decreased)
    }
  }

  /// Rotate left to rebalance tree.
  fn rotate_left(&mut self, x: usize) -> (usize, bool)
  {
    // Right is 2 levels higher than Left.
    let mut height_decreased = true;
    let z = self.right(x);
    let y = self.left(z);
    let zb = self.balance(z);
    if zb != LeftHigher
    // Single rotation.
    {
      self.set_left(z, x);
      self.set_right(x, y);
      if zb == Balanced
      // Can only occur when deleting Records.
      {
        self.set_balance(x, RightHigher);
        self.set_balance(z, LeftHigher);
        height_decreased = false;
      }
      else
      {
        // zb = RightHigher
        self.set_balance(x, Balanced);
        self.set_balance(z, Balanced);
      }
      (z, height_decreased)
    }
    else
    {
      // Double rotation
      self.set_right(x, self.left(y));
      self.set_left(z, self.right(y));
      self.set_left(y, x);
      self.set_right(y, z);
      let yb = self.balance(y);
      if yb == RightHigher
      {
        self.set_balance(x, LeftHigher);
        self.set_balance(z, Balanced);
      }
      else if yb == Balanced
      {
        self.set_balance(x, Balanced);
        self.set_balance(z, Balanced);
      }
      else
      {
        // yb == LeftHigher
        self.set_balance(x, Balanced);
        self.set_balance(z, RightHigher);
      }
      self.set_balance(y, Balanced);
      (y, height_decreased)
    }
  }

  /// Remove record from tree x.
  fn remove_from(&mut self, db: &DB, mut x: usize, r: &dyn Record) -> (usize, bool) // out bool heightDecreased
  {
    if x == 0
    // key not found.
    {
      return (x, false);
    }
    let mut height_decreased: bool = true;
    let compare = self.compare(db, r, x);
    if compare == Ordering::Equal
    {
      let deleted = x;
      if self.left(x) == 0
      {
        x = self.right(x);
      }
      else if self.right(x) == 0
      {
        x = self.left(x);
      }
      else
      {
        // Remove the smallest element in the right sub-tree and substitute it for x.
        let t = self.remove_least(self.right(deleted));
        let right = t.0;
        x = t.1;
        height_decreased = t.2;

        self.set_left(x, self.left(deleted));
        self.set_right(x, right);
        self.set_balance(x, self.balance(deleted));
        if height_decreased
        {
          if self.balance(x) == LeftHigher
          {
            let rr = self.rotate_right(x);
            x = rr.0;
            height_decreased = rr.1;
          }
          else if self.balance(x) == RightHigher
          {
            self.set_balance(x, Balanced);
          }
          else
          {
            self.set_balance(x, LeftHigher);
            height_decreased = false;
          }
        }
      }
      self.free_node(deleted);
    }
    else if compare == Ordering::Greater
    {
      let rem = self.remove_from(db, self.left(x), r);
      self.set_left(x, rem.0);
      height_decreased = rem.1;
      if height_decreased
      {
        let xb = self.balance(x);
        if xb == RightHigher
        {
          return self.rotate_left(x);
        }
        if xb == LeftHigher
        {
          self.set_balance(x, Balanced);
        }
        else
        {
          self.set_balance(x, RightHigher);
          height_decreased = false;
        }
      }
    }
    else
    {
      let rem = self.remove_from(db, self.right(x), r);
      self.set_right(x, rem.0);
      height_decreased = rem.1;
      if height_decreased
      {
        let xb = self.balance(x);
        if xb == LeftHigher
        {
          return self.rotate_right(x);
        }
        if self.balance(x) == RightHigher
        {
          self.set_balance(x, Balanced);
        }
        else
        {
          self.set_balance(x, LeftHigher);
          height_decreased = false;
        }
      }
    }
    (x, height_decreased)
  }

  /// Remove smallest node from tree x. Returns root of tree, removed node and height_decreased.
  fn remove_least(&mut self, x: usize) -> (usize, usize, bool)
  {
    if self.left(x) == 0
    {
      (self.right(x), x, true)
    }
    else
    {
      let t = self.remove_least(self.left(x));
      self.set_left(x, t.0);
      let least = t.1;
      let mut height_decreased = t.2;
      if height_decreased
      {
        let xb = self.balance(x);
        if xb == RightHigher
        {
          let rl = self.rotate_left(x);
          return (rl.0, least, rl.1);
        }
        if xb == LeftHigher
        {
          self.set_balance(x, Balanced);
        }
        else
        {
          self.set_balance(x, RightHigher);
          height_decreased = false;
        }
      }
      (x, least, height_decreased)
    }
  }
} // end impl Page
