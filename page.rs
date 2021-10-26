use std::{ cmp::Ordering, rc::Rc, cell::RefCell };
use crate::{sf::Record,DB,util};

/// ```Rc<RefCell<Page>>```
pub type PagePtr = Rc<RefCell<Page>>;

/// = 3. Size of Balance,Left,Right in a Node ( 2 + 2 x 11 = 24 bits = 3 bytes ).
const NODE_OVERHEAD : usize = 3;

/// = 6. 45 bits ( 1 + 4 x 11 ) needs 6 bytes.
const NODE_BASE : usize = 6; 

/// = 6. Number of bytes used to store a page number.
const PAGE_ID_SIZE : usize = 6; 

/// = 0. The left sub-tree is higher than the right.  
const BAL_LEFT_HIGHER : u8 = 0;

/// = 1. The left and right sub-trees have equal height.
const BALANCED : u8 = 1;

/// = 2. The right sub-tree is higher than the left. 
const BAL_RIGHT_HIGHER : u8 = 2;

/// = 11. Node ids are 11 bits.
const NODE_ID_BITS : usize = 11;

/// = 2047. Largest Node id.
const MAX_NODE : usize = bitmask!( 0, NODE_ID_BITS );

/// = 0x4000. The maximum size in bytes of each page.
pub const PAGE_SIZE : usize = 0x4000;

/// A page in a SortedFile.
pub struct Page
{
  /// Data storage. Other items can be derived from data.
  pub data: Vec<u8>,
  /// Number of records currently stored in the page.
  pub count: usize, 
  /// Does page need to be saved to backing storage? 
  pub dirty: bool,  
  /// Is page a parent page? 
  pub parent: bool, 
  /// Number of bytes required for each node.
  node_size: usize, 
  /// Root node for the page.
  pub root: usize,
  /// First Free node.
  free: usize,  
  /// Number of Nodes currently allocated.     
  alloc: usize,  
  /// First child page ( for a parent page ).    
  pub first_page: u64,   
}

impl Page
{
  /// The size of the page in bytes.
  pub fn size( &self ) -> usize
  {
    NODE_BASE + self.alloc * self.node_size + if self.parent {PAGE_ID_SIZE} else {0}
  }

  /// Construct a new page.
  pub fn new( rec_size:usize, parent:bool, data: Vec<u8> ) -> Page
  {
    let node_size = NODE_OVERHEAD + rec_size + if parent {PAGE_ID_SIZE} else {0};

    let u = util::get( &data, 0, NODE_BASE );
    let root  = getbits!( u, 1               , NODE_ID_BITS ) as usize;
    let count = getbits!( u, 1+NODE_ID_BITS  , NODE_ID_BITS ) as usize;
    let free  = getbits!( u, 1+NODE_ID_BITS*2, NODE_ID_BITS ) as usize;
    let alloc = getbits!( u, 1+NODE_ID_BITS*3, NODE_ID_BITS ) as usize;

    let first_page = if parent   
    { 
      util::get( &data, NODE_BASE + alloc * node_size , PAGE_ID_SIZE ) 
    } else { 
      0 
    };

    Page
    {
      data,
      node_size,
      root, 
      count,
      free,
      alloc,
      first_page,
      parent,
      dirty: false,
    }
  }

  /// Sets header and trailer data (if parent). Called just before page is saved to file.
  pub fn write_header(&mut self)
  { 
    let u  = 
    if self.parent {1} else {0}
    | ( ( self.root as u64 ) << 1 )
    | ( ( self.count as u64 ) << (1+NODE_ID_BITS) )
    | ( ( self.free as u64 ) << (1+2*NODE_ID_BITS) )
    | ( ( self.alloc as u64 ) << (1+3*NODE_ID_BITS) );

    util::set( &mut self.data, 0, u, NODE_BASE );
    if self.parent
    { 
      let off = self.size() - PAGE_ID_SIZE;
      util::set( &mut self.data, off, self.first_page, PAGE_ID_SIZE );
    }
  }

  /// Is the page full?
  pub fn full( &self ) -> bool
  {
    self.free == 0 && ( self.alloc == MAX_NODE ||
     NODE_BASE + ( self.alloc + 1 ) * self.node_size
     + if self.parent {PAGE_ID_SIZE} else {0} >= PAGE_SIZE )
  }

  /// The offset of the client data in page data for record number x.
  pub fn rec_offset( &self, x:usize ) -> usize
  {
    NODE_BASE + NODE_OVERHEAD + (x-1) * self.node_size
  }

  /// The record size for this page ( user data ).
  pub fn rec_size( &self ) -> usize
  {
    self.node_size - NODE_OVERHEAD - if self.parent { PAGE_ID_SIZE } else { 0 }
  }

  /// Construct a new empty page inheriting record size and parent from self.
  pub fn new_page( &self ) -> Page
  {
    Page::new( self.rec_size(), self.parent, vec![ 0; PAGE_SIZE ] )
  }

  /// Returns node id of the greatest Record less than or equal to v, or zero if no such node exists.
  pub fn find_node( &self, db: &DB, r: &dyn Record ) -> usize
  {
    let mut x = self.root;
    let mut result = 0;
    while x != 0
    {
      let c = self.compare( db, r, x );
      match c
      {
        Ordering::Greater => x = self.left( x ),
        Ordering::Less => { result = x; x = self.right( x ) }
        Ordering::Equal => { result = x; break; }
      }
    }
    result
  }

  /// Returns node id of Record equal to r, or zero if no such node exists.
  pub fn find_equal( &self, db: &DB, r: &dyn Record ) -> usize
  {
    let mut x = self.root;
    while x != 0
    {
      let c = self.compare( db, r, x );
      match c
      {
        Ordering::Greater => x = self.left( x ),
        Ordering::Less => { x = self.right( x ) }
        Ordering::Equal => { return x; }
      }
    }
    0
  }

  /// Insert a record into the page ( if the key is a duplicate, nothing happens, and the record is not saved ).
  pub fn insert( &mut self, db: &DB, r: &dyn Record )
  {
    let inserted = self.next_alloc();
    self.root = self.insert_into( db, self.root, Some(r) ).0;
    if inserted != self.next_alloc()
    {
      self.dirty = true;
      self.set_record( inserted, r );
    }
  }

  pub fn insert_child( &mut self, db: &DB, r: &dyn Record, cp: u64 )
  {
    let inserted = self.next_alloc();
    self.root = self.insert_into( db, self.root, Some(r) ).0;
    self.dirty = true;
    self.set_record( inserted, r );
    self.set_child_page( inserted, cp );    
  }

  pub fn append_child( &mut self, db: &DB, r: &dyn Record, cp: u64 )
  {
    let inserted = self.next_alloc();
    self.root = self.insert_into( db, self.root, None ).0;
    self.dirty = true;
    self.set_record( inserted, r );
    self.set_child_page( inserted, cp );
  }

  pub fn append_from( &mut self, db: &DB, from: &Page, x: usize ) 
  {
    if self.parent && self.first_page == 0
    {
      self.first_page = from.child_page( x );
    } else {
      let inserted = self.next_alloc();
      self.root = self.insert_into( db, self.root, None ).0;
      let dest_off = self.rec_offset( inserted );
      let src_off = from.rec_offset( x );
      let n = self.node_size - NODE_OVERHEAD;
      for i in 0..n
      {
        self.data[ dest_off + i ] = from.data[ src_off + i ];
      }
    }
    self.dirty = true;
  }

  pub fn remove( &mut self, db: &DB, r: &dyn Record )
  {
    self.root = self.remove_from( db, self.root, r ).0;
    self.dirty = true;
  }

  // Node access functions.

  fn balance( &self, x: usize ) -> u8
  {
    let off = NODE_BASE + (x-1) * self.node_size;
    getbits!( self.data[ off ], 0, 2 )
  }

  fn set_balance( &mut self, x: usize, balance: u8 )
  {
    let off = NODE_BASE + (x-1) * self.node_size;
    setbits!( self.data[ off ], 0, 2, balance );
  } 
 
  /// Get the left child node for a node. Result is zero if there is no child.
  pub fn left( &self, x: usize ) -> usize
  {
    let off = NODE_BASE + (x-1) * self.node_size;
    self.data[ off + 1 ] as usize | ( getbits!( self.data[ off ] as usize, 2, NODE_ID_BITS-8 ) << 8 )
  }

  /// Get the right child node for a node. Result is zero if there is no child.
  pub fn right( &self, x: usize ) -> usize
  { 
    let off = NODE_BASE + (x-1) * self.node_size;
    self.data[ off + 2 ] as usize | ( getbits!( self.data[ off ] as usize, 2+NODE_ID_BITS-8, NODE_ID_BITS-8 ) << 8 )
  }

  fn set_left( &mut self, x: usize, y: usize )
  {
    let off : usize = NODE_BASE + (x-1) * self.node_size;
    self.data[ off + 1 ] = ( y & 255 ) as u8;
    setbits!( self.data[ off ], 2, NODE_ID_BITS-8, ( y >> 8 ) as u8 );
    debug_assert!( self.left( x ) == y );
  }

  fn set_right( &mut self, x: usize, y: usize )
  {
    let off : usize = NODE_BASE + (x-1) * self.node_size;
    self.data[ off + 2 ] = ( y & 255 ) as u8;
    setbits!( self.data[ off ], 2+NODE_ID_BITS-8, NODE_ID_BITS-8, ( y >> 8 ) as u8 );
    debug_assert!( self.right( x ) == y );
  }

  /// Get the child page number for a node in a parent page.
  pub fn child_page( &self, x: usize ) -> u64
  {
    let off = NODE_BASE + x * self.node_size - PAGE_ID_SIZE;
    util::get( &self.data, off, PAGE_ID_SIZE )
  }

  fn set_child_page( &mut self, x: usize, pnum: u64 )
  {
    let off = NODE_BASE + x * self.node_size - PAGE_ID_SIZE;
    util::set( &mut self.data, off, pnum as u64, PAGE_ID_SIZE );
  }

  fn set_record( &mut self, x:usize, r: &dyn Record )
  {
    let off = self.rec_offset( x );
    let size = self.rec_size();
    r.save( &mut self.data[off..off+size] );
  }

  pub fn compare( &self, db: &DB, r: &dyn Record, x:usize ) -> Ordering
  {
    let off = self.rec_offset( x );
    let size = self.rec_size();
    r.compare( db, &self.data[off..off+size] )
  }

  pub fn get_key( &self, db: &DB, x:usize, r: &dyn Record ) -> Box<dyn Record>
  {
    let off = self.rec_offset( x );
    let size = self.rec_size();
    r.key( db, &self.data[off..off+size] )
  }

  // Node Id Allocation.

  fn next_alloc( &self ) -> usize
  {
    if self.free != 0 { self.free } else { self.count + 1 }
  }

  fn alloc_node( &mut self ) -> usize
  {
    self.count += 1;
    if self.free == 0
    {
      self.alloc += 1;
      self.count
    } else {
      let result = self.free;
      self.free = self.left( self.free );
      result
    }
  }

  fn free_node( &mut self, x: usize )
  {
    self.set_left( x, self.free );
    self.free = x;
    self.count -= 1;
  }

  fn insert_into( &mut self, db: &DB, mut x: usize, r: Option<&dyn Record> ) -> ( usize, bool )
  {
    let mut height_increased: bool;
    if x == 0
    {
      x = self.alloc_node();
      self.set_balance( x, BALANCED );
      self.set_left( x, 0 );
      self.set_right( x, 0 );
      height_increased = true;
    } else {
      let c = match r 
      {
        Some(r) => self.compare( db, r, x ),
        None => Ordering::Less
      };

      if c == Ordering::Greater
      {
        let p = self.insert_into( db, self.left(x), r );
        self.set_left( x, p.0 );
        height_increased = p.1;
        if height_increased
        {
          let bx = self.balance( x );
          if bx == BALANCED
          {
            self.set_balance( x, BAL_LEFT_HIGHER );
          } else {
            height_increased = false;
            if bx == BAL_LEFT_HIGHER
            {
              return ( self.rotate_right( x ).0, false );
            }
            self.set_balance( x, BALANCED );
          }
        }
      } else if c == Ordering::Less {
        let p = self.insert_into( db, self.right(x), r );
        self.set_right( x, p.0 );
        height_increased = p.1;
        if height_increased
        {
          let bx = self.balance( x );
          if bx == BALANCED
          {
            self.set_balance( x, BAL_RIGHT_HIGHER );
          } else {
            if bx == BAL_RIGHT_HIGHER
            {
              return ( self.rotate_left( x ).0, false );
            }
            height_increased = false;
            self.set_balance( x, BALANCED );
          }
        }
      } else {
        height_increased = false; // Duplicate key
      }
    }
    ( x, height_increased )
  }

  fn rotate_right( &mut self, x: usize ) -> ( usize, bool )
  {
    // Left is 2 levels higher than Right.
    let mut height_decreased = true;
    let z = self.left( x );
    let y = self.right( z );
    let zb = self.balance( z );
    if zb != BAL_RIGHT_HIGHER // Single rotation.
    {
      self.set_right( z, x );
      self.set_left( x, y );
      if zb == BALANCED // Can only occur when deleting Records.
      {
        self.set_balance( x, BAL_LEFT_HIGHER );
        self.set_balance( z, BAL_RIGHT_HIGHER );
        height_decreased = false;
      } else { // zb = BAL_LEFT_HIGHER
        self.set_balance( x, BALANCED );
        self.set_balance( z, BALANCED );
      }
      ( z, height_decreased )
    } else { // Double rotation.
      self.set_left( x, self.right( y ) );
      self.set_right( z, self.left( y ) );
      self.set_right( y, x );
      self.set_left( y, z );
      let yb = self.balance( y );
      if yb == BAL_LEFT_HIGHER
      {
        self.set_balance( x, BAL_RIGHT_HIGHER );
        self.set_balance( z, BALANCED );
      } else if yb == BALANCED {
        self.set_balance( x, BALANCED );
        self.set_balance( z, BALANCED );
      } else { // yb == BAL_RIGHT_HIGHER
        self.set_balance( x, BALANCED );
        self.set_balance( z, BAL_LEFT_HIGHER );
      }
      self.set_balance( y, BALANCED );
      ( y, height_decreased )
    }
  }

  fn rotate_left( &mut self, x: usize ) -> ( usize, bool )
  {
    // Right is 2 levels higher than Left.
    let mut height_decreased = true;
    let z = self.right( x );
    let y = self.left( z );
    let zb = self.balance( z );
    if zb != BAL_LEFT_HIGHER // Single rotation.
    {
      self.set_left( z, x );
      self.set_right( x, y );
      if zb == BALANCED // Can only occur when deleting Records.
      {
        self.set_balance( x, BAL_RIGHT_HIGHER );
        self.set_balance( z, BAL_LEFT_HIGHER );
        height_decreased = false;
      } else { // zb = BAL_RIGHT_HIGHER
        self.set_balance( x, BALANCED );
        self.set_balance( z, BALANCED );
      }
      (z, height_decreased )
    } else { // Double rotation
      self.set_right( x, self.left( y ) );
      self.set_left( z, self.right( y ) );
      self.set_left( y, x );
      self.set_right( y, z );
      let yb = self.balance( y );
      if yb == BAL_RIGHT_HIGHER
      {
        self.set_balance( x, BAL_LEFT_HIGHER );
        self.set_balance( z, BALANCED );
      } else if yb == BALANCED {
        self.set_balance( x, BALANCED );
        self.set_balance( z, BALANCED );
      } else { // yb == BAL_LEFT_HIGHER
        self.set_balance( x, BALANCED );
        self.set_balance( z, BAL_RIGHT_HIGHER );
      }
      self.set_balance( y, BALANCED );
      ( y, height_decreased )
    }
  }

  fn remove_from( &mut self, db: &DB, mut x: usize, r: &dyn Record  ) -> ( usize, bool ) // out bool heightDecreased
  {
    if x == 0 // key not found.
    {
      return ( x, false );
    }
    let mut height_decreased: bool = true;
    let compare = self.compare( db, r, x );
    if compare == Ordering::Equal
    {
      let deleted = x;
      if self.left( x ) == 0
      {
        x = self.right( x );
      } else if self.right( x ) == 0 {
        x = self.left( x );
      } else {
        // Remove the smallest element in the right sub-tree and substitute it for x.
        let t = self.remove_least( self.right(deleted) );
        let right = t.0;
        x = t.1;
        height_decreased = t.2;

        self.set_left( x, self.left( deleted ) );
        self.set_right( x, right );
        self.set_balance( x, self.balance( deleted ) );
        if height_decreased
        {
          if self.balance( x ) == BAL_LEFT_HIGHER
          {
            let rr = self.rotate_right( x );
            x = rr.0;
            height_decreased = rr.1;
          } else if self.balance( x ) == BAL_RIGHT_HIGHER {
            self.set_balance( x, BALANCED );
          } else {
            self.set_balance( x, BAL_LEFT_HIGHER );
            height_decreased = false;
          }
        }
      }
      self.free_node( deleted );
    } else if compare == Ordering::Greater {
      let rem = self.remove_from( db, self.left( x ), r );
      self.set_left( x, rem.0 );
      height_decreased = rem.1;
      if height_decreased
      {
        let xb = self.balance( x );
        if xb == BAL_RIGHT_HIGHER
        {
          return self.rotate_left( x );
        }
        if xb == BAL_LEFT_HIGHER
        {
          self.set_balance( x, BALANCED );
        } else {
          self.set_balance( x, BAL_RIGHT_HIGHER );
          height_decreased = false;
        }
      }
    } else {
      let rem = self.remove_from( db, self.right(x), r );
      self.set_right( x, rem.0 );
      height_decreased = rem.1;
      if height_decreased
      { 
        let xb = self.balance( x );
        if xb == BAL_LEFT_HIGHER
        {
          return self.rotate_right( x );
        }
        if self.balance( x ) == BAL_RIGHT_HIGHER
        {
          self.set_balance( x, BALANCED );
        } else {
          self.set_balance( x, BAL_LEFT_HIGHER );
          height_decreased = false;
        }
      }
    }
    ( x, height_decreased )
  }

  // Returns root of tree, removed node and height_decreased.
  fn remove_least( &mut self, x: usize ) -> ( usize, usize, bool )
  {
    if self.left(x) == 0
    {
      ( self.right( x ), x, true )
    } else {
      let t = self.remove_least( self.left(x) );
      self.set_left( x, t.0 );
      let least = t.1;
      let mut height_decreased = t.2;
      if height_decreased
      {
        let xb = self.balance( x );
        if xb == BAL_RIGHT_HIGHER
        {
          let rl = self.rotate_left( x );
          return ( rl.0, least, rl.1 );
        }
        if xb == BAL_LEFT_HIGHER
        {
          self.set_balance( x, BALANCED );
        } else {
          self.set_balance( x, BAL_RIGHT_HIGHER );
          height_decreased = false;
        }
      }
      ( x, least, height_decreased )
    }
  }
} // end impl Page

