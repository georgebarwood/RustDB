use std::{ collections::HashMap, collections::hash_map::Entry, cmp::Ordering, cell::RefCell, rc::Rc };
use crate::{DB,util,page::*};

/// A record to be stored in a SortedFile.
pub trait Record
{
  /// Save record as bytes( if both is false save only key ).
  fn save( &self, _data: &mut [u8], _off: usize, _both: bool ){}
  /// Read record from bytes ( if both is false read only key ).
  fn load( &mut self, _db: &DB, _data: &[u8], _off: usize, _both: bool ){}
  /// Compare record with stored bytes.
  fn compare( &self, db: &DB, data: &[u8], off: usize ) -> std::cmp::Ordering;
  /// Load key from bytes.
  fn key( &self, db: &DB, data: &[u8], off: usize ) -> Box<dyn Record>;
}

/// Sorted Record storage. 
///
/// SortedFile is a tree of pages. 
///
/// Each page is either a parent page with links to child pages, or a leaf page.
pub struct SortedFile
{
  /// Cached pages.
  pub pages: RefCell<HashMap<u64,PagePtr>>,
  /// Size of a record.
  pub rec_size: usize,
  /// Size of a key.
  pub key_size: usize,
  /// The root page.
  pub root_page: u64,
}

impl SortedFile
{
  /// For debugging, dumps a summary of each page of the file.
  pub(crate) fn dump( &self )
  {
    for ( pnum, ptr ) in self.pages.borrow().iter()
    {
      let p = ptr.borrow();
      println!( "Page pnum={} count={} Parent={} size()={}", pnum, p.count, p.parent, p.size() );
    }
  }

  /// Create File with specified record size, key size, root page.
  pub fn new( rec_size: usize, key_size: usize, root_page: u64 ) -> Self
  {
    SortedFile
    { 
      pages: util::newmap(), 
      rec_size, 
      key_size,
      root_page
    }    
  }

  /// Insert a Record. If the key is a duplicate, the record is not saved.
  pub fn insert( &self, db: &DB, r: &dyn Record )
  {
    while !self.insert_leaf( db, self.root_page, r, None ) 
    {
      // We get here if a child page needed to be split.
    }
  }

  /// Locate a record with matching key. Result is page ptr and offset of data.
  pub fn get( &self, db: &DB, r: &dyn Record ) -> Option< ( PagePtr, usize ) >
  {
    let mut ptr = self.load_page( db, self.root_page );
    let off;
    loop
    {
      let cp;
      {
        let p = ptr.borrow();
        if !p.parent 
        { 
          let x = p.find_equal( db, r );
          if x == 0 { return None; }
          off = p.rec_offset( x );
          break;
        }
        let x = p.find_node( db, r );
        cp = if x == 0 { p.first_page } else { p.child_page( x ) };
      }
      ptr = self.load_page( db, cp );
    }
    Some( ( ptr, off ) )
  }

  /// Remove a Record.
  pub fn remove( &self, db: &DB, r: &dyn Record )
  {
    let mut ptr = self.load_page( db, self.root_page );
    loop
    {
      let cp;
      {
        let mut p = ptr.borrow_mut();
        if !p.parent 
        { 
          p.remove( db, r );
          break; 
        }
        let x = p.find_node( db, r );
        cp = if x == 0 { p.first_page } else { p.child_page( x ) };
      }
      ptr = self.load_page( db, cp );
    }
  }

  /// For iteration in ascending order from start.
  pub fn asc ( self: &Rc<Self>, db: &DB, start: Box<dyn Record> ) -> Asc
  {
    Asc::new( db, start, self.clone() )   
  }

  /// For iteration in descending order from start.
  pub fn dsc ( self: &Rc<Self>, db: &DB, start: Box<dyn Record> ) -> Dsc
  {
    Dsc::new( db, start, self.clone() )
  }

  /// Save any changed pages.
  pub(crate) fn save( &self, db: &DB  )
  {
    for ( pnum, ptr ) in self.pages.borrow().iter()
    {
      let mut p = ptr.borrow_mut();
      if p.dirty
      {
        p.write_header();
        p.dirty = false;
        db.file.write_page( *pnum, &p.data);
      }
    }
  }

  /// Insert a record into a leaf page.
  fn insert_leaf( &self, db: &DB, pnum: u64, r: &dyn Record, pi: Option<&ParentInfo> ) -> bool
  {
    let p = self.load_page( db, pnum );
    // If this is a parent page, we have to be careful to release the borrow before recursing.
    let child_page;
    {
      let mut p = p.borrow_mut();
      if p.parent
      {
        let x = p.find_node( db, r );
        child_page = if x == 0 { p.first_page } else { p.child_page( x ) };
      }
      else if !p.full()
      {
        p.insert( db, r );
        return true;
      }
      else
      {
        // Page is full, divide it into left and right.
        let sp = Split::new( db, &p );
        let sk = &*p.get_key( db, sp.split_node, r );

        // Could insert r into left or right here.

        let pnum2 = self.alloc_page( db, sp.right );
        match pi 
        {
          None =>
          {
            // New root page needed.
            let mut new_root = self.new_page( true );
            new_root.first_page = self.alloc_page( db, sp.left );
            self.publish_page( self.root_page, new_root );
            self.append_page( db, self.root_page, sk, pnum2 );
          }
          Some( pi ) =>
          {  
            self.publish_page( pnum, sp.left );
            self.insert_page( db, pi, sk, pnum2 );
          }
        }
        return false; // r has not yet been inserted.
      }
    };
    self.insert_leaf( db, child_page, r, Some(&ParentInfo{ pnum, parent:pi }) )
  } 

  fn insert_page( &self, db: &DB, into: &ParentInfo, r: &dyn Record, cpnum: u64 )
  {
    let p = self.load_page( db, into.pnum );
    // Need to check if page is full.
    if !p.borrow().full() 
    {
      p.borrow_mut().insert_child( db, r, cpnum );
    } else {
      // Split the parent page.

      let mut sp = Split::new( db, &p.borrow() );       
      let sk = &*p.borrow().get_key( db, sp.split_node, r );

      // Insert into either left or right.
      let c = p.borrow().compare( db, r, sp.split_node );
      if c == Ordering::Greater 
      { 
        sp.left.insert_child( db, r, cpnum ) 
      } else { 
        sp.right.insert_child( db, r, cpnum ) 
      }

      let pnum2 = self.alloc_page( db, sp.right );     
      match into.parent
      {
        None =>
        {
          // New root page needed.
          let mut new_root = self.new_page( true );
          new_root.first_page = self.alloc_page( db, sp.left );
          self.publish_page( self.root_page, new_root );
          self.append_page( db, self.root_page, sk, pnum2 );
        }
        Some( pi ) =>
        {  
          self.publish_page( into.pnum, sp.left );
          self.insert_page( db, pi, sk, pnum2 );
        }
      }
    }   
  }

  fn append_page( &self, db: &DB, into: u64, k: &dyn Record, pnum: u64 )
  {
    let ptr = self.load_page( db, into );
    let mut p = ptr.borrow_mut();
    p.append_child( db, k, pnum );
  }

  /// Construct a new empty page.
  fn new_page( &self, parent:bool ) -> Page
  {
    Page::new( if parent {self.key_size} else {self.rec_size}, parent, vec![0;PAGE_SIZE] )
  }

  /// Allocate a page number, publish the page in the cache.
  fn alloc_page( &self, db: &DB, p:Page ) -> u64
  {
    let pnum = db.file.alloc_page();
    self.publish_page( pnum, p );
    pnum
  }

  /// Publish a page in the cache.
  fn publish_page( &self, pnum: u64, p:Page )
  {
    self.pages.borrow_mut().insert( pnum, util::new(p) );
  }

  /// Get a page from the cache, or if it is not in the cache, load it from external storage.
  fn load_page( &self, db: &DB, pnum: u64 ) -> PagePtr
  {
    match self.pages.borrow_mut().entry( pnum )
    {
      Entry::Occupied( e ) => e.get().clone(),
      Entry::Vacant( e ) => 
      {
        let mut data = vec![ 0; PAGE_SIZE ];
        db.file.read_page( pnum, &mut data );
        let parent = data[0] & 1 != 0;
        let p = util::new( Page::new( if parent {self.key_size} else {self.rec_size}, parent, data ) );
        e.insert(p.clone());
        p
      }
    }
  }
} // end impl File

// *********************************************************************

/// Used to pass parent page number for insert operations.
struct ParentInfo<'a>
{
  pnum: u64,
  parent: Option<&'a ParentInfo<'a>>
}  

/// For dividing full pages into two.
struct Split
{
  count: usize,
  half: usize,
  split_node: usize,
  left: Page,
  right: Page
}  

impl Split
{
  fn new( db: &DB, p: &Page ) -> Self
  {
    let mut result =
    Split
    {
      count:0,
      half: p.count/2,
      split_node: 0,
      left: p.new_page(),
      right: p.new_page()
    };
    result.left.first_page = p.first_page; 
    result.split( db, p, p.root );
    result
  }

  fn split( &mut self, db: &DB, p: &Page, x: usize )
  {
    if x != 0 
    {
      self.split( db, p, p.left(x) );
      if self.count  < self.half 
      { 
        self.left.append_from( db, p, x ); 
      } else { 
        if self.count == self.half { self.split_node = x; }
        self.right.append_from( db, p, x );
      }
      self.count += 1;
      self.split( db, p, p.right(x) );
    }
  }
}

// *********************************************************************

/// Fetch records from SortedFile in ascending order. The iterator result is a PagePtr and offset of the data.
pub struct Asc
{
  stk: Stack,
  file: Rc<SortedFile>
}

impl Asc
{
  fn new( db: &DB, start: Box<dyn Record>, file: Rc<SortedFile> ) -> Self
  {
    let root_page = file.root_page;
    let mut result = Asc{ stk: Stack::new(db,start), file: file.clone() };
    let ptr = file.load_page( db, root_page );
    result.stk.v.push( (ptr, 0) );
    result
  }
}

impl Iterator for Asc
{
  type Item = ( PagePtr, usize );
  fn next(&mut self) -> Option<<Self as Iterator>::Item> 
  { 
    self.stk.next( &self.file )
  }
}

/// Fetch records from SortedFile in descending order.
pub struct Dsc
{
  stk: Stack,
  file: Rc<SortedFile>
}

impl Dsc
{
  fn new( db: &DB, start: Box<dyn Record>, file: Rc<SortedFile> ) -> Self
  {
    let root_page = file.root_page;
    let mut result = Dsc{ stk: Stack::new(db,start), file: file.clone() };
    result.stk.add_page_left( &file, root_page );
    result
  }
}

impl Iterator for Dsc
{
  type Item = ( PagePtr, usize );
  fn next(&mut self) -> Option<<Self as Iterator>::Item> 
  { 
    self.stk.prev( &self.file )
  }
}

/// Stack for implementing iteration.
struct Stack
{
  v : Vec<(PagePtr,usize)>,
  start: Box<dyn Record>,
  seeking: bool,
  db:DB
}

impl Stack
{
  /// Create a new Stack with specified start key.
  fn new( db: &DB, start: Box<dyn Record> ) -> Self
  {
    Stack{ v: Vec::new(), start, seeking:true, db: db.clone() }
  }

  fn next( &mut self, file: &SortedFile ) -> Option< ( PagePtr, usize ) >
  {
    while let Some( ( ptr, x ) ) = self.v.pop()
    {
      if x == 0
      {
        self.add_page_right( file, ptr );
      } else {
        let p = ptr.borrow();
        self.add_right( &p, ptr.clone(), p.left( x ) );
        if p.parent 
        {
          let cp = p.child_page( x );
          let cptr = file.load_page( &self.db, cp );
          self.add_page_right( file, cptr ); 
        } 
        else 
        {
          self.seeking = false;
          return Some( ( ptr.clone(), p.rec_offset(x) ) )
        }
      }                   
    }
    None
  }

  fn prev( &mut self, file: &SortedFile ) -> Option< ( PagePtr, usize ) >
  {
    while let Some( ( ptr, x ) ) = self.v.pop()
    {     
      let p = ptr.borrow();
      self.add_left( &p, ptr.clone(), p.right( x ) );
      if p.parent 
      {
        let cp = p.child_page( x );
        self.add_page_left( file, cp ); 
      } 
      else 
      {
        self.seeking = false;
        return Some( ( ptr.clone(), p.rec_offset(x) ) )
      }                   
    }              
    None
  }

  fn add_right( &mut self, p: &Page, ptr: PagePtr, mut x: usize )
  {
    while x != 0
    {
      self.v.push( (ptr.clone(), x) );
      x = p.right( x );
    }
  }

  fn add_left( &mut self, p: &Page, ptr: PagePtr, mut x: usize )
  {
    while x != 0
    {
      self.v.push( (ptr.clone(), x) );
      x = p.left( x );
    }
  }

  fn seek_right( &mut self, p: &Page, ptr: PagePtr, mut x:usize )
  {
    while x != 0
    {
      match p.compare( &self.db, &*self.start, x )
      {
        Ordering::Less =>
        {
          self.v.push( ( ptr.clone(), x ) );
          x = p.right( x );
        }
        Ordering::Equal => 
        {
          self.v.push( ( ptr, x ) );
          break;
        }
        Ordering::Greater => 
        {
          x = p.left( x )
        }
      }
    }
  }

  fn seek_left( &mut self, p: &Page, ptr: PagePtr, mut x: usize ) -> bool
  // Returns true if a node is found which is <= start.
  // This is used to decide whether the the preceding child page is added.
  {
    while x != 0
    {
      match p.compare( &self.db, &*self.start, x )
      {
        Ordering::Less =>
        {
          if !self.seek_left( p, ptr.clone(), p.right( x ) ) && p.parent
          {
            self.v.push( (ptr, x) );
          }
          return true;
        }
        Ordering::Equal => 
        {
          self.v.push( (ptr, x) );
          return true;
        }
        Ordering::Greater =>
        {
          self.v.push( (ptr.clone(), x) );
          x = p.left( x );
        }
      }
    }
    false
  }

  fn add_page_right( &mut self, file: &SortedFile, ptr: PagePtr )
  {
    let p = ptr.borrow();
    if p.parent 
    { 
      let fp = file.load_page( &self.db, p.first_page );
      self.v.push( (fp, 0) ); 
    }
    let root = p.root;
    if self.seeking 
    {
      self.seek_right( &p, ptr.clone(), root );
    } else { 
      self.add_right( &p, ptr.clone(), root ); 
    }
  }

  fn add_page_left( &mut self, file: &SortedFile, mut pnum: u64 )
  {
    loop
    {
      let ptr = file.load_page( &self.db, pnum );
      let p = ptr.borrow();
      let root = p.root;
      if self.seeking 
      {
        if self.seek_left( &p, ptr.clone(), root ) { return; }
      } else { 
        self.add_left( &p, ptr.clone(), root ); 
      }
      if !p.parent { return; }
      pnum = p.first_page;
    }
  }
} // end impl Stack
