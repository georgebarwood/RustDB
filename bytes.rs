use std::{ rc::Rc, cell::Cell };
use crate::{ DB,sf::SortedFile,util,sf::Record };

/// Storage of variable size values.
pub struct ByteStorage
{
  pub file: Rc<SortedFile>,
  pub id_alloc: Cell<u64>,
}

impl ByteStorage
{
  pub fn new( root_page: u64 ) -> Self 
  {
    let file = Rc::new(SortedFile::new( 9+BPF, 8, root_page));
    ByteStorage{ file, id_alloc: Cell::new(0) }
  }

  pub fn init( &self, db: &DB )
  {
    // Initialise id_alloc to id of last record.
    let start = Fragment::new( u64::MAX );
    if let Some( ( p, off ) ) = self.file.clone().dsc( db, Box::new(start) ).next()
    {
      let p = p.borrow();
      self.id_alloc.set( 1 + util::getu64( &p.data, off ) );
    }
  }

  pub fn save( &self, db: &DB )
  {
   self.file.save( db );
  }

  pub fn encode( &self, db: &DB, bytes: &[u8] ) -> u64 
  {
    let result = self.id_alloc.get();
    let mut r = Fragment::new( 0 );
    let n = bytes.len();
    let mut done = 0;
    let mut _frags = 0;
    loop
    {
      r.id = self.id_alloc.get();
      self.id_alloc.set( r.id + 1 );
      let mut len = n - done;
      if len > BPF { r.len = (BPF << 1) as u8; len = BPF } else { r.len = 1 + ( ( len as u8 ) << 1 ); }
      // for i in 0..len { r.bytes[ i ] = bytes[ done + i ]; }
      r.bytes[..len].clone_from_slice(&bytes[done..(len + done)]);
      done += len;
      _frags += 1;
      self.file.insert( db, &r );
      if done == n { break; }
    }
    // println!( "encode result={} frags={}", &result, &frags );
    result
  }

  pub fn decode( &self, db: &DB, mut id: u64 ) -> Vec<u8>
  {
    let mut result = vec![ 0u8; 7 ]; // First 7 bytes will be filled in from inline data.
    let start = Fragment::new( id );
    for ( p, off ) in self.file.asc( db, Box::new(start) )
    {
      let p = p.borrow();
      let xid = util::getu64( &p.data, off );
      if xid != id { break; } // Maybe this shoould be a panic?
      id += 1;
      let len = p.data[ off + 8 ] as usize;
      //for i in 0..(len>>1) { result.push( p.data[ rr.off + 9 + i ] ); }
      let off = off + 9;
      result.extend_from_slice( &p.data[ off..off+(len>>1) ] );
      
      if len & 1 == 1 { break; }
    }
    result
  }

  pub fn delcode( &self, db: &DB, id: u64 )
  {
    let start = Fragment::new( id );
    let mut n = 0;
    for ( p, off ) in self.file.asc( db, Box::new(start) )
    {
      let p = p.borrow();
      let xid = util::getu64( &p.data, off );
      if xid != id + n { break; } // Maybe this shoould be a panic?
      n += 1;
      let len = &p.data[ off + 8 ];
      if len & 1 == 1 { break; }
    }
    // println!( "delcode code={} frags={}", id, n ); 
    let mut r = Fragment::new(0);
    for xid in id..id+n
    {
      r.id = xid;
      self.file.remove( db, &r );
    }
  }
}

/// = 52. Number of bytes stored in each fragment.
const BPF : usize = 52;

/// Values are split into BPF size fragments.
struct Fragment 
{
  id: u64,
  /// Bit 0 encodes whether this is the last fragment.
  len: u8, 
  bytes: [u8;BPF],
}

impl Fragment
{
  pub fn new( id: u64 ) -> Self
  {
     Fragment{ id, len:0, bytes:[0;BPF] }
  }
}

impl Record for Fragment
{
  fn save( &self, data: &mut [u8] )
  {
    util::set( data, 0, self.id, 8 );
    data[ 8 ] = self.len;
    data[9..9 + BPF].clone_from_slice(&self.bytes[..BPF]);
  }

  fn compare( &self, _db: &DB, data: &[u8] ) -> std::cmp::Ordering
  {
    let val = util::getu64( data, 0 );
    self.id.cmp( &val )
  }
}
