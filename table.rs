use std::{ rc::Rc, cell::{Cell}, collections::HashMap };
use crate::{Value,util,sf::*,DB,sql::*,run::default,page::*};

/// Table Pointer.
pub type TablePtr = Rc<Table>;

/// Database base table. Underlying file, type information about the columns and id allocation.
pub struct Table
{
  /// Underlying SortedFile.
  pub file: Rc<SortedFile>,
  /// Type information about the columns.
  pub info: Rc<TableInfo>,
  pub(crate) id: i64,
  pub(crate) id_alloc: Cell<i64>,
  pub(crate) id_alloc_dirty: Cell<bool>,
}

impl Table
{
  pub fn access<'d,'t>( &'t self, p: &'d Page, off:usize ) -> Access::<'d,'t>
  {
    Access::<'d,'t>{ data: &p.data[ off..PAGE_SIZE ], info: &self.info }
  }

  pub fn write_access<'d,'t>( &'t self, p: &'d mut Page, off:usize ) -> WriteAccess::<'d,'t>
  {
    WriteAccess::<'d,'t>{ data: &mut p.data[ off..PAGE_SIZE ], info: &self.info }
  }

  pub fn row( &self ) -> Row
  {
    Row::new( self.info.clone() )
  }

  pub fn alloc_id( &self ) -> i64
  {
    let result = self.id_alloc.get();
    self.id_alloc.set( result + 1 );
    self.id_alloc_dirty.set( true );
    result
  }
 
  pub(crate) fn save( &self, db: &DB )
  {
    self.file.save( db );
  }

  pub(crate) fn new0( id: i64, root_page: u64, schema: &str, name: &str, ct: &[(&str,DataType)] ) -> TablePtr
  {
    let name = ObjRef::new( schema, name );
    let info = TableInfo::new( name, ct );
    Self::new( id, root_page, 0, Rc::new(info) )
  }

  pub(crate) fn new( id: i64, root_page: u64, id_alloc:i64, info: Rc<TableInfo> ) -> TablePtr
  {
    let rec_size = info.size;
    let key_size = 8;
    let file = Rc::new(SortedFile::new( rec_size, key_size, root_page ));
    Rc::new( Table{ id, file, info, id_alloc: Cell::new(id_alloc), id_alloc_dirty: Cell::new(false) } )
  }   

  pub fn _dump( &self, db: &DB )
  {
    // println!( "table_dump info={:?}", self.info );
    self.file.dump();
    let mut r = self.row();
    for ( p, off ) in self.file.asc( db, Box::new(Zero{}) )
    {
      let p = p.borrow();
      r.load( db, &p.data, off, true );
      println!( "row id={} value={:?}", r.id, r.values );
    }
  }

}

pub struct Zero
{
}

impl Record for Zero
{
  fn compare( &self, _db: &DB, _data: &[u8], _off: usize ) -> std::cmp::Ordering
  {
    std::cmp::Ordering::Less
  }
  fn key( &self, _db: &DB, _data: &[u8], _off: usize ) -> Box<dyn Record>
  {
    Box::new( Zero{} )
  }
}

/// Id key for specifying start of iteration.
pub struct Id
{
  pub id: i64
}

impl Record for Id
{
  fn compare( &self, _db: &DB, data: &[u8], off: usize ) -> std::cmp::Ordering
  {
    let x = util::get64( data, off ) as i64;
    self.id.cmp( &x )
  }
  fn key( &self, _db: &DB, _data: &[u8], _off: usize ) -> Box<dyn Record>
  {
    Box::new( Id{ id:self.id } )
  }
}

/// Helper class to read byte data using TableInfo.
pub struct Access <'d,'i>
{
  data: &'d [u8],
  info: &'i TableInfo
}

impl <'d,'i> Access <'d,'i>
{
  /// Extract int from byte data for column number colnum.
  pub fn int( &self, colnum: usize ) -> i64
  {
    util::get( self.data, self.info.off[colnum], self.info.sizes[colnum] ) as i64
  }

  /// Extract string from byte data for column number colnum.
  pub fn str( &self, db: &DB, colnum: usize ) -> String
  {
    let u = util::get( self.data, self.info.off[colnum], self.info.sizes[colnum] );
    let bytes = db.decode( u );
    String::from_utf8( bytes ).unwrap()
  }

  /// Extract Id from byte data.
  pub fn id( &self ) -> i64
  {
    util::get64( self.data, 0 ) as i64
  }
}  

/// Helper class to write byte data using TableInfo.
pub struct WriteAccess <'d,'i>
{
  data: &'d mut[u8],
  info: &'i TableInfo
}

impl <'d,'i> WriteAccess <'d,'i>
{
  /// Save int to byte data.
  pub fn set_int( &mut self, colnum: usize, val: i64 )
  {
    util::set( self.data, self.info.off[colnum], val as u64, self.info.sizes[colnum] );
  }
}

/// Holds column names and types for a table.
pub struct TableInfo
{
  pub name: ObjRef,
  pub colmap: HashMap<String,usize>,
  pub colnames: Vec<String>,
  pub types: Vec<DataType>,
  pub sizes: Vec<usize>,
  pub off: Vec<usize>,
  pub size: usize,
}

impl TableInfo
{
  /// Construct a new TableInfo struct with no columns.
  pub fn empty( name:ObjRef ) -> Self
  {
    TableInfo
    { 
      name, 
      colmap: HashMap::new(),
      types: Vec::new(), 
      colnames: Vec::new(),
      sizes: Vec::new(), 
      off: Vec::new(), 
      size:8
    }
  }

  pub(crate) fn new( name: ObjRef, ct: &[(&str,DataType)] ) -> Self
  {
    let mut result = Self::empty( name ); 
    for (n,t) in ct
    {
      result.add( n.to_string(), *t );
    }
    result
  }

  /// Add a column. If the column already exists ( an error ) the result is true.
  pub fn add( &mut self, name: String, typ: DataType ) -> bool
  {
    if self.colmap.contains_key( &name ) { return true; }
    let cn = self.types.len();
    self.types.push( typ );
    self.colnames.push( name.clone() );
    let size = data_size( typ );
    self.off.push( self.size );
    self.sizes.push( size );
    self.size += size;
    self.colmap.insert( name, cn );
    false
  }

  /// Get a column number from a column name.
  /// usize::MAX is returned for "Id". 
  pub fn get( &self, name: &str ) -> Option<&usize>
  {
    if name == "Id" { Some(&usize::MAX) } 
    else { self.colmap.get( name ) }
  }
}

/// Row of Values, with type information.
pub struct Row
{
  pub id: i64,
  pub values: Vec<Value>,
  pub info: Rc<TableInfo>
}

impl Row
{
  pub fn new( info: Rc<TableInfo> ) -> Self
  {
    let mut result = Row{ id:0, values: Vec::new(), info };
    for t in &result.info.types
    {
      result.values.push( default( *t ) );
    }    
    result
  }
}

impl Record for Row
{
  fn save( &self, db: &DB, data: &mut [u8], mut off:usize, both: bool )
  {
    util::set( data, off, self.id as u64, 8 );
    let t = &self.info;
    let chk = off + t.size;
    off += 8;
    if both 
    { 
      for i in 0..t.types.len()
      {
        let size = t.sizes[i];
        match &self.values[i]
        {
          Value::Bool(x) => { data[ off ] = if *x {1} else {0}; }
          Value::Int(x) => util::set( data, off, *x as u64, size ),
          Value::Float(x) =>
          {
            let bytes = (*x).to_le_bytes();
            // for i in 0..size { data[off+i] = bytes[i]; }
            data[off..size + off].clone_from_slice(&bytes);
          }
          Value::String(x) =>
          {
            let u = db.encode( x.as_bytes() );
            util::set( data, off, u, 8 );
          }
          Value::Binary(x) => 
          {
            let u = db.encode( x );
            util::set( data, off, u, 8 );
          }
          _ => {}
        }
        off += size;
        assert!( off <= chk );
      }
      assert!( off == chk );
    }
  }

  fn load( &mut self, db: &DB, data: &[u8], mut off: usize, both: bool )
  {
    self.id = util::get64( data, off ) as i64;
    off += 8;
    let t = &self.info;
    if both
    {
      for i in 0..t.types.len()
      {
        let size = t.sizes[i] as usize;
        let typ = t.types[i];
        self.values[ i ] = match data_kind( typ )
        {
          DK::Bool => Value::Bool( data[off] != 0 ),
          DK::String =>
          {
            let u = util::get64( data, off ); 
            let bytes = db.decode( u );
            let str = String::from_utf8( bytes ).unwrap();
            Value::String( Rc::new( str ) )
          }
          DK::Binary => 
          { 
            let u = util::get64( data, off ); 
            Value::Binary( Rc::new( db.decode( u ) ) )
          }
          _ => Value::Int( util::get( data, off, size ) as i64  )
        };
        off += size;
      }
    }
  }

  fn compare( &self, _db: &DB, data: &[u8], off:usize ) -> std::cmp::Ordering
  {
    let id = util::get64( data, off ) as i64;
    self.id.cmp( &id )
  }

  fn key( &self, db: &DB, data: &[u8], off: usize ) -> Box<dyn Record>
  {
    let mut result = Box::new
    ( 
      Row
       { 
        id: 0,
        values: Vec::new(),
        info: self.info.clone()
      }
    );
    result.load( db, data, off, false );
    result
  }
}

