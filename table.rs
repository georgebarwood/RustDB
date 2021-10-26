use std::{ rc::Rc, cell::{Cell,RefCell}, collections::HashMap, cmp::Ordering };
use crate::{Value,util,sf::*,DB,sql::*,run::*,page::*,sqlparse::Parser,compile::*};

/// Table Pointer.
pub type TablePtr = Rc<Table>;

/// Database base table. Underlying file, type information about the columns and id allocation.
pub struct Table
{
  /// Underlying SortedFile.
  file: Rc<SortedFile>,

  /// Type information about the columns.
  pub(crate) info: Rc<TableInfo>,

  /// List of indexes.
  ixlist: RefCell<IxList>,

  /// Table id in sys.Table.
  pub(crate) id: i64,

  /// Row id allocator.
  pub(crate) id_alloc: Cell<i64>,

  /// Row id allocator has changed.
  pub(crate) id_alloc_dirty: Cell<bool>,
}

pub type IxList = Vec<(Rc<SortedFile>,Rc<Vec<usize>>)>;

impl Table
{
  /// Optimise WHERE clause with form "Name = <someconst>".
  pub fn index_from( self: &TablePtr, p: &Parser, we: &mut Expr ) -> Option< CTableExpression >
  {
    if let ExprIs::Binary(op,e1,e2) = &mut we.exp
    {
      if *op == Token::Equal && e2.is_constant
      {
        if let ExprIs::Name( name ) = &e1.exp
        {
          if name == "Id"
          {
            return Some( CTableExpression::IdGet( self.clone(), cexp_int(p,e2) ) );
          }
          else
          {
            let list = self.ixlist.borrow();
            for (_f,c) in &*list
            {
              if c[0] == e1.col
              {
                return Some( CTableExpression::IxGet( self.clone(), cexp_value(p,e2), e1.col ) );
              }
            }
          }
        }
      }
    }
    None
  }

  /// Get record with specified id.
  pub fn id_get( &self, db: &DB, id: u64 ) -> Option< ( PagePtr, usize ) >
  {
    self.file.get( db, &Id{id} )
  }

  /// Get record with specified key, using an index.
  pub fn ix_get( &self, db: &DB, keycols: &[usize], key: Vec<Value> ) -> Option<( PagePtr, usize )>
  {
    let list = self.ixlist.borrow();
    for (f,c) in &*list
    {
      if **c == keycols
      {
        let key = IndexKey::new( self, c.clone(), key, Ordering::Equal );
        if let Some( ( p, off ) ) = f.get( db, &key )
        {
          let p = p.borrow();
          let off = off + p.rec_size() - 8;
          let id = util::getu64( &p.data, off );
          let row = Id{ id };   
          return self.file.get( db, &row );
        }
        break;
      }
    }
    None
  }

  /// Scan all the records in the table.
  pub fn scan( &self, db: &DB ) -> Asc
  {
    self.file.asc( db, Box::new(Zero{}) )
  }

  /// Get a single record with specified id.
  pub fn scan_id( self: &TablePtr, db: &DB, id: i64 ) -> IdScan
  {
    IdScan{ table: self.clone(), db: db.clone(), id, done: false }
  }

  /// Get records with matching key.
  pub fn scan_key( self: &TablePtr, db: &DB, keycol: usize, key: Value ) -> IndexScan
  {
    let keys = vec![ key ];
    let keycols = [ keycol ];
    self.scan_keys( db, &keycols, keys )
  }

  /// Get records matching the specified keys.
  pub fn scan_keys( self: &TablePtr, db: &DB, keycols: &[usize], keys: Vec<Value> ) -> IndexScan
  {
    let list = self.ixlist.borrow();
    for (f,c) in &*list
    {
      if c.len() >= keycols.len() && keycols == &c[0..keycols.len()]
      {
        // println!("found scan_from keys={:?}", &keys);
        let ikey = IndexKey::new( self, c.clone(), keys.clone(), Ordering::Less );
        let ixa = f.asc( db, Box::new(ikey) );
        return IndexScan
        { 
          ixa, 
          id_off: f.key_size - 8, 
          table: self.clone(), 
          db: db.clone(),
          cols: c.clone(),
          keys
        };
      }
    }
    panic!()
  }

  /// Insert specified row into the table.
  pub fn insert( &self, db: &DB, row: &mut Row )
  {
    let rowid = row.id;
    row.encode( db ); // Calculate codes for Binary and String values.
    self.file.insert( db, row );
    // Update any indexes.
    for ( f, cols ) in &*self.ixlist.borrow()
    {
      let ixr = IndexRow::new( self, rowid, cols.clone(), row );
      f.insert( db, &ixr );
    }
  }

  /// Remove specified row from the table.
  pub fn remove( &self, db: &DB, r: &Row )
  {
    let rowid = r.id;
    self.file.remove( db, r );
    for (f,cols) in &*self.ixlist.borrow()
    {
      let ixr = IndexRow::new( self, rowid, cols.clone(), r );
      f.remove( db, &ixr );
    }
  }

  /// Add the specified index to the table.
  pub fn add_index( &self, root: u64, cols: Vec<usize> )
  {
    let key_size = self.info.calc_index_key_size( &cols ) + 8;
    let file = Rc::new(SortedFile::new( key_size, key_size, root ));

    let mut list = self.ixlist.borrow_mut();
    list.push( (file, Rc::new(cols)) );
  }

  /// Utility for accessing fields by number.
  pub fn access<'d,'t>( &'t self, p: &'d Page, off:usize ) -> Access::<'d,'t>
  {
    Access::<'d,'t>{ data: &p.data[ off..PAGE_SIZE ], info: &self.info }
  }

  /// Utility for updating fields by number.
  pub fn write_access<'d,'t>( &'t self, p: &'d mut Page, off:usize ) -> WriteAccess::<'d,'t>
  {
    WriteAccess::<'d,'t>{ data: &mut p.data[ off..PAGE_SIZE ], info: &self.info }
  }

  /// Construct a row for the table.
  pub fn row( &self ) -> Row
  {
    Row::new( self.info.clone() )
  }

  /// Allocate  row id.
  pub fn alloc_id( &self ) -> i64
  {
    let result = self.id_alloc.get();
    self.id_alloc.set( result + 1 );
    self.id_alloc_dirty.set( true );
    result
  }

  /// Update id allocator if supplied row id exceeds current value.
  pub fn id_allocated( &self, id: i64 )
  {
    if id >= self.id_alloc.get()
    {
      self.id_alloc.set( id + 1 );
      self.id_alloc_dirty.set( true );
    }
  }
 
  /// Save files.
  pub(crate) fn save( &self, db: &DB )
  {
    self.file.save( db );
    for (f,_) in &*self.ixlist.borrow()
    {
      f.save( db );
    }
  }

  /// Construct a new table with specified info.
  pub(crate) fn new( id: i64, root_page: u64, id_alloc:i64, info: Rc<TableInfo> ) -> TablePtr
  {
    let rec_size = info.size;
    let key_size = 8;
    let file = Rc::new(SortedFile::new( rec_size, key_size, root_page ));
    let ixlist = RefCell::new(Vec::new());
    Rc::new( Table{ id, file, info, ixlist, id_alloc: Cell::new(id_alloc), id_alloc_dirty: Cell::new(false) } )
  }   

  pub fn _dump( &self, db: &DB )
  {
    // println!( "table_dump info={:?}", self.info );
    self.file.dump();
    let mut r = self.row();
    for ( p, off ) in self.file.asc( db, Box::new(Zero{}) )
    {
      let p = p.borrow();
      r.load( db, &p.data, off );
      println!( "row id={} value={:?}", r.id, r.values );
    }
  }

}

/// Dummy record for iterating over whole table.
struct Zero{}

impl Record for Zero
{
  fn compare( &self, _db: &DB, _data: &[u8], _off: usize ) -> std::cmp::Ordering
  {
    std::cmp::Ordering::Less
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
    util::getu64( self.data, 0 ) as i64
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

  fn calc_index_key_size( &self, cols: &[usize] ) -> usize
  {
    let mut total = 0;
    for cnum in cols
    {
      total += data_size( self.types[ *cnum ] );
    }
    total
  }
}

/// Index information.
pub struct IndexInfo
{
  pub tname: ObjRef,
  pub iname: String,
  pub cols: Vec<usize>
}

/// Row of Values, with type information.
pub struct Row
{
  pub id: i64,
  pub values: Vec<Value>,
  pub info: Rc<TableInfo>,
  pub codes: Vec<u64>
}

impl Row
{
  pub fn new( info: Rc<TableInfo> ) -> Self
  {
    let mut result = Row{ id:0, values: Vec::new(), info, codes: Vec::new() };
    for t in &result.info.types
    {
      result.values.push( default( *t ) );
    }    
    result
  }
  pub fn newkey( info: Rc<TableInfo> ) -> Self
  {
    Row{ id:0, values: Vec::new(), info, codes: Vec::new() }
  }
  pub fn encode( &mut self, db: &DB )
  {
    self.codes.clear();
    for val in &self.values
    {
      let u = match val
      {
        Value::Binary(x) => db.encode( x ),
        Value::String(x) => db.encode( x.as_bytes() ),
        _ => 0
      };
      self.codes.push( u );
    }    
  }

  fn load( &mut self, db: &DB, data: &[u8], mut off: usize )
  {
    self.id = util::getu64( data, off ) as i64;
    off += 8;
    let t = &self.info;
    for i in 0..t.types.len()
    {
      let typ = t.types[i];
      self.values[ i ] = Value::load( db, typ, data, off );
      off += data_size( typ );
    }
  }
}

impl Record for Row
{
  fn save( &self, data: &mut [u8], mut off:usize, both: bool )
  {
    debug_assert!(both);
    util::set( data, off, self.id as u64, 8 );
    let t = &self.info;
    let chk = off + t.size;
    off += 8;
    for (i,typ) in t.types.iter().enumerate()
    {
      self.values[i].save( t.types[i], data, off, self.codes[i] );
      off += data_size(*typ);
    }
    debug_assert!( off == chk );
  }

  fn compare( &self, _db: &DB, data: &[u8], off:usize ) -> std::cmp::Ordering
  {
    let id = util::getu64( data, off ) as i64;
    self.id.cmp( &id )
  }
}

/// Row for inserting into an index.
struct IndexRow
{
  pub tinfo: Rc<TableInfo>,
  pub cols: Rc<Vec<usize>>,
  pub keys: Vec<Value>,
  pub codes: Vec<u64>,
  pub rowid: i64,
}

impl IndexRow
{
  fn new( table: &Table, rowid: i64, cols: Rc<Vec<usize>>, row: &Row ) -> Self
  {
    let mut keys = Vec::new();
    let mut codes = Vec::new();
    for c in &*cols
    {
      keys.push( row.values[*c].clone() );
      codes.push( row.codes[*c] );
    }
    Self{ tinfo: table.info.clone(), rowid, keys, cols, codes }
  }

  fn load(&mut self, db: &DB, data: &[u8], off: usize )
  {
    let mut off = off;
    for (ix,col) in self.cols.iter().enumerate()
    {
      let typ = self.tinfo.types[ *col ];
      self.keys[ix] = Value::load( db, typ, data, off );
      off += data_size(typ);
    }
    self.rowid = util::getu64( data, off ) as i64;
  }
}

impl Record for IndexRow
{
  fn save(&self, data: &mut [u8], off: usize, _both: bool)
  {
    // println!( "IndexRow::save rowid={} keys={:?}", self.rowid, self.keys );
    let mut off = off;
    for (ix,k) in self.keys.iter().enumerate() 
    {
      let typ = self.tinfo.types[ self.cols[ ix ] ];
      k.save( typ, data, off, self.codes[ix] );
      off += data_size(typ);
    }
    util::set( data, off, self.rowid as u64, 8 );
  }

  fn compare( &self, db: &DB, data: &[u8], off: usize ) -> Ordering
  {
    let mut ix = 0;
    let mut off = off;
    loop
    {
      let typ = self.tinfo.types[ self.cols[ ix ] ];
      let val = Value::load( db, typ, data, off );

      // println!( "IndexRow comparing {:?} with {:?}", &val, &self.keys[ix] );

      let cf = val.cmp( &self.keys[ix ] );
      if cf != Ordering::Equal
      {
        return cf;
      }
      ix += 1;
      if ix == self.cols.len() 
      { 
        let id = util::getu64( data, off ) as i64;
        return self.rowid.cmp( &id );
      }  
      off += data_size( typ );  
    }
  }

  fn key( &self, db: &DB, data: &[u8], off: usize ) -> Box<dyn Record>
  {
    let mut result = Box::new
    ( 
      IndexRow
      { 
        rowid: 0,
        keys: Vec::new(),
        codes: Vec::new(),
        cols: self.cols.clone(),
        tinfo: self.tinfo.clone(),
      }
    );
    result.load( db, data, off );
    result
  }
}

/// Key for searching index.
pub struct IndexKey
{
  pub tinfo: Rc<TableInfo>,
  pub cols: Rc<Vec<usize>>,
  pub key: Vec<Value>,
  pub def: Ordering,
}

impl IndexKey
{
  fn new( table: &Table, cols: Rc<Vec<usize>>, key: Vec<Value>, def:Ordering ) -> Self
  {
    Self{ tinfo: table.info.clone(), key, cols, def }
  }
}

impl Record for IndexKey
{
  fn compare( &self, db: &DB, data: &[u8], off: usize ) -> Ordering
  {
    let mut ix = 0;
    let mut off = off;
    loop
    {
      let typ = self.tinfo.types[ self.cols[ ix ] ];
      let val = Value::load( db, typ, data, off );

      let cf = val.cmp( &self.key[ix ] );
      if cf != Ordering::Equal
      {
        return cf;
      }
      ix += 1;
      if ix == self.key.len() 
      { 
        return self.def
      }  
      off += data_size( typ );  
    }
  }

  fn key( &self, _db: &DB, _data: &[u8], _off: usize ) -> Box<dyn Record>
  {
    panic!()
  }
}

/// Fetch records using an index.
pub struct IndexScan
{
  ixa: Asc,
  id_off: usize,
  table: TablePtr,
  db: DB,
  cols: Rc<Vec<usize>>,
  keys: Vec<Value>, 
}

impl IndexScan
{
  fn keys_equal( &self, data: &[u8] ) -> bool
  {
    let mut off = 0;
    for (ix,k) in self.keys.iter().enumerate()
    {
      let typ = self.table.info.types[ self.cols[ix] ];
      let val = Value::load( &self.db, typ, data, off );
      let cf = val.cmp( k );
      if cf != Ordering::Equal
      {
        return false;
      }
      off += data_size( typ );  
    }
    true
  }
}

impl Iterator for IndexScan
{
  type Item = ( PagePtr, usize );
  fn next(&mut self) -> Option<<Self as Iterator>::Item> 
  { 
    if let Some((p,off)) = self.ixa.next()
    {
      let p = p.borrow();
      let data = &p.data[off..];

      if !self.keys_equal( data ) { return None; }

      let id = util::getu64( data, self.id_off );
      return self.table.id_get( &self.db, id );
    }
    None 
  }
}

/// Fetch record with specified id.
pub struct IdScan
{
  id: i64,
  table: TablePtr,
  db: DB,
  done: bool,
}

impl Iterator for IdScan
{
  type Item = ( PagePtr, usize );
  fn next(&mut self) -> Option<<Self as Iterator>::Item> 
  { 
    if self.done { return None; }
    self.done = true;
    self.table.id_get( &self.db, self.id as u64 )
  }
}
