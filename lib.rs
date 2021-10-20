//! Database with SQL-like language.
//! Example program:
//! ```
//! use std::net::TcpListener;
//! use database::{Database,spf::SimplePagedFile,web::WebQuery};
//! fn main() 
//! {
//!   let file = Box::new( SimplePagedFile::new( "c:\\Users\\pc\\rust\\test01.rustdb" ) );
//!   let db = Database::new( file, INITSQL );    
//!   let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
//!   for tcps in listener.incoming() 
//!   {
//!     let mut tcps = tcps.unwrap();
//!     let mut wq = WebQuery::new( &tcps ); // Reads the http request from the TCP stream into wq.
//!     db.run( SQL, &mut wq ); // Executes SQL, output is accumulated in wq.
//!     wq.write( &mut tcps ); // Writes the http response to the TCP stream.
//!     db.save(); // Saves database changes to disk.
//!   }
//! }
//! const SQL : &str = "SELECT 'hello world'";
//! const INITSQL : &str = "";
//!```
//!
//!
//!General Design of Database
//!
//!Lowest level is SortedFile which stores fixed size Records.
//!
//!SortedFile is used to implement:
//!
//!(1) Variable length values ( which are split into fragments - see bytes module ).
//!
//!(2) Database Table storage. Each record has a 64-bit Id.
//!
//!(3) Index storage ( an index record refers back to the main table ). This is ToDo.
//!
//!Write transactions ( which modify the database ) are expected to be short.
//!
//!Only one write transaction runs at a time.
//!
//!Read transactions may be much longer, but they do not block write transactions.

/* 
Next: EditRoutine and ALTER ROUTINE/ALTER FUNCTION.
Next: get floating point and decimal working.
Next: get EditRow, AddRow working.
Next: implement multipart requests ( for file upload )

Implement Dump()?

Store short strings inline ( say up to 15 bytes ).
  First byte has string length ( 255 means unknown, more than 254 bytes ).
  Next 7 bytes are start of string.
  Next 8 bytes are either rest of string, or pointer into ByteStorage.
*/

use std::{ panic, cell::RefCell, rc::Rc, cell::Cell, collections::HashMap };
use crate::{ bytes::ByteStorage, run::RoutinePtr, compile::CompileFunc,
  table::{Table,TablePtr}, sql::{DK,ObjRef,STRING,BIGINT,TINYINT,SqlError} };

/// WebQuery struct for making a http web server.
pub mod web;

/// Structured Query Language : various types.
pub mod sql; 

/// SQL parser.
pub mod sqlparse;

/// Compile parsed expressions, checking types.
pub mod compile;

/// Simple Paged File.
pub mod spf;

// Remaining mmodules need not be pub, made pub to expose inner workings of crate. 

/// Utility functions.
#[macro_use] pub mod util;

/// Functions to create system objects (Schema,Table,Routine).
pub mod sys;

/// Sorted Record storage : SortedFile.
pub mod sf;

/// Page for SortedFile.
///
/// A page is 0x4000 (16kb) bytes, logically divided into up to 2047 fixed size nodes, which implement a balanced binary tree.
///
/// Nodes are numbered from 1..2047, with 0 indicating a null ( non-existent ) node.
/// 
/// Each record has a 3 byte overhead, 2 bits to store the balance, 2 x 11 bits to store left and right node ids. 
pub mod page;

/// Table : TableInfo, Row, other Table types.
pub mod table;

/// SQL execution : Instruction (Inst) and other run time types.
pub mod run;

/// SQL execution : EvalEnv struct.
pub mod eval; 

/// CExp implementations for basic expressions.
pub mod cexp;

/// Storage of variable length values : ByteStorage.
pub mod bytes;

/// Compilation of SQL builtin functions.
pub mod builtin;

/// ```Rc<Database>```
pub type DB = Rc<Database>;

// End of modules.

/// Database with SQL-like interface.
pub struct Database
{
  /// Page storage.
  pub file: Box<dyn PagedFile>,
  pub sys_schema: TablePtr, 
  pub sys_table: TablePtr,
  pub sys_col: TablePtr,
  /// Database is newly created.
  pub bs: ByteStorage,
  pub routines: RefCell<HashMap<ObjRef, RoutinePtr>>,
  pub tables: RefCell<HashMap<ObjRef, TablePtr>>,
  pub builtins: RefCell<HashMap<String,(DK,CompileFunc)>>,
  pub routines_dirty: Cell<bool>,
}

impl Database
{
  /// Construct a new DB, based on the specified file.
  pub fn new( file: Box<dyn PagedFile>, initsql: &str ) -> DB
  {
    let mut crs = ConsoleQuery::new( 0 );

    let is_new = file.is_new();

    // Note: bs (byte storage) uses root page 0.
    let sys_schema = Table::new0( 0, 1, "sys", "Schema", &[ ( "Name", STRING ) ] );
    let sys_table = Table::new0( 1, 2, "sys", "Table", &[ 
       ("Root",BIGINT),
       ("Schema",BIGINT), 
       ("Name",STRING),
       ("IsView",TINYINT),
       ("Definition",STRING),
       ("IdGen",BIGINT)
    ]);

    let sys_col = Table::new0( 2, 3, "sys", "Column", &[ 
       ( "Table", BIGINT ),
       ( "Name", STRING ),
       ( "Type", BIGINT ) 
    ]);

    let db = Rc::new( Database
    { file, sys_schema, sys_table, sys_col,
      bs: ByteStorage::new( 0 ),
      routines: RefCell::new( HashMap::new() ),
      tables: RefCell::new( HashMap::new() ),
      builtins: RefCell::new( HashMap::new() ),  
      routines_dirty: Cell::new( false ),
    } );

    db.bs.init( &db );    

    db.init( &db.sys_schema, is_new );
    db.init( &db.sys_table, is_new );
    db.init( &db.sys_col, is_new );

    if is_new 
    {
      println!( "New database... initialising" );
      let sysinit = "
CREATE SCHEMA sys
GO
CREATE TABLE sys.Schema( Name string )
CREATE TABLE sys.Table( Root bigint, Schema bigint, Name string, IsView tinyint, Def string, IdGen bigint )
CREATE TABLE sys.Column( Table bigint, Name string, Type bigint )
CREATE TABLE sys.Routine( Schema bigint, Name string, Def string )
GO
";
      db.run( sysinit, &mut crs );
      db.run( initsql, &mut crs );
    }
    builtin::register_builtins( &db );
    db
  }

  /// Register a builtin function.
  pub fn register( self: &DB, name: &str, typ: sql::DK, cf: CompileFunc )
  {
    self.builtins.borrow_mut().insert( name.to_string(), (typ,cf) );
  }

  /// Run a batch of SQL.
  pub fn run( self: &DB, source: &str, qy: &mut dyn Query )
  {
    if let Some(e) = self.go( source, qy )
    {
      let err = format!( "Error : {} in {} at line {} column {}.", e.msg, e.rname, e.line, e.column );
      println!( "Run error {}", &err );
      qy.set_error( err );
    }
  }

  /// Run a batch of SQL, printing the execution time.
  pub fn runtimed( self: &DB, source: &str, qy: &mut dyn Query )
  {
    let start = std::time::Instant::now();
    self.run( source, qy );
    println!( "db run time={} micro sec.", start.elapsed().as_micros() );
  }

  /// Run a batch of SQL.
  fn go( self: &DB, source: &str, qy: &mut dyn Query ) -> Option<SqlError>
  {
    let mut p = sqlparse::Parser::new( source, self );
    
    let result = std::panic::catch_unwind(panic::AssertUnwindSafe( || 
    { 
      p.batch( qy ); 
    }));

    if let Err(x) = result
    {
      Some(
        if let Some(e) = x.downcast_ref::<SqlError>()
        {
          SqlError{ msg:e.msg.clone(), line: e.line, column:e.column, rname:e.rname.clone() }
        }
        else if let Some(s) = x.downcast_ref::<&str>()
        {
          p.make_error(s.to_string())
        }
        else if let Some(s) = x.downcast_ref::<String>()
        {
          p.make_error(s.to_string())
        }
        else
        {
          p.make_error("unrecognised/unexpected error".to_string())
        }
      )
    }
    else
    {
      None
    }
  }

  /// Save updated tables to file.
  pub fn save( self: &DB )
  {
    self.bs.save( self );

    let tm = &*self.tables.borrow();
    for t in tm.values()
    {
      let (id, alloc, dirty) =
      {
        ( t.id, t.id_alloc.get(), t.id_alloc_dirty.get() )
      };      
      if dirty
      {
        sys::save_alloc( self, id, alloc );
      }
    }

    for t in tm.values()
    {
      t.save( self );
    }

    if self.routines_dirty.get()
    {
      for routine in self.routines.borrow().values()
      {
        routine.ilist.borrow_mut().clear();
      }
      self.routines.borrow_mut().clear();
      self.routines_dirty.set( false );
    }
  }

  /// Print the tables ( for debugging ).
  pub fn dump_tables( self: &DB )
  {
    println!( "Byte Storage" );
    self.bs.file.dump();

    for (n,t) in &*self.tables.borrow()
    {
      println!( "Dump Table {:?} {:?}", n, t.info.colnames );
      t._dump( self );
    }  
  }

  /// Initialise and publish system table.
  fn init( self: &DB, t: &TablePtr, is_new: bool )
  {
    if !is_new 
    { 
      t.id_alloc.set( sys::get_alloc( self, t.id ) );
    }
    self.publish_table( t.clone() );
  }

  /// Load the named table.
  pub(crate) fn load_table( self: &DB, name: &ObjRef ) -> Option< TablePtr >
  {
    if let Some(r) = self.tables.borrow().get( name )
    {
      return Some( r.clone() );
    }
    sys::get_table( self, name ) 
  }

  /// Load the named routine.
  pub(crate) fn load_routine( self: &DB, name: &ObjRef ) -> Option< RoutinePtr >
  {
    if let Some(r) = self.routines.borrow().get( name )
    {
      return Some( r.clone() );
    }
    sys::get_routine( self, name )
  }

  /// Insert the table into the map of tables.
  pub(crate) fn publish_table( &self, table: TablePtr )
  {
    let name = table.info.name.clone();
    self.tables.borrow_mut().insert( name, table );
  }

  /// Insert the routine into the map of tables.
  pub(crate) fn publish_routine( &self, name: &ObjRef, routine: RoutinePtr )
  {
    self.routines.borrow_mut().insert( name.clone(), routine );
  }

  /// Encode byte slice as u64.
  pub(crate) fn encode( self: &DB, bytes: &[u8] ) -> u64
  {
    self.bs.encode( self, bytes )
  }

  /// Decode u64 to bytes.
  pub(crate) fn decode( self: &DB, code: u64 ) -> Vec<u8>
  {
    self.bs.decode( self, code )
  }

  /// Delete encoding.
  pub(crate) fn delcode( self: &DB, code: u64 )
  {
    self.bs.delcode( self, code );
  }


} // end impl Database

impl Drop for Database
{
  /// Clear routine instructions to avoid leaking memory.
  fn drop(&mut self) 
  { 
    for routine in self.routines.borrow().values()
    {
      routine.ilist.borrow_mut().clear();
    }
  }
}

/// Simple value ( bool, integer, float, string, binary ).
#[derive(Debug,Clone)]
pub enum Value
{
  None,
  Bool(bool),
  Int(i64),
  Float(f64),
  String(Rc<String>),
  Binary(Rc<Vec<u8>>),
  For(Rc<RefCell<run::ForState>>),
  ForSort(Rc<RefCell<run::ForSortState>>)
}

impl Value
{
  pub fn str( &self ) -> Rc<String>
  {
    match self
    {
      Value::String(s) => s.clone(),
      Value::Int(x) => Rc::new(x.to_string()),
      _ => Rc::new(format!("unexpected {:?}", self )),
    }
  }
}

/// Backing storage for database tables.
pub trait PagedFile
{
  fn read_page( &self, pnum: u64, data: &mut [u8] );
  fn write_page( &self, pnum: u64, data: &[u8] );
  fn alloc_page( &self ) -> u64;  
  fn is_new( &self ) -> bool;
}

/// IO Methods.
pub trait Query
{
  fn push( &mut self, values: &[Value] );
  // fn begin_table( &mut self, _colnames: &[String] ){}
  // fn end_table( &mut self ){}

  // May want put_header method to write contenttype, cookie and other headers.

  /// ARG builtin function.
  fn arg( &self, _kind: i64, _name: &str ) -> Rc<String> { Rc::new(String::new()) }

  /// GLOBAL builtin function.
  fn global( &self, _kind: i64 ) -> i64 { 0 }

  /// Called when a panic ( error ) occurs.
  fn set_error( &mut self, err: String );

  /// EXCEPTION builtin function.
  fn get_error( &mut self ) -> String{ String::new() }
}

/// Query where output is printed to console.
pub struct ConsoleQuery
{
  pub mode: i8,
}

impl ConsoleQuery
{
  pub fn new( mode: i8 ) -> Self
  {
    Self{ mode }
  }
}

impl Query for ConsoleQuery
{
  fn push( &mut self, values: &[Value] )
  {
    println!( "{:?}", values );
  }

  /// Called when a panic ( error ) occurs.
  fn set_error( &mut self, err: String )
  {
    println!( "Error: {}", err );
  }
}