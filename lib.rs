//!
//!ToDo List:
//!
//!Optimise WHERE condition for UPDATE and DELETE.
//!
//!Decimal shifting when scales do not match.
//!
//!Multi-column index use from WHERE (Done).
//!
//!multipart requests ( for file upload ).
//!
//!Implement DROP TABLE(done), DROP INDEX, DROP FUNCTION(done) etc.
//!
//!Implement ALTER TABLE?
//!
//!Fully implement CREATE INDEX?
//!
//!Sort out error handling for PARSEINT etc.
//!
//!Handle HTTP IO in parallel. Read-only transactions.
//!
//!Allow new data types to be registered?
//!
//!Work on improving SQL code, browse schema. Scripting of individual schemas, preservation of browse data.
//!
//! Database with SQL-like language.
//! Example program:
//! ```
//! use std::net::TcpListener;
//! use database::{Database,web::WebQuery};
//! fn main()
//! {
//!     let stg = Box::new(database::stg::SimpleFileStorage::new(
//!         "c:\\Users\\pc\\rust\\doctest01.rustdb",
//!     ));
//!     let db = Database::new( stg, INITSQL );    
//!     let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
//!     for tcps in listener.incoming()
//!     {
//!        let mut tcps = tcps.unwrap();
//!        let mut wq = WebQuery::new( &tcps ); // Reads the http request from the TCP stream into wq.
//!        db.run( SQL, &mut wq ); // Executes SQL, SELECT output is accumulated in wq.
//!        wq.write( &mut tcps ); // Writes the http response to the TCP stream.
//!        db.save(); // Saves database changes to disk.
//!     }
//! }
//! const SQL : &str = "SELECT 'hello world'";
//! const INITSQL : &str = "";
//!```
//!
//!General Design of Database
//!
//!SortedFile stores fixed size Records in a tree of Pages.
//!SortedFile is used to implement:
//!
//!(1) Variable length values ( which are split into fragments - see bytes module - although up to 15 bytes can be stored directly. ).
//!
//!(2) Database Table storage. Each record has a 64-bit Id.
//!
//!(3) Index storage ( an index record refers back to the main table ).
//!
//!Pages have a maximum size, and are stored in stg::CompactFile, which stores logical pages in smaller regions of backing storage.
//!
//!When a page becomes too big, it is split into two pages.
//!
//!Each page is implemented as a binary tree ( so there is a tree of trees ).

use crate::{
  bytes::*, compile::*, exec::*, expr::*, page::*, parse::*, run::*, sortedfile::*, stg::*, table::*, util::newmap,
  value::*,
};
use std::{
  cell::Cell,
  cell::RefCell,
  cmp::Ordering,
  collections::{HashMap},
  panic,
  rc::Rc,
};
/// Utility functions and macros.
#[macro_use]
mod util;
/// Compilation of builtin functions.
mod builtin;
/// Storage of variable length values : ByteStorage.
mod bytes;
/// Compiled expressions.
mod cexp;
/// Compile parsed expressions, checking types.
pub mod compile;
/// Instruction execution.
mod exec;
/// Expression types, result of parsing.
pub mod expr;
/// Initial SQL
pub mod init;
/// Page for SortedFile.
pub mod page;
/// Parser.
mod parse;
/// Instruction and other run time types.
mod run;
/// Sorted Record storage.
pub mod sortedfile;
/// Storage of logical pages in smaller regions of backing storage.
pub mod stg;
/// System table functions.
mod sys;
/// Table, ColInfo, Row and other Table types.
pub mod table;
/// Run-time Value.
pub mod value;
/// WebQuery struct for making a http web server.
pub mod web;
// End of modules.
/// ```Rc<Database>```
pub type DB = Rc<Database>;
/// Database with SQL-like interface.
pub struct Database
{
  /// Page storage.
  pub file: RefCell<CompactFile>,
  // System tables.
  sys_schema: TablePtr,
  sys_table: TablePtr,
  sys_column: TablePtr,
  sys_index: TablePtr,
  sys_index_col: TablePtr,
  /// Storage of variable length data.
  bs: ByteStorage,
  // Various maps for named database objects.
  schemas: RefCell<HashMap<String, i64>>,
  tables: RefCell<HashMap<ObjRef, TablePtr>>,
  functions: RefCell<HashMap<ObjRef, FunctionPtr>>,
  builtins: RefCell<HashMap<String, (DataKind, CompileFunc)>>,
  /// Flag to reset the functions cache after save.
  function_reset: Cell<bool>,
  /// Last id generated by INSERT.
  lastid: Cell<i64>,
  /// Has there been an error since last save?
  pub err: Cell<bool>,
}
impl Database
{
  /// Construct a new DB, based on the specified file.
  pub fn new(mut stg: Box<dyn Storage>, initsql: &str) -> DB
  {
    let mut dq = DummyQuery {};
    let is_new = stg.size() == 0;
    let mut tb = TableBuilder::new();
    let sys_schema = tb.nt("sys", "Schema", &[("Name", STRING)]);
    let sys_table = tb.nt(
      "sys",
      "Table",
      &[
        ("Root", BIGINT),
        ("Schema", BIGINT),
        ("Name", STRING),
        ("IsView", TINYINT),
        ("Def", STRING),
        ("IdGen", BIGINT),
      ],
    );
    let sys_column = tb.nt(
      "sys",
      "Column",
      &[("Table", BIGINT), ("Name", STRING), ("Type", BIGINT)],
    );
    let sys_index = tb.nt("sys", "Index", &[("Root", BIGINT), ("Table", BIGINT), ("Name", STRING)]);
    let sys_index_col = tb.nt("sys", "IndexColumn", &[("Index", BIGINT), ("ColId", BIGINT)]);
    sys_table.add_index(6, vec![1, 2]);
    sys_column.add_index(7, vec![0]);
    sys_index.add_index(8, vec![1]);
    sys_index_col.add_index(9, vec![0]);
    let db = Rc::new(Database {
      file: RefCell::new(CompactFile::new(stg, 400, 1024)),
      sys_schema,
      sys_table,
      sys_column,
      sys_index,
      sys_index_col,
      bs: ByteStorage::new(0),
      schemas: newmap(),
      functions: newmap(),
      tables: newmap(),
      builtins: newmap(),
      function_reset: Cell::new(false),
      lastid: Cell::new(0),
      err: Cell::new(false),
    });
    if is_new
    {
      db.alloc_page(); // Allocate page for byte storage.
    }
    db.bs.init(&db);
    for t in &tb.list
    {
      if !is_new
      {
        t.id_gen.set(sys::get_id_gen(&db, t.id as u64));
      }
      db.publish_table(t.clone());
    }
    if is_new
    {
      // The creation order has to match the order above ( so root values are as predicted ).
      let sysinit = "
CREATE SCHEMA sys
GO
CREATE TABLE sys.Schema( Name string )
CREATE TABLE sys.Table( Root bigint, Schema bigint, Name string, IsView tinyint, Def string, IdGen bigint )
CREATE TABLE sys.Column( Table bigint, Name string, Type bigint )
CREATE TABLE sys.Index( Root bigint, Table bigint, Name string )
CREATE TABLE sys.IndexColumn( Index bigint, ColId bigint )
GO
CREATE INDEX BySchemaName ON sys.Table(Schema,Name)
GO
CREATE INDEX ByTable ON sys.Column(Table)
CREATE INDEX ByTable ON sys.Index(Table)
CREATE INDEX ByIndex ON sys.IndexColumn(Index)
GO
CREATE TABLE sys.Function( Schema bigint, Name string, Def string )
GO
CREATE INDEX BySchemaName ON sys.Function(Schema,Name)
GO
";
      db.run(sysinit, &mut dq);
      db.run(initsql, &mut dq);
      db.save();
    }
    builtin::register_builtins(&db);
    db
  }
  /// Register a builtin function.
  pub fn register(self: &DB, name: &str, typ: DataKind, cf: CompileFunc)
  {
    self.builtins.borrow_mut().insert(name.to_string(), (typ, cf));
  }
  /// Run a batch of SQL.
  pub fn run(self: &DB, source: &str, qy: &mut dyn Query)
  {
    if let Some(e) = self.go(source, qy)
    {
      let err = format!("{} in {} at line {} column {}.", e.msg, e.rname, e.line, e.column);
      println!("Run error {}", &err);
      qy.set_error(err);
      self.err.set(true);
    }
  }
  /// Run a batch of SQL, printing the execution time.
  pub fn run_timed(self: &DB, source: &str, qy: &mut dyn Query)
  {
    let start = std::time::Instant::now();
    self.run(source, qy);
    println!("db run time={} micro sec.", start.elapsed().as_micros());
  }
  /// Run a batch of SQL.
  fn go(self: &DB, source: &str, qy: &mut dyn Query) -> Option<SqlError>
  {
    let mut p = Parser::new(source, self);
    let result = std::panic::catch_unwind(panic::AssertUnwindSafe(|| {
      p.batch(qy);
    }));
    if let Err(x) = result
    {
      Some(
        if let Some(e) = x.downcast_ref::<SqlError>()
        {
          SqlError { msg: e.msg.clone(), line: e.line, column: e.column, rname: e.rname.clone() }
        }
        else if let Some(s) = x.downcast_ref::<&str>()
        {
          p.make_error((*s).to_string())
        }
        else if let Some(s) = x.downcast_ref::<String>()
        {
          p.make_error(s.to_string())
        }
        else
        {
          p.make_error("unrecognised/unexpected error".to_string())
        },
      )
    }
    else
    {
      None
    }
  }
  /// Save updated tables to underlying file ( or rollback if there was an error ).
  pub fn save(self: &DB)
  {
    let op = if self.err.get()
    {
      self.err.set(false);
      SaveOp::RollBack
    }
    else
    {
      SaveOp::Save
    };
    self.bs.save(self, op);
    let tm = &*self.tables.borrow();
    for t in tm.values()
    {
      if t.id_gen_dirty.get()
      {
        if op == SaveOp::Save
        {
          sys::save_id_gen(self, t.id as u64, t.id_gen.get());
        }
        else
        {
          t.id_gen.set(sys::get_id_gen(self, t.id as u64));
        }
        t.id_gen_dirty.set(false);
      }
    }
    for t in tm.values()
    {
      t.save(self, op);
    }
    if self.function_reset.get()
    {
      for function in self.functions.borrow().values()
      {
        function.ilist.borrow_mut().clear();
      }
      self.functions.borrow_mut().clear();
      self.function_reset.set(false);
    }
    self.file.borrow_mut().save();

    // self.dump_tables();
  }
  /// Get the named table.
  fn get_table(self: &DB, name: &ObjRef) -> Option<TablePtr>
  {
    if let Some(t) = self.tables.borrow().get(name)
    {
      return Some(t.clone());
    }
    sys::get_table(self, name)
  }
  /// Get the named function.
  fn get_function(self: &DB, name: &ObjRef) -> Option<FunctionPtr>
  {
    if let Some(f) = self.functions.borrow().get(name)
    {
      return Some(f.clone());
    }
    sys::get_function(self, name)
  }
  /// Insert the table into the map of tables.
  fn publish_table(&self, table: TablePtr)
  {
    let name = table.info.name.clone();
    self.tables.borrow_mut().insert(name, table);
  }
  /// Get code for value.
  fn encode(self: &DB, val: &Value) -> u64
  {
    let bytes = match val
    {
      | Value::Binary(x) => x,
      | Value::String(x) => x.as_bytes(),
      | _ =>
      {
        return u64::MAX;
      }
    };
    if bytes.len() < 16
    {
      return u64::MAX;
    }
    self.bs.encode(self, &bytes[7..])
  }
  /// Decode u64 to bytes.
  fn decode(self: &DB, code: u64) -> Vec<u8>
  {
    self.bs.decode(self, code)
  }
  /// Delete encoding.
  fn delcode(self: &DB, code: u64)
  {
    self.bs.delcode(self, code);
  }
  /// Allocate a page of underlying file storage.
  fn alloc_page(self: &DB) -> u64
  {
    self.file.borrow_mut().alloc_page()
  }
  /// Free a pagee of underyling file storage.
  fn free_page(self: &DB, lpnum: u64)
  {
    self.file.borrow_mut().free_page(lpnum);
  }
} // end impl Database
impl Drop for Database
{
  /// Clear function instructions to avoid leaking memory.
  fn drop(&mut self)
  {
    for function in self.functions.borrow().values()
    {
      function.ilist.borrow_mut().clear();
    }
  }
}
/// For creating system tables.
struct TableBuilder
{
  alloc: i64,
  list: Vec<TablePtr>,
}
impl TableBuilder
{
  fn new() -> Self
  {
    Self { alloc: 1, list: Vec::new() }
  }
  fn nt(&mut self, schema: &str, name: &str, ct: &[(&str, DataType)]) -> TablePtr
  {
    let id = self.alloc;
    let root_page = id as u64;
    self.alloc += 1;
    let name = ObjRef::new(schema, name);
    let info = ColInfo::new(name, ct);
    let table = Table::new(id, root_page, 1, Rc::new(info));
    self.list.push(table.clone());
    table
  }
}
/// Input/Output interface.
pub trait Query
{
  /// Append SELECT values to output.
  fn push(&mut self, values: &[Value]);
  /// ARG builtin function.
  fn arg(&mut self, _kind: i64, _name: &str) -> Rc<String>
  {
    Rc::new(String::new())
  }
  /// GLOBAL builtin function.
  fn global(&self, _kind: i64) -> i64
  {
    0
  }
  /// Set the error string.
  fn set_error(&mut self, err: String);
  /// Get the error string.
  fn get_error(&mut self) -> String
  {
    String::new()
  }
}
/// Query where output is printed to console (used for initialisation ).
struct DummyQuery {}
impl Query for DummyQuery
{
  fn push(&mut self, _values: &[Value]) {}
  /// Called if a panic ( error ) occurs.
  fn set_error(&mut self, err: String)
  {
    println!("Error: {}", err);
  }
}
