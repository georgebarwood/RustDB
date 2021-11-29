//!# Interface
//!
//!The method [Database]::run (or alternatively Database::run_timed) is called to execute an SQL query.
//!This takes a [Query] parameter which accumulates SELECT results and which also has methods
//!for accessing the environment and controlling output. Custom builtin functions implement [CExp] and have access to the query
//!via an [EvalEnv] parameter, which can be downcast if ncessary.   
//!
//!# Examples
//! ```
//!use rustdb::{Database, SharedPagedData, SimpleFileStorage, WebQuery, INITSQL};
//!use std::net::TcpListener;
//!use std::sync::Arc;
//!
//!    let sfs = Box::new(SimpleFileStorage::new(
//!        "c:\\Users\\pc\\rust\\sftest01.rustdb",
//!    ));
//!    let spd = Arc::new(SharedPagedData::new(sfs));
//!    let apd = spd.open_write();
//!    let db = Database::new(apd, INITSQL);
//!
//!    let listener = TcpListener::bind("127.0.0.1:3000").unwrap();
//!    for tcps in listener.incoming() {
//!        if let Ok(mut tcps) = tcps {
//!            if let Ok(mut wq) = WebQuery::new(&tcps) {
//!                // wq.trace();
//!                let sql = "EXEC web.Main()";
//!                // Execute SQL. http response, SQL output, (status,headers,content) is accumulated in wq.
//!                db.run_timed(&sql, &mut wq);
//!                // Write the http response to the TCP stream.
//!                let _err = wq.write(&mut tcps);
//!                // Save database changes to disk.
//!                db.save();
//!            }
//!        }
//!    }
//!```
//!
//![See here](https://github.com/georgebarwood/RustDB/blob/main/examples/axumtest.rs) for more advanced example (Axum webserver with ARGON hash function).
//!
//!# Features
//!
//! This crate supports two cargo features.
//! - `builtin` : Allows extra SQL builtin functions to be defined.
//! - `max` : Exposes maximal interface, including all internal modules (default).
//!
//!# General Design of Database
//!
//!SortedFile stores fixed size Records in a tree of Pages.
//!SortedFile is used to implement:
//!
//! - Variable length values ( which are split into fragments - see bytes module - although up to 15 bytes can be stored directly. ).
//!
//! - Database Table storage. Each record has a 64-bit Id.
//!
//! - Index storage ( an index record refers back to the main table ).
//!
//!Pages have a maximum size, and are stored in CompactFile, which stores logical pages in smaller regions of backing storage.
//!
//!When a page becomes too big, it is split into two pages.
//!
//!Each page is implemented as a binary tree ( so there is a tree of trees ).
//!
//!# ToDo List
//!
//!Implement DROP INDEX, ALTER TABLE, fully implement CREATE INDEX.
//!
//!Consider replication/backup/durability issues [Here](https://en.wikipedia.org/wiki/Durability_(database_systems))
//!
//! Have a replication server. Replication server has old copy of database, and log.
//! Changes are sent to replication server, which adds changes to the log.
//! At some point the old copy can be updated ( and the history is lost ).
//! Replication server could be set to run 1 week behind.  
//! Can set up extra replication servers from primary replication server.
//! During cloning process, the replication server stops applying updates.
//! Database at a paticular point in time can be recreated by running replication log forwards.
//! Question is WHAT to sent to replication server?
//! Seems logical to send update messages ( SQL strings and environment values ).
//!
//!Sort out error handling for PARSEINT etc.
//!
//!Work on improving/testing SQL code, browse schema, float I/O. Login.
//!

pub use crate::{
    genquery::{GenQuery, Part},
    init::INITSQL,
    pstore::{AccessPagedData, SharedPagedData},
    stg::SimpleFileStorage,
    web::WebQuery,
};

#[cfg(feature = "builtin")]
pub use crate::{
    builtin::check_types,
    compile::{c_bool, c_float, c_int, c_value},
    exec::EvalEnv,
    expr::{Block, DataKind, Expr},
    run::{CExp, CExpPtr, CompileFunc},
    value::Value,
};
#[cfg(not(feature = "builtin"))]
use crate::{
    compile::{c_bool, c_int, c_value},
    exec::EvalEnv,
    expr::{Block, DataKind, Expr},
    run::{CExp, CExpPtr, CompileFunc},
    value::Value,
};

use crate::{
    bytes::ByteStorage,
    compact::CompactFile,
    expr::*,
    page::{Page, PagePtr},
    parse::Parser,
    run::*,
    sortedfile::{Asc, Id, Record, SortedFile},
    stg::Storage,
    table::{ColInfo, IndexInfo, Row, SaveOp, Table, TablePtr},
    util::{newmap, SmallSet},
    value::*,
};

use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    panic,
    rc::Rc,
    sync::{Arc, Mutex, RwLock},
};

/// Utility functions and macros, [SmallSet].
#[cfg(feature = "max")]
#[macro_use]
pub mod util;
#[cfg(not(feature = "max"))]
#[macro_use]
mod util;

// Modules that are always public.

/// General Query.
pub mod genquery;

/// WebQuery struct with http support.
pub mod web;

/// Initial SQL
pub mod init;

/// Backing storage for database.
pub mod stg;

/// Page storage.
pub mod pstore;

// Conditional modules.

// #[cfg(target_os = "windows")]
// Optimised implementatation of [Storage] (windows only).
// This didn't work out - actually ran slower!
// pub mod stgwin;

#[cfg(feature = "max")]
/// Compilation of builtin functions.
pub mod builtin;
#[cfg(not(feature = "max"))]
mod builtin;

#[cfg(feature = "max")]
/// Storage of variable length values : [ByteStorage].
pub mod bytes;
#[cfg(not(feature = "max"))]
mod bytes;

#[cfg(feature = "max")]
/// Structs that implement [CExp] trait.
pub mod cexp;
#[cfg(not(feature = "max"))]
mod cexp;

#[cfg(feature = "max")]
/// [CompactFile] : storage of logical pages in smaller regions of backing storage.
pub mod compact;
#[cfg(not(feature = "max"))]
mod compact;

#[cfg(feature = "builtin")]
/// Functions to compile parsed expressions, checking types.
pub mod compile;
#[cfg(not(feature = "builtin"))]
mod compile;

#[cfg(feature = "max")]
/// Instruction execution.
pub mod exec;
#[cfg(not(feature = "max"))]
mod exec;

#[cfg(feature = "builtin")]
/// Expression types, result of parsing.
pub mod expr;
#[cfg(not(feature = "builtin"))]
mod expr;

#[cfg(feature = "max")]
/// [Page] for [SortedFile].
pub mod page;
#[cfg(not(feature = "max"))]
mod page;

#[cfg(feature = "max")]
/// [Parser].
pub mod parse;
#[cfg(not(feature = "max"))]
mod parse;

#[cfg(feature = "max")]
/// [Instruction] and other run time types.
pub mod run;
#[cfg(not(feature = "max"))]
mod run;

#[cfg(feature = "max")]
/// Sorted [Record] storage.
pub mod sortedfile;
#[cfg(not(feature = "max"))]
mod sortedfile;

#[cfg(feature = "max")]
/// System table functions.
pub mod sys;
#[cfg(not(feature = "max"))]
mod sys;

#[cfg(feature = "max")]
/// [Table], [ColInfo], [Row] and other Table types.
pub mod table;
#[cfg(not(feature = "max"))]
mod table;

#[cfg(feature = "max")]
/// Run-time [Value].
pub mod value;
#[cfg(not(feature = "max"))]
mod value;

// End of modules.

/// ```Arc<Vec<u8>>```
pub type Data = Arc<Vec<u8>>;

/// ```Rc<Database>```
pub type DB = Rc<Database>;

/// Database with SQL-like interface.
pub struct Database {
    /// Page storage.
    pub file: AccessPagedData,
    // System tables.
    pub sys_schema: TablePtr,
    pub sys_table: TablePtr,
    pub sys_column: TablePtr,
    pub sys_index: TablePtr,
    pub sys_index_col: TablePtr,
    pub sys_function: TablePtr,
    /// Storage of variable length data.
    pub bs: ByteStorage,
    // Various maps for named database objects.
    pub schemas: RefCell<HashMap<String, i64>>,
    pub tables: RefCell<HashMap<ObjRef, TablePtr>>,
    pub functions: RefCell<HashMap<ObjRef, FunctionPtr>>,
    pub builtins: RefCell<HashMap<String, (DataKind, CompileFunc)>>,
    /// Flag to reset the functions cache after save.
    pub function_reset: Cell<bool>,
    /// Last id generated by INSERT.
    pub lastid: Cell<i64>,
    /// Has there been an error since last save?
    pub err: Cell<bool>,
}
impl Database {
    /// Construct a new DB, based on the specified file.
    pub fn new(file: AccessPagedData, initsql: &str) -> DB {
        let mut dq = DummyQuery {};
        let is_new = file.is_new();
        let mut tb = TableBuilder::new();
        let sys_schema = tb.nt("Schema", &[("Name", STRING)]);
        let sys_table = tb.nt(
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
            "Column",
            &[("Table", BIGINT), ("Name", STRING), ("Type", BIGINT)],
        );
        let sys_index = tb.nt(
            "Index",
            &[("Root", BIGINT), ("Table", BIGINT), ("Name", STRING)],
        );
        let sys_index_col = tb.nt("IndexColumn", &[("Index", BIGINT), ("ColId", BIGINT)]);
        let sys_function = tb.nt(
            "Function",
            &[("Schema", BIGINT), ("Name", STRING), ("Def", STRING)],
        );
        sys_schema.add_index(7, vec![0]);
        sys_table.add_index(8, vec![1, 2]);
        sys_column.add_index(9, vec![0]);
        sys_index.add_index(10, vec![1]);
        sys_index_col.add_index(11, vec![0]);
        sys_function.add_index(12, vec![0, 1]);

        let db = Rc::new(Database {
            file,
            sys_schema,
            sys_table,
            sys_column,
            sys_index,
            sys_index_col,
            sys_function,
            bs: ByteStorage::new(0),
            schemas: newmap(),
            functions: newmap(),
            tables: newmap(),
            builtins: newmap(),
            function_reset: Cell::new(false),
            lastid: Cell::new(0),
            err: Cell::new(false),
        });
        if is_new {
            db.alloc_page(); // Allocate page for byte storage.
        }
        db.bs.init(&db);
        for t in &tb.list {
            if !is_new {
                t.id_gen.set(sys::get_id_gen(&db, t.id as u64));
            }
            db.publish_table(t.clone());
        }
        if is_new {
            // The creation order has to match the order above ( so root values are as predicted ).
            let sysinit = "
CREATE SCHEMA sys
GO
CREATE TABLE sys.Schema( Name string )
CREATE TABLE sys.Table( Root bigint, Schema bigint, Name string, IsView tinyint, Def string, IdGen bigint )
CREATE TABLE sys.Column( Table bigint, Name string, Type bigint )
CREATE TABLE sys.Index( Root bigint, Table bigint, Name string )
CREATE TABLE sys.IndexColumn( Index bigint, ColId bigint )
CREATE TABLE sys.Function( Schema bigint, Name string, Def string )
GO
CREATE INDEX ByName ON sys.Schema(Name)
CREATE INDEX BySchemaName ON sys.Table(Schema,Name)
CREATE INDEX ByTable ON sys.Column(Table)
CREATE INDEX ByTable ON sys.Index(Table)
CREATE INDEX ByIndex ON sys.IndexColumn(Index)
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

    #[cfg(feature = "builtin")]
    /// Register a builtin function.
    pub fn register(self: &DB, name: &str, typ: DataKind, cf: CompileFunc) {
        self.builtins
            .borrow_mut()
            .insert(name.to_string(), (typ, cf));
    }

    #[cfg(not(feature = "builtin"))]
    /// Register a builtin function.
    fn register(self: &DB, name: &str, typ: DataKind, cf: CompileFunc) {
        self.builtins
            .borrow_mut()
            .insert(name.to_string(), (typ, cf));
    }

    /// Run a batch of SQL.
    pub fn run(self: &DB, source: &str, qy: &mut dyn Query) {
        if let Some(e) = self.go(source, qy) {
            let err = format!(
                "{} in {} at line {} column {}.",
                e.msg, e.rname, e.line, e.column
            );
            println!("Run error {}", &err);
            qy.set_error(err);
            self.err.set(true);
        }
    }
    /// Run a batch of SQL, printing the execution time.
    pub fn run_timed(self: &DB, source: &str, qy: &mut dyn Query) {
        let start = std::time::Instant::now();
        self.run(source, qy);
        println!(
            "run_timed path={} run time={} micro sec.",
            qy.arg(0, ""),
            start.elapsed().as_micros()
        );
    }
    /// Run a batch of SQL.
    pub fn go(self: &DB, source: &str, qy: &mut dyn Query) -> Option<SqlError> {
        let mut p = Parser::new(source, self);
        let result = std::panic::catch_unwind(panic::AssertUnwindSafe(|| {
            p.batch(qy);
        }));
        if let Err(x) = result {
            Some(if let Some(e) = x.downcast_ref::<SqlError>() {
                SqlError {
                    msg: e.msg.clone(),
                    line: e.line,
                    column: e.column,
                    rname: e.rname.clone(),
                }
            } else if let Some(s) = x.downcast_ref::<&str>() {
                p.make_error((*s).to_string())
            } else if let Some(s) = x.downcast_ref::<String>() {
                p.make_error(s.to_string())
            } else {
                p.make_error("unrecognised/unexpected error".to_string())
            })
        } else {
            None
        }
    }

    /// Save updated tables to underlying file ( or rollback if there was an error ).
    /// Returns the number of logical pages that were updated.
    pub fn save(self: &DB) -> usize {
        let op = if self.err.get() {
            self.err.set(false);
            SaveOp::RollBack
        } else {
            SaveOp::Save
        };
        self.bs.save(self, op);
        let tm = &*self.tables.borrow();
        for t in tm.values() {
            if t.id_gen_dirty.get() {
                if op == SaveOp::Save {
                    sys::save_id_gen(self, t.id as u64, t.id_gen.get());
                } else {
                    t.id_gen.set(sys::get_id_gen(self, t.id as u64));
                }
                t.id_gen_dirty.set(false);
            }
        }
        for t in tm.values() {
            t.save(self, op);
        }
        if self.function_reset.get() {
            for function in self.functions.borrow().values() {
                function.ilist.borrow_mut().clear();
            }
            self.functions.borrow_mut().clear();
            self.function_reset.set(false);
        }
        self.file.save(op)
    }

    /// Get the named table.
    pub fn get_table(self: &DB, name: &ObjRef) -> Option<TablePtr> {
        if let Some(t) = self.tables.borrow().get(name) {
            return Some(t.clone());
        }
        sys::get_table(self, name)
    }

    /// Get the named function.
    pub fn get_function(self: &DB, name: &ObjRef) -> Option<FunctionPtr> {
        if let Some(f) = self.functions.borrow().get(name) {
            return Some(f.clone());
        }
        sys::get_function(self, name)
    }

    /// Insert the table into the map of tables.
    pub fn publish_table(&self, table: TablePtr) {
        let name = table.info.name.clone();
        self.tables.borrow_mut().insert(name, table);
    }

    /// Get code for value.
    pub fn encode(self: &DB, val: &Value) -> u64 {
        let bytes = match val {
            Value::RcBinary(x) => &**x,
            Value::ArcBinary(x) => &**x,
            Value::String(x) => x.as_bytes(),
            _ => {
                return u64::MAX;
            }
        };
        if bytes.len() < 16 {
            return u64::MAX;
        }
        self.bs.encode(self, &bytes[7..])
    }

    /// Decode u64 to bytes.
    pub fn decode(self: &DB, code: u64) -> Vec<u8> {
        self.bs.decode(self, code)
    }

    /// Delete encoding.
    pub fn delcode(self: &DB, code: u64) {
        self.bs.delcode(self, code);
    }

    /// Allocate a page of underlying file storage.
    pub fn alloc_page(self: &DB) -> u64 {
        self.file.alloc_page()
    }

    /// Free a page of underyling file storage.
    pub fn free_page(self: &DB, lpnum: u64) {
        self.file.free_page(lpnum);
    }
} // end impl Database

impl Drop for Database {
    /// Clear function instructions to avoid leaking memory.
    fn drop(&mut self) {
        for function in self.functions.borrow().values() {
            function.ilist.borrow_mut().clear();
        }
    }
}

/// For creating system tables.
struct TableBuilder {
    alloc: i64,
    list: Vec<TablePtr>,
}
impl TableBuilder {
    fn new() -> Self {
        Self {
            alloc: 1,
            list: Vec::new(),
        }
    }
    fn nt(&mut self, name: &str, ct: &[(&str, DataType)]) -> TablePtr {
        let id = self.alloc;
        let root_page = id as u64;
        self.alloc += 1;
        let name = ObjRef::new("sys", name);
        let info = ColInfo::new(name, ct);
        let table = Table::new(id, root_page, 1, Rc::new(info));
        self.list.push(table.clone());
        table
    }
}

/// Input/Output message. Query and response.
pub trait Query: std::any::Any {
    /// STATUSCODE builtin function. sets the response status code.
    fn status_code(&mut self, _code: i64) {}

    /// HEADER builtin function, adds header to response.
    fn header(&mut self, _name: &str, _value: &str) {}

    /// Append SELECT values to response body.
    fn selected(&mut self, values: &[Value]);

    /// GLOBAL builtin function. Used to get request time.
    fn global(&self, _kind: i64) -> i64 {
        0
    }

    /// ARG builtin function. Get path, query parameter, form value or cookie.
    fn arg(&mut self, _kind: i64, _name: &str) -> Rc<String> {
        Rc::new(String::new())
    }

    /// Get file attribute ( One of name, content_type, file_name )
    fn file_attr(&mut self, _fnum: i64, _atx: i64) -> Rc<String> {
        Rc::new(String::new())
    }

    /// Get file content.
    fn file_content(&mut self, _fnum: i64) -> Arc<Vec<u8>> {
        Arc::new(Vec::new())
    }

    /// Set the error string.
    fn set_error(&mut self, err: String);

    /// Get the error string.
    fn get_error(&mut self) -> String {
        String::new()
    }
}

/// Query where output is printed to console (used for initialisation ).
struct DummyQuery {}
impl Query for DummyQuery {
    fn selected(&mut self, _values: &[Value]) {}
    /// Called if a panic ( error ) occurs.
    fn set_error(&mut self, err: String) {
        println!("Error: {}", err);
    }
}
