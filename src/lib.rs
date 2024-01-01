//! This crate (rustdb) implements a high-performance database written entirely in [Rust](https://www.rust-lang.org/).
//!
//! The SQL-like language is relatively minimal, and does not (currently) include features such as joins or views.
//! Instead it has high performance SET .. FROM ... and FOR .. FROM statements to access database tables,
//! generally using an INDEX.
//!
//! Read-only transactions run immediately and concurrently on a virtual read-only copy of the database, and cannot be blocked.
//! Write transactions run sequentially (and should typically execute in around 100 micro-seconds). The [Storage] trait allows a variety of underlying storage, including [SimpleFileStorage], [MemFile] and [AtomicFile].
//!
//! Transactions that modify the database can be logged, which allows for database replication.

//!# Interface
//!
//! The method [Database::run] is called to execute an SQL query.
//! This takes a [Transaction] parameter which accumulates SELECT results and which also has methods
//! for accessing input parameters and controlling output. Custom builtin functions implement CExp
//! and have access to the transaction via an EvalEnv parameter, which can be downcast if necessary.
//!
//! It is also possible to access the table data directly, see email_loop in example program.   
//!
//!# Example
//! [See here](https://github.com/georgebarwood/rustweb2) for an example program -
//! a webserver, with timed jobs, password hashing, data compression, email transmission and database replication.
//! Also has a Manual for the SQL-like language, user interface for database browsing/editing etc.
//!
//!# Features
//!
//! This crate supports the following cargo features:
//! - `gentrans` : enables gentrans module ( sample implementation of [Transaction] ).
//! - `builtin` : Allows extra SQL builtin functions to be defined.
//! - `table` : Allow direct access to database tables.
//! - `max` : maximal interface, including internal modules (which may not be stable).
//! - `verify` : Allows database structure to be verified using builtin function VERIFYDB.
//! - `pack` : Allows database pages to be packed using builtin function REPACKFILE.
//! - `renumber` : Allows database pages to be renumbered using builtin function RENUMBER, eliminating free pages.
//!
//! By default, all features are enabled.
//!
//!# General Design of Database
//!
//! SortedFile stores fixed size Records in a tree of Pages.
//! SortedFile is used to implement:
//!
//! - Database Table storage. Each fixed size record has a 64-bit Id.
//!
//! - Variable length values which are split into fragments, although up to 249 bytes can be stored in the fixed size record.
//!
//! - Index storage - an index record refers back to the main table using the 64-bit Id.
//!
//! When a page becomes too big, it is split into two pages.
//!
//! Each page is implemented as a binary tree ( so there is a tree of trees ).
//!
//! [SharedPagedData] allows logical database pages to be shared to allow concurrent readers.
//!
//! [AtomicFile] ensures that database updates are all or nothing.
//!
//! The hierarchy overall: Table -> SortedFile -> PagedData -> CompactFile -> AtomicFile -> Storage.
//!
//!# Test example
//!
//! ```
//!     use rustdb::*;
//!     use std::sync::Arc;
//!     let stg = AtomicFile::new(MemFile::new(), MemFile::new());
//!
//!     let spd = SharedPagedData::new(stg);
//!     let wapd = AccessPagedData::new_writer(spd);
//!
//!     let mut bmap = BuiltinMap::default();
//!     standard_builtins(&mut bmap);
//!     let bmap = Arc::new(bmap);
//!
//!     let db = Database::new(wapd, "", bmap);
//!     let mut tr = GenTransaction::default();
//!     let sql = "
//! CREATE SCHEMA test GO
//! CREATE TABLE test.Cust(Name string) GO
//! INSERT INTO test.Cust(Name) VALUES ('freddy')
//! SELECT Name FROM test.Cust
//! ";
//!     db.run(&sql, &mut tr);
//!     assert!( db.changed() );
//!     assert!( db.save() > 0 );
//!     assert!( tr.rp.output == b"freddy" );
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use crate::{
    atomfile::AtomicFile,
    builtin::standard_builtins,
    pstore::{AccessPagedData, SharedPagedData},
    stg::{MemFile, SimpleFileStorage, Storage},
};

#[cfg(feature = "gentrans")]
pub use crate::gentrans::{GenTransaction, Part};

#[cfg(feature = "builtin")]
pub use crate::{
    builtin::check_types,
    compile::{c_bool, c_float, c_int, c_value},
    exec::EvalEnv,
    expr::ObjRef,
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
    table::{ColInfo, IndexInfo, Row, SaveOp, Table},
    util::{nd, newmap, SmallSet},
    value::*,
};

use std::{
    any::Any,
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    panic,
    rc::Rc,
    sync::{Arc, Mutex, RwLock},
};

// use std::collections::{HashMap,HashSet};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

/// Utility functions and macros, [SmallSet].
#[cfg(feature = "max")]
#[macro_use]
pub mod util;
#[cfg(not(feature = "max"))]
#[macro_use]
mod util;

#[cfg(feature = "gentrans")]
/// [GenTransaction] ( implementation of [Transaction] ).
pub mod gentrans;

/// Backing [Storage] for database. See also [AtomicFile].
pub mod stg;

/// Test module.
pub mod test;

/// Benchmark - compare RustDb with competitors!
pub mod bench;

/// Page storage and sharing, [SharedPagedData] and [AccessPagedData].
/// Note: several structs and their fields are pub to allow diagnostics but are subject to change.
pub mod pstore;

/// [AtomicFile].
pub mod atomfile;

// Conditional modules.

// #[cfg(target_os = "windows")]
// Optimised implementatation of [Storage] (windows only).
// This didn't work out - actually ran slower!
// pub mod stgwin;

#[cfg(feature = "builtin")]
/// Compilation of builtin functions, [standard_builtins].
pub mod builtin;
#[cfg(not(feature = "builtin"))]
mod builtin;

#[cfg(feature = "builtin")]
/// Functions to compile parsed expressions, checking types.
pub mod compile;
#[cfg(not(feature = "builtin"))]
mod compile;

#[cfg(feature = "builtin")]
/// Expression types, result of parsing. [Expr], [DataKind], [ObjRef], [Block].
pub mod expr;
#[cfg(not(feature = "builtin"))]
mod expr;

#[cfg(feature = "table")]
/// [SortedFile] : [Record] storage.
pub mod sortedfile;
#[cfg(not(feature = "table"))]
mod sortedfile;

#[cfg(feature = "table")]
/// [Table], [ColInfo], [Row] and other Table types for direct table access.
pub mod table;
#[cfg(not(feature = "table"))]
mod table;

#[cfg(feature = "table")]
/// [Page] of records for [SortedFile].
pub mod page;
#[cfg(not(feature = "table"))]
mod page;

#[cfg(feature = "builtin")]
/// Run-time [Value].
pub mod value;
#[cfg(not(feature = "builtin"))]
mod value;

#[cfg(feature = "max")]
/// [EvalEnv] : [Instruction] execution.
pub mod exec;
#[cfg(not(feature = "max"))]
mod exec;

#[cfg(feature = "max")]
/// [CompactFile] : storage of logical pages in smaller regions of backing storage.
pub mod compact;
#[cfg(not(feature = "max"))]
mod compact;

#[cfg(feature = "max")]
/// System table functions.
pub mod sys;
#[cfg(not(feature = "max"))]
mod sys;

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
/// Structs that implement [CExp] trait.
pub mod cexp;
#[cfg(not(feature = "max"))]
mod cexp;

#[cfg(feature = "max")]
/// Storage of variable length values : [ByteStorage].
pub mod bytes;
#[cfg(not(feature = "max"))]
mod bytes;

// End of modules.

/// ```Arc<Vec<u8>>```
pub type Data = Arc<Vec<u8>>;

/// Mutable Data, copied on write.
pub struct MData(Data);

impl MData {
    ///
    pub fn new(data: Data) -> MData {
        MData(data)
    }
    ///
    pub fn to_data(&mut self) -> Data {
        self.0.clone()
    }
}

impl std::ops::Deref for MData {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

/// ```Rc<Database>```
pub type DB = Rc<Database>;

/// Map that defines SQL pre-defined functions.
pub type BuiltinMap = HashMap<String, (DataKind, CompileFunc)>;

/// Database with SQL-like interface.
pub struct Database {
    /// Page storage.
    pub apd: AccessPagedData,

    /// Defined builtin functions.
    pub builtins: Arc<BuiltinMap>,

    // System tables.
    ///
    pub sys_schema: Rc<Table>,
    ///
    pub sys_table: Rc<Table>,
    ///
    pub sys_column: Rc<Table>,
    ///
    pub sys_index: Rc<Table>,
    ///
    pub sys_index_col: Rc<Table>,
    ///
    pub sys_function: Rc<Table>,

    /// Cache of loaded Schemas.
    pub schemas: RefCell<HashMap<String, i64>>,
    /// Cache of loaded Tables.
    pub tables: RefCell<HashMap<ObjRef, Rc<Table>>>,
    /// Cache of loaded Functions.
    pub functions: RefCell<HashMap<ObjRef, Rc<Function>>>,

    /// Last id generated by INSERT.
    pub lastid: Cell<i64>,
    /// Has there been an error since last save?
    pub err: Cell<bool>,
    /// Is the database new?
    pub is_new: bool,

    /// Storage of variable length data.
    bs: Vec<ByteStorage>,
    /// Flag to reset the functions cache after save.
    function_reset: Cell<bool>,
    /// Maximum size of logical page.
    page_size_max: usize,
}

const SYS_ROOT_LAST: u64 = 16;

impl Database {
    /// Construct a new DB, based on the specified file.
    /// initsql is used to initialise a new database.
    /// builtins specifies the functions callable in SQL code such as SUBSTR, REPLACE etc.
    pub fn new(apd: AccessPagedData, initsql: &str, builtins: Arc<BuiltinMap>) -> DB {
        let is_new = apd.is_new();
        let mut tb = TableBuilder::new();
        let sys_schema = tb.nt("Schema", &[("Name", STRING)]);
        let sys_table = tb.nt(
            "Table",
            &[
                ("Root", INT),
                ("Schema", INT),
                ("Name", STRING),
                ("IdGen", INT),
            ],
        );
        let sys_column = tb.nt("Column", &[("Table", INT), ("Name", STRING), ("Type", INT)]);
        let sys_index = tb.nt("Index", &[("Root", INT), ("Table", INT), ("Name", STRING)]);
        let sys_index_col = tb.nt("IndexColumn", &[("Index", INT), ("ColId", INT)]);
        let sys_function = tb.nt(
            "Function",
            &[("Schema", INT), ("Name", NAMESTR), ("Def", BIGSTR)],
        );
        sys_schema.add_index(tb.rt(), vec![0], 1);
        sys_table.add_index(tb.rt(), vec![1, 2], 2);
        sys_column.add_index(tb.rt(), vec![0], 3);
        sys_index.add_index(tb.rt(), vec![1], 4);
        sys_index_col.add_index(tb.rt(), vec![0], 5);
        sys_function.add_index(tb.rt(), vec![0, 1], 6);
        sys_function.add_index(tb.rt(), vec![1], 7);

        let mut bs = Vec::new();
        for ft in 0..bytes::NFT {
            bs.push(ByteStorage::new(ft as u64, ft));
        }

        let page_size_max = apd.spd.page_size_max();

        let db = Rc::new(Database {
            apd,
            sys_schema,
            sys_table,
            sys_column,
            sys_index,
            sys_index_col,
            sys_function,
            bs,
            schemas: newmap(),
            functions: newmap(),
            tables: newmap(),
            builtins,
            function_reset: Cell::new(false),
            lastid: Cell::new(0),
            err: Cell::new(false),
            is_new,
            page_size_max,
        });

        assert!(tb.alloc as u64 - 1 == SYS_ROOT_LAST);

        if is_new {
            for _ft in 0..bytes::NFT {
                db.alloc_page(); // Allocate page for byte storage.
            }
        }
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
CREATE TABLE sys.Table( Root int, Schema int, Name string, IdGen int )
CREATE TABLE sys.Column( Table int, Name string, Type int )
CREATE TABLE sys.Index( Root int, Table int, Name string )
CREATE TABLE sys.IndexColumn( Index int, ColId int )
CREATE TABLE sys.Function( Schema int, Name string(31), Def string(249) )
GO
CREATE INDEX ByName ON sys.Schema(Name)
CREATE INDEX BySchemaName ON sys.Table(Schema,Name)
CREATE INDEX ByTable ON sys.Column(Table)
CREATE INDEX ByTable ON sys.Index(Table)
CREATE INDEX ByIndex ON sys.IndexColumn(Index)
CREATE INDEX BySchemaName ON sys.Function(Schema,Name)
CREATE INDEX ByName ON sys.Function(Name)
GO
";
            let mut dq = DummyTransaction {};
            db.run(sysinit, &mut dq);
            db.run(initsql, &mut dq);
            db.save();
        }

        db
    }

    /// Run a batch of SQL.
    pub fn run(self: &DB, source: &str, tr: &mut dyn Transaction) {
        if let Some(e) = self.go(source, tr) {
            let err = format!(
                "{} in {} at line {} column {}.",
                e.msg, e.rname, e.line, e.column
            );
            tr.set_error(err);
            self.err.set(true);
        }
    }

    /// Run a batch of SQL.
    fn go(self: &DB, source: &str, tr: &mut dyn Transaction) -> Option<SqlError> {
        let mut p = Parser::new(source, self);
        let result = std::panic::catch_unwind(panic::AssertUnwindSafe(|| {
            p.batch(tr);
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

    /// Test whether there are unsaved changes.
    pub fn changed(self: &DB) -> bool {
        if self.err.get() {
            return false;
        }
        for bs in &self.bs {
            if bs.changed() {
                return true;
            }
        }
        for t in self.tables.borrow().values() {
            if t.id_gen_dirty.get() {
                return true;
            }
            if t.file.changed() {
                return true;
            }
        }
        false
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
        for bs in &self.bs {
            bs.save(self, op);
        }
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
        self.apd.save(op)
    }

    #[cfg(not(feature = "table"))]
    /// Get the named table.
    fn get_table(self: &DB, name: &ObjRef) -> Option<Rc<Table>> {
        if let Some(t) = self.tables.borrow().get(name) {
            return Some(t.clone());
        }
        sys::get_table(self, name)
    }

    #[cfg(feature = "table")]
    /// Get the named table.
    pub fn get_table(self: &DB, name: &ObjRef) -> Option<Rc<Table>> {
        if let Some(t) = self.tables.borrow().get(name) {
            return Some(t.clone());
        }
        sys::get_table(self, name)
    }

    #[cfg(feature = "table")]
    /// Get the named table ( panics if it does not exist ).
    pub fn table(self: &DB, schema: &str, name: &str) -> Rc<Table> {
        self.get_table(&ObjRef::new(schema, name)).unwrap()
    }

    /// Get the named function.
    fn get_function(self: &DB, name: &ObjRef) -> Option<Rc<Function>> {
        if let Some(f) = self.functions.borrow().get(name) {
            return Some(f.clone());
        }
        sys::get_function(self, name)
    }

    /// Insert the table into the map of tables.
    fn publish_table(&self, table: Rc<Table>) {
        let name = table.info.name.clone();
        self.tables.borrow_mut().insert(name, table);
    }

    /// Get code for value.
    fn encode(self: &DB, val: &Value, size: usize) -> Code {
        let bytes = match val {
            Value::RcBinary(x) => &**x,
            Value::ArcBinary(x) => &**x,
            Value::String(x) => x.as_bytes(),
            _ => {
                return Code {
                    id: u64::MAX,
                    ft: 0,
                };
            }
        };
        if bytes.len() < size {
            return Code {
                id: u64::MAX,
                ft: 0,
            };
        }
        let tbe = &bytes[size - 9..];
        let ft = bytes::fragment_type(tbe.len());
        let id = self.bs[ft].encode(self, &bytes[size - 9..]);
        Code { id, ft }
    }

    /// Decode u64 to bytes.
    fn decode(self: &DB, code: Code, inline: usize) -> Vec<u8> {
        self.bs[code.ft].decode(self, code.id, inline)
    }

    /// Delete encoding.
    fn delcode(self: &DB, code: Code) {
        if code.id != u64::MAX {
            self.bs[code.ft].delcode(self, code.id);
        }
    }

    /// Allocate a page of underlying file storage.
    fn alloc_page(self: &DB) -> u64 {
        self.apd.alloc_page()
    }

    /// Free a page of underlying file storage.
    fn free_page(self: &DB, lpnum: u64) {
        self.apd.free_page(lpnum);
    }

    #[cfg(feature = "pack")]
    /// Get size of logical page.
    fn lp_size(&self, pnum: u64) -> u64 {
        self.apd.spd.file.read().unwrap().lp_size(pnum) as u64
    }

    #[cfg(feature = "pack")]
    /// Repack the specified sortedfile.
    fn repack_file(self: &DB, k: i64, schema: &str, tname: &str) -> i64 {
        if k >= 0 {
            let name = ObjRef::new(schema, tname);
            if let Some(t) = self.get_table(&name) {
                return t.repack(self, k as usize);
            }
        } else {
            let k = (-k - 1) as usize;
            if k < 4 {
                return self.bs[k].repack_file(self);
            }
        }
        -1
    }

    #[cfg(feature = "verify")]
    /// Verify the page structure of the database.
    pub fn verify(self: &DB) -> String {
        let (mut pages, total) = self.apd.spd.file.read().unwrap().get_info();
        let total = total as usize;

        let free = pages.len();

        for bs in &self.bs {
            bs.file.get_used(self, &mut pages);
        }

        for t in self.tables.borrow().values() {
            t.get_used(self, &mut pages);
        }

        // assert_eq!(pages.len(), total);

        format!(
            "Logical page summary: free={} used={} total={} pages={}",
            free,
            total - free,
            total,
            pages.len()
        )
    }

    /// Renumber pages.
    #[cfg(feature = "renumber")]
    pub fn renumber(self: &DB) {
        let target = self.apd.spd.file.write().unwrap().load_free_pages();

        for bs in &self.bs {
            bs.file.renumber(self, target);
        }

        for t in self.tables.borrow().values() {
            let tf = &t.file;
            let mut root_page = tf.root_page.get();
            if root_page >= target {
                root_page = self.apd.renumber_page(root_page);
                tf.root_page.set(root_page);
                sys::set_root(self, t.id, root_page);
            }
            tf.renumber(self, target);
            for ix in &mut *t.ixlist.borrow_mut() {
                let mut root_page = ix.file.root_page.get();
                if root_page >= target {
                    root_page = self.apd.renumber_page(root_page);
                    ix.file.root_page.set(root_page);
                    sys::set_ix_root(self, ix.id, root_page);
                }
                ix.file.renumber(self, target);
            }
        }
        self.apd.spd.file.write().unwrap().set_lpalloc(target);
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
    alloc: usize,
    list: Vec<Rc<Table>>,
}
impl TableBuilder {
    fn new() -> Self {
        Self {
            alloc: bytes::NFT,
            list: Vec::new(),
        }
    }

    fn nt(&mut self, name: &str, ct: &[(&str, DataType)]) -> Rc<Table> {
        let root = self.rt();
        let id = 1 + (root - bytes::NFT as u64);
        let name = ObjRef::new("sys", name);
        let info = ColInfo::new(name, ct);
        let table = Table::new(id as i64, root, 1, Rc::new(info));
        self.list.push(table.clone());
        table
    }

    fn rt(&mut self) -> u64 {
        let result = self.alloc;
        self.alloc += 1;
        result as u64
    }
}

/// Input/Output message. Query and Response.
pub trait Transaction: Any {
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
        nd()
    }

    /// Set the error string.
    fn set_error(&mut self, err: String);

    /// Get the error string.
    fn get_error(&mut self) -> String {
        String::new()
    }

    /// Set the extension.
    fn set_extension(&mut self, _ext: Box<dyn Any + Send + Sync>) {}

    /// Get the extension. Note: this takes ownership, so extension needs to be set afterwards.
    fn get_extension(&mut self) -> Box<dyn Any + Send + Sync> {
        Box::new(())
    }
}

/// [Transaction] where output is discarded (used for initialisation ).
struct DummyTransaction {}
impl Transaction for DummyTransaction {
    fn selected(&mut self, _values: &[Value]) {}
    /// Called if a panic ( error ) occurs.
    fn set_error(&mut self, err: String) {
        println!("Error: {}", err);
    }
}
