use crate::*;

/// Table Pointer.
pub type TablePtr = Rc<Table>;

/// Database base table. Underlying file, type information about the columns and id allocation.
pub struct Table
{
  /// Underlying SortedFile.
  pub file: Rc<SortedFile>,

  /// Type information about the columns.
  pub(crate) info: Rc<ColInfo>,

  /// List of indexes.
  ixlist: RefCell<IxList>,

  /// Table id in sys.Table.
  pub(crate) id: i64,

  /// Row id allocator.
  pub(crate) id_gen: Cell<i64>,

  /// Row id allocator has changed.
  pub(crate) id_gen_dirty: Cell<bool>,
}

/// List of indexes. Each index has a file and a list of column numbers.
pub type IxList = Vec<(Rc<SortedFile>, Rc<Vec<usize>>)>;

impl Table
{
  /// Optimise WHERE clause with form "Name = <someconst>".
  pub fn index_from(self: &TablePtr, p: &Parser, we: &mut Expr) -> Option<CTableExpression>
  {
    if let ExprIs::Binary(op, e1, e2) = &mut we.exp
    {
      if *op == Token::Equal && e2.is_constant
      {
        if let ExprIs::ColName(name) = &e1.exp
        {
          if name == "Id"
          {
            return Some(CTableExpression::IdGet(self.clone(), c_int(p, e2)));
          }
          let list = &self.ixlist.borrow();
          for (index, (_f, c)) in list.iter().enumerate()
          {
            if c[0] == e1.col
            {
              return Some(CTableExpression::IxGet(self.clone(), c_value(p, e2), index));
            }
          }
        }
      }
    }
    None
  }

  /// Get record with specified id.
  pub fn id_get(&self, db: &DB, id: u64) -> Option<(PagePtr, usize)>
  {
    self.file.get(db, &Id { id })
  }

  /// Get record with matching key, using specified index.
  pub fn ix_get(&self, db: &DB, key: Vec<Value>, index: usize) -> Option<(PagePtr, usize)>
  {
    let list = &self.ixlist.borrow();
    let (f, c) = &list[index];
    let key = IndexKey::new(self, c.clone(), key, Ordering::Equal);
    if let Some((p, off)) = f.get(db, &key)
    {
      let p = &p.borrow();
      let id = util::getu64(&p.data, off);
      let row = Id { id };
      return self.file.get(db, &row);
    }
    None
  }

  /// Scan all the records in the table.
  pub fn scan(&self, db: &DB) -> Asc
  {
    self.file.asc(db, Box::new(Zero {}))
  }

  /// Get a single record with specified id.
  pub fn scan_id(self: &TablePtr, db: &DB, id: i64) -> IdScan
  {
    IdScan { table: self.clone(), db: db.clone(), id, done: false }
  }

  /// Get records with matching key.
  pub fn scan_key(self: &TablePtr, db: &DB, key: Value, index: usize) -> IndexScan
  {
    let keys = vec![key];
    self.scan_keys(db, keys, index)
  }

  /// Get records with matching keys.
  pub fn scan_keys(self: &TablePtr, db: &DB, keys: Vec<Value>, index: usize) -> IndexScan
  {
    let ixlist = &self.ixlist.borrow();
    let (f, c) = &ixlist[index];
    let ikey = IndexKey::new(self, c.clone(), keys.clone(), Ordering::Less);
    let ixa = f.asc(db, Box::new(ikey));
    IndexScan { ixa, keys, cols: c.clone(), table: self.clone(), db: db.clone() }
  }

  /// Insert specified row into the table.
  pub fn insert(&self, db: &DB, row: &mut Row)
  {
    row.encode(db); // Calculate codes for Binary and String values.
    self.file.insert(db, row);
    // Update any indexes.
    for (f, cols) in &*self.ixlist.borrow()
    {
      let ixr = IndexRow::new(self, cols.clone(), row);
      f.insert(db, &ixr);
    }
  }

  /// Remove specified loaded row from the table.
  pub fn remove(&self, db: &DB, row: &Row)
  {
    self.file.remove(db, row);
    for (f, cols) in &*self.ixlist.borrow()
    {
      let ixr = IndexRow::new(self, cols.clone(), row);
      f.remove(db, &ixr);
    }
    row.delcodes(db); // Deletes codes for Binary and String values.
  }

  /// Add the specified index to the table.
  pub fn add_index(&self, root: u64, cols: Vec<usize>)
  {
    let key_size = self.info.calc_index_key_size(&cols) + 8;
    let file = Rc::new(SortedFile::new(key_size, key_size, root));

    let list = &mut self.ixlist.borrow_mut();
    list.push((file, Rc::new(cols)));
  }

  /// Utility for accessing fields by number.
  pub fn access<'d, 't>(&'t self, p: &'d Page, off: usize) -> Access<'d, 't>
  {
    Access::<'d, 't> { data: &p.data[off..], info: &self.info }
  }

  /// Utility for updating fields by number.
  pub fn write_access<'d, 't>(&'t self, p: &'d mut Page, off: usize) -> WriteAccess<'d, 't>
  {
    WriteAccess::<'d, 't> { data: &mut p.data[off..], info: &self.info }
  }

  /// Construct a row for the table.
  pub fn row(&self) -> Row
  {
    Row::new(self.info.clone())
  }

  /// Allocate  row id.
  pub fn alloc_id(&self) -> i64
  {
    let result = self.id_gen.get();
    self.id_gen.set(result + 1);
    self.id_gen_dirty.set(true);
    result
  }

  /// Update id allocator if supplied row id exceeds current value.
  pub fn id_allocated(&self, id: i64)
  {
    if id >= self.id_gen.get()
    {
      self.id_gen.set(id + 1);
      self.id_gen_dirty.set(true);
    }
  }

  /// Save files.
  pub(crate) fn save(&self, db: &DB)
  {
    self.file.save(db);
    for (f, _) in &*self.ixlist.borrow()
    {
      f.save(db);
    }
  }

  /// Construct a new table with specified info.
  pub(crate) fn new(id: i64, root_page: u64, id_gen: i64, info: Rc<ColInfo>) -> TablePtr
  {
    let rec_size = info.total;
    let key_size = 8;
    let file = Rc::new(SortedFile::new(rec_size, key_size, root_page));
    let ixlist = RefCell::new(Vec::new());
    Rc::new(Table { id, file, info, ixlist, id_gen: Cell::new(id_gen), id_gen_dirty: Cell::new(false) })
  }

  pub fn _dump(&self, _db: &DB)
  {
    // println!( "table_dump info={:?}", self.info );
    self.file.dump();
    /*
        let mut r = self.row();
        for (p, off) in self.file.asc(db, Box::new(Zero {}))
        {
          let p = &p.borrow();
          r.load(db, &p.data[off..]);
          println!("row id={} value={:?}", r.id, r.values);
        }
    */
  }
}

/// Dummy record for iterating over whole table.
struct Zero {}

impl Record for Zero
{
  fn compare(&self, _db: &DB, _data: &[u8]) -> std::cmp::Ordering
  {
    std::cmp::Ordering::Less
  }
}

/// Helper class to read byte data using ColInfo.
pub struct Access<'d, 'i>
{
  data: &'d [u8],
  info: &'i ColInfo,
}

impl<'d, 'i> Access<'d, 'i>
{
  /// Extract int from byte data for column number colnum.
  pub fn int(&self, colnum: usize) -> i64
  {
    util::get(self.data, self.info.off[colnum], self.info.siz[colnum]) as i64
  }

  /// Extract string from byte data for column number colnum.
  pub fn str(&self, db: &DB, colnum: usize) -> String
  {
    let off = self.info.off[colnum];
    let bytes = get_bytes(db, &self.data[off..]).0;
    String::from_utf8(bytes).unwrap()
  }

  /// Extract Id from byte data.
  pub fn id(&self) -> i64
  {
    util::getu64(self.data, 0) as i64
  }
}

/// Helper class to write byte data using ColInfo.
pub struct WriteAccess<'d, 'i>
{
  data: &'d mut [u8],
  info: &'i ColInfo,
}

impl<'d, 'i> WriteAccess<'d, 'i>
{
  /// Save int to byte data.
  pub fn set_int(&mut self, colnum: usize, val: i64)
  {
    util::set(self.data, self.info.off[colnum], val as u64, self.info.siz[colnum]);
  }
}

/// Table name and column names/types and other calculated values for a table.
pub struct ColInfo
{
  /// Table name.
  pub name: ObjRef,
  /// Map from column name to column number.
  pub colmap: HashMap<String, usize>,
  /// Column names.
  pub colnames: Vec<String>,
  /// Column types.
  pub typ: Vec<DataType>,
  /// Column sizes.
  pub siz: Vec<usize>,
  /// Column offsets.
  pub off: Vec<usize>,
  /// Total data size, including Id.
  pub total: usize,
}

impl ColInfo
{
  /// Construct a new ColInfo struct with no columns.
  pub fn empty(name: ObjRef) -> Self
  {
    ColInfo {
      name,
      colmap: HashMap::new(),
      typ: Vec::new(),
      colnames: Vec::new(),
      siz: Vec::new(),
      off: Vec::new(),
      total: 8,
    }
  }

  pub(crate) fn new(name: ObjRef, ct: &[(&str, DataType)]) -> Self
  {
    let mut result = Self::empty(name);
    for (n, t) in ct
    {
      result.add(n.to_string(), *t);
    }
    result
  }

  /// Add a column. If the column already exists ( an error ) the result is true.
  pub fn add(&mut self, name: String, typ: DataType) -> bool
  {
    if self.colmap.contains_key(&name)
    {
      return true;
    }

    let cn = self.typ.len();
    self.typ.push(typ);
    let size = data_size(typ);
    self.siz.push(size);
    self.off.push(self.total);
    self.total += size;
    self.colnames.push(name.clone());
    self.colmap.insert(name, cn);
    false
  }

  /// Get a column number from a column name.
  /// usize::MAX is returned for "Id".
  pub fn get(&self, name: &str) -> Option<&usize>
  {
    if name == "Id"
    {
      Some(&usize::MAX)
    }
    else
    {
      self.colmap.get(name)
    }
  }

  fn calc_index_key_size(&self, cols: &[usize]) -> usize
  {
    let mut total = 0;
    for cnum in cols
    {
      total += data_size(self.typ[*cnum]);
    }
    total
  }
}

/// Index information.
pub struct IndexInfo
{
  pub tname: ObjRef,
  pub iname: String,
  pub cols: Vec<usize>,
}

/// Row of Values, with type information.
#[derive(Clone)]
pub struct Row
{
  pub id: i64,
  pub values: Vec<Value>,
  pub info: Rc<ColInfo>,
  pub codes: Vec<u64>,
}

impl Row
{
  pub fn new(info: Rc<ColInfo>) -> Self
  {
    let mut result = Row { id: 0, values: Vec::new(), info, codes: Vec::new() };
    for t in &result.info.typ
    {
      result.values.push(default(*t));
    }
    result
  }

  pub fn encode(&mut self, db: &DB)
  {
    self.codes.clear();
    for val in &self.values
    {
      let u = db.encode(val);
      self.codes.push(u);
    }
  }

  pub fn delcodes(&self, db: &DB)
  {
    for u in &self.codes
    {
      if *u != u64::MAX
      {
        db.delcode(*u);
      }
    }
  }

  /// Load the row values and codes from data.
  pub fn load(&mut self, db: &DB, data: &[u8])
  {
    self.values.clear();
    self.codes.clear();
    self.id = util::getu64(data, 0) as i64;
    let mut off = 8;
    for typ in &self.info.typ
    {
      let (val, code) = Value::load(db, *typ, data, off);
      self.values.push(val);
      self.codes.push(code);
      off += data_size(*typ);
    }
  }
}

impl Record for Row
{
  fn save(&self, data: &mut [u8])
  {
    util::setu64(data, self.id as u64);
    let t = &self.info;
    let mut off = 8;
    for (i, typ) in t.typ.iter().enumerate()
    {
      self.values[i].save(t.typ[i], data, off, self.codes[i]);
      off += data_size(*typ);
    }
  }

  fn compare(&self, _db: &DB, data: &[u8]) -> std::cmp::Ordering
  {
    let id = util::getu64(data, 0) as i64;
    self.id.cmp(&id)
  }
}

/// Row for inserting into an index.
struct IndexRow
{
  pub tinfo: Rc<ColInfo>,
  pub cols: Rc<Vec<usize>>,
  pub keys: Vec<Value>,
  pub codes: Vec<u64>,
  pub rowid: i64,
}

impl IndexRow
{
  // Construct IndexRow from Row.
  fn new(table: &Table, cols: Rc<Vec<usize>>, row: &Row) -> Self
  {
    let mut keys = Vec::new();
    let mut codes = Vec::new();
    for c in &*cols
    {
      keys.push(row.values[*c].clone());
      codes.push(row.codes[*c]);
    }
    Self { tinfo: table.info.clone(), cols, rowid: row.id, keys, codes }
  }

  // Load IndexRow from data ( note: new codes are computed, as old codes may be deleted ).
  // Since it's unusual for long strings to be keys, code computation should be rare.
  fn load(&mut self, db: &DB, data: &[u8])
  {
    self.rowid = util::getu64(data, 0) as i64;
    let mut off = 8;
    for col in &*self.cols
    {
      let typ = self.tinfo.typ[*col];
      let val = Value::load(db, typ, data, off).0;
      let code = db.encode(&val);
      self.keys.push(val);
      self.codes.push(code);
      off += data_size(typ);
    }
  }
}

impl Record for IndexRow
{
  fn save(&self, data: &mut [u8])
  {
    util::setu64(data, self.rowid as u64);
    let mut off = 8;
    for (ix, k) in self.keys.iter().enumerate()
    {
      let typ = self.tinfo.typ[self.cols[ix]];
      k.save(typ, data, off, self.codes[ix]);
      off += data_size(typ);
    }
  }

  fn compare(&self, db: &DB, data: &[u8]) -> Ordering
  {
    let mut ix = 0;
    let mut off = 8;
    loop
    {
      let typ = self.tinfo.typ[self.cols[ix]];

      // Could have special purpose Value method which compares instead of loading to save heap allocations.
      let val = Value::load(db, typ, data, off).0;
      let cf = val.cmp(&self.keys[ix]);
      if cf != Ordering::Equal
      {
        return cf;
      }
      ix += 1;
      off += data_size(typ);
      if ix == self.cols.len()
      {
        let rowid = util::getu64(data, 0) as i64;
        return self.rowid.cmp(&rowid);
      }
    }
  }

  fn key(&self, db: &DB, data: &[u8]) -> Box<dyn Record>
  {
    let mut result = Box::new(IndexRow {
      cols: self.cols.clone(),
      tinfo: self.tinfo.clone(),
      rowid: 0,
      keys: Vec::new(),
      codes: Vec::new(),
    });
    result.load(db, data);
    result
  }

  fn dropkey(&self, db: &DB, data: &[u8])
  {
    let mut off = 8;
    for col in &*self.cols
    {
      let typ = self.tinfo.typ[*col];
      let code = Value::load(db, typ, data, off).1;
      if code != u64::MAX
      {
        db.delcode(code);
      }
      off += data_size(typ);
    }
  }
}

/// Key for searching index.
pub struct IndexKey
{
  pub tinfo: Rc<ColInfo>,
  pub cols: Rc<Vec<usize>>,
  pub key: Vec<Value>,
  pub def: Ordering,
}

impl IndexKey
{
  fn new(table: &Table, cols: Rc<Vec<usize>>, key: Vec<Value>, def: Ordering) -> Self
  {
    Self { tinfo: table.info.clone(), key, cols, def }
  }
}

impl Record for IndexKey
{
  fn compare(&self, db: &DB, data: &[u8]) -> Ordering
  {
    let mut ix = 0;
    let mut off = 8;
    loop
    {
      let typ = self.tinfo.typ[self.cols[ix]];
      let val = Value::load(db, typ, data, off).0;

      let cf = val.cmp(&self.key[ix]);
      if cf != Ordering::Equal
      {
        return cf;
      }
      ix += 1;
      if ix == self.key.len()
      {
        return self.def;
      }
      off += data_size(typ);
    }
  }
}

/// Fetch records using an index.
pub struct IndexScan
{
  ixa: Asc,
  table: TablePtr,
  db: DB,
  cols: Rc<Vec<usize>>,
  keys: Vec<Value>,
}

impl IndexScan
{
  fn keys_equal(&self, data: &[u8]) -> bool
  {
    let mut off = 8;
    for (ix, k) in self.keys.iter().enumerate()
    {
      let typ = self.table.info.typ[self.cols[ix]];
      let val = Value::load(&self.db, typ, data, off).0;
      let cf = val.cmp(k);
      if cf != Ordering::Equal
      {
        return false;
      }
      off += data_size(typ);
    }
    true
  }
}

impl Iterator for IndexScan
{
  type Item = (PagePtr, usize);
  fn next(&mut self) -> Option<<Self as Iterator>::Item>
  {
    if let Some((p, off)) = self.ixa.next()
    {
      let p = &p.borrow();
      let data = &p.data[off..];
      if !self.keys_equal(data)
      {
        return None;
      }
      let id = util::getu64(data, 0);
      return self.table.id_get(&self.db, id);
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
  type Item = (PagePtr, usize);
  fn next(&mut self) -> Option<<Self as Iterator>::Item>
  {
    if self.done
    {
      return None;
    }
    self.done = true;
    self.table.id_get(&self.db, self.id as u64)
  }
}
