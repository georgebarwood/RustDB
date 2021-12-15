use crate::*;

/// Table Pointer. ```Rc<Table>```
pub type TablePtr = Rc<Table>;

/// List of indexes. Each index has a file and a list of column numbers.
pub type IxList = Vec<(Rc<SortedFile>, Rc<Vec<usize>>)>;

/// Save or Rollback.
#[derive(PartialEq, PartialOrd, Clone, Copy)]
pub enum SaveOp {
    Save,
    RollBack,
}

/// Database base table. Underlying file, type information about the columns and id allocation.
pub struct Table {
    /// Underlying SortedFile.
    pub file: Rc<SortedFile>,

    /// Type information about the columns.
    pub info: Rc<ColInfo>,

    /// List of indexes. ( Maybe could eliminate the RefCell )
    pub ixlist: RefCell<IxList>,

    /// Table id in sys.Table.
    pub id: i64,

    /// Row id allocator.
    pub id_gen: Cell<i64>,

    /// Row id allocator has changed.
    pub id_gen_dirty: Cell<bool>,
}

impl Table {
    /// Construct a table with specified info.
    pub fn new(id: i64, root_page: u64, id_gen: i64, info: Rc<ColInfo>) -> TablePtr {
        let rec_size = info.total;
        let key_size = 8;
        let file = Rc::new(SortedFile::new(rec_size, key_size, root_page));
        let ixlist = RefCell::new(Vec::new());
        Rc::new(Table {
            id,
            file,
            info,
            ixlist,
            id_gen: Cell::new(id_gen),
            id_gen_dirty: Cell::new(false),
        })
    }

    /// Save or Rollback underlying files.
    pub fn save(&self, db: &DB, op: SaveOp) {
        self.file.save(db, op);
        for (f, _) in &*self.ixlist.borrow() {
            f.save(db, op);
        }
    }

    /// Drop the underlying file storage ( the table is not useable after this ).
    pub fn free_pages(&self, db: &DB) {
        let row = self.row();
        self.file.free_pages(db, &row);
        for (f, cols) in &*self.ixlist.borrow() {
            let ixr = IndexRow::new(self, cols.clone(), &row);
            f.free_pages(db, &ixr);
        }
    }

    pub fn get_used(&self, db: &DB, to: &mut HashSet<u64>)
    {
        self.file.get_used(db, to);
        for (f, _cols) in &*self.ixlist.borrow() 
        {
          f.get_used(db, to);
        }
    }

    /// Insert specified row into the table.
    pub fn insert(&self, db: &DB, row: &mut Row) {
        row.encode(db); // Calculate codes for Binary and String values.
        self.file.insert(db, row);
        // Update any indexes.
        for (f, cols) in &*self.ixlist.borrow() {
            let ixr = IndexRow::new(self, cols.clone(), row);
            f.insert(db, &ixr);
        }
    }

    /// Remove specified loaded row from the table.
    pub fn remove(&self, db: &DB, row: &Row) {
        self.file.remove(db, row);
        for (f, cols) in &*self.ixlist.borrow() {
            let ixr = IndexRow::new(self, cols.clone(), row);
            f.remove(db, &ixr);
        }
        row.delcodes(db); // Deletes codes for Binary and String values.
    }

    pub fn repack(&self, db: &DB, k: usize) -> i64 {
        let row = self.row();
        if k == 0 {
            self.file.repack(db, &row)
        } else {
            let list = &*self.ixlist.borrow();
            if k <= list.len() {
                let (f, cols) = &list[k - 1];
                let ixr = IndexRow::new(self, cols.clone(), &row);
                f.repack(db, &ixr)
            } else {
                -1
            }
        }
    }

    /// Look for indexed table expression based on supplied WHERE expression (we).
    pub fn index_from(
        self: &TablePtr,
        b: &Block,
        we: &mut Expr,
    ) -> (Option<CExpPtr<bool>>, Option<CTableExpression>) {
        let mut kc = SmallSet::default(); // Set of known columns.
        get_known_cols(we, &mut kc);

        let list = &*self.ixlist.borrow();

        let mut best_match = 0;
        let mut best_index = 0;
        for (index, (_f, clist)) in list.iter().enumerate() {
            let m = covered(clist, &kc);
            if m > best_match {
                best_match = m;
                best_index = index;
            }
        }
        if best_match > 0 {
            // Get the key values for the chosen index.
            let clist = &list[best_index].1;
            let mut cols = SmallSet::default();
            for col in clist.iter().take(best_match) {
                cols.insert(*col);
            }
            let mut kmap = BTreeMap::new();
            let cwe = get_keys(b, we, &mut cols, &mut kmap);
            let keys = clist
                .iter()
                .take(best_match)
                .map(|col| kmap.remove(col).unwrap())
                .collect();
            return (
                cwe,
                Some(CTableExpression::IxGet(self.clone(), keys, best_index)),
            );
        }

        // ToDo: check for mirror expression, AND conditions, also Id = x OR Id = y ...  Id in (....) etc.
        if let ExprIs::Binary(op, e1, e2) = &mut we.exp {
            if *op == Token::Equal && e2.is_constant {
                if let ExprIs::ColName(_) = &e1.exp {
                    if e1.col == usize::MAX
                    // Id column.
                    {
                        return (
                            None,
                            Some(CTableExpression::IdGet(self.clone(), c_int(b, e2))),
                        );
                    }
                }
            }
        }
        println!("No index found for table {}", self.info.name.str());
        (Some(c_bool(b, we)), None)
    }

    /// Get record with specified id.
    pub fn id_get(&self, db: &DB, id: u64) -> Option<(PagePtr, usize)> {
        self.file.get(db, &Id { id })
    }

    /// Get record with matching key, using specified index.
    pub fn ix_get(&self, db: &DB, key: Vec<Value>, index: usize) -> Option<(PagePtr, usize)> {
        let list = &*self.ixlist.borrow();
        let (sf, cols) = &list[index];
        let key = IndexKey::new(self, cols.clone(), key, Ordering::Equal);
        if let Some((pp, off)) = sf.get(db, &key) {
            let p = pp.borrow();
            let id = util::getu64(&p.data, off);
            let row = Id { id };
            return self.file.get(db, &row);
        }
        None
    }

    /// Scan all the records in the table.
    pub fn scan(&self, db: &DB) -> Asc {
        self.file.asc(db, Box::new(Zero {}))
    }

    /// Get a single record with specified id.
    pub fn scan_id(self: &TablePtr, db: &DB, id: i64) -> IdScan {
        IdScan {
            table: self.clone(),
            db: db.clone(),
            id,
            done: false,
        }
    }

    /// Get records with matching key.
    pub fn scan_key(self: &TablePtr, db: &DB, key: Value, index: usize) -> IndexScan {
        let keys = vec![key];
        self.scan_keys(db, keys, index)
    }

    /// Get records with matching keys.
    pub fn scan_keys(self: &TablePtr, db: &DB, keys: Vec<Value>, index: usize) -> IndexScan {
        let ixlist = &*self.ixlist.borrow();
        let (sf, cols) = &ixlist[index];
        let ikey = IndexKey::new(self, cols.clone(), keys.clone(), Ordering::Less);
        let ixa = sf.asc(db, Box::new(ikey));
        IndexScan {
            ixa,
            keys,
            cols: cols.clone(),
            table: self.clone(),
            db: db.clone(),
        }
    }

    /// Add the specified index to the table.
    pub fn add_index(&self, root: u64, cols: Vec<usize>) {
        let key_size = self.info.index_key_size(&cols) + 8;
        let sf = Rc::new(SortedFile::new(key_size, key_size, root));
        let list = &mut self.ixlist.borrow_mut();
        list.push((sf, Rc::new(cols)));
    }

    // Delete the specified index.
    pub fn delete_index(&self, db: &DB, ix: usize) {
        let ixlist = &*self.ixlist.borrow();
        let (f, cols) = &ixlist[ix];
        let row = self.row();
        let ixr = IndexRow::new(self, cols.clone(), &row);
        f.free_pages(db, &ixr);
    }

    /// Initialises last index ( called just after add_index ).
    pub fn init_index(&self, db: &DB) {
        let mut row = self.row();
        let ixlist = self.ixlist.borrow();
        let (f, cols) = ixlist.last().unwrap();

        for (pp, off) in self.scan(db) {
            let p = pp.borrow();
            let data = &p.data[off..];
            row.load(db, data);
            let ixr = IndexRow::new(self, cols.clone(), &row);
            f.insert(db, &ixr);
        }
    }

    /// Utility for accessing fields by number.
    pub fn access<'d, 't>(&'t self, p: &'d Page, off: usize) -> Access<'d, 't> {
        Access::<'d, 't> {
            data: &p.data[off..],
            info: &self.info,
        }
    }

    /// Utility for updating fields by number.
    pub fn write_access<'d, 't>(&'t self, p: &'d mut Page, off: usize) -> WriteAccess<'d, 't> {
        let data = Data::make_mut(&mut p.data);
        WriteAccess::<'d, 't> {
            data: &mut data[off..],
            info: &self.info,
        }
    }

    /// Construct a row for the table.
    pub fn row(&self) -> Row {
        Row::new(self.info.clone())
    }

    /// Allocate row id.
    pub fn alloc_id(&self) -> i64 {
        let result = self.id_gen.get();
        self.id_gen.set(result + 1);
        self.id_gen_dirty.set(true);
        result
    }

    /// Update id allocator if supplied row id exceeds current value.
    pub fn id_allocated(&self, id: i64) {
        if id >= self.id_gen.get() {
            self.id_gen.set(id + 1);
            self.id_gen_dirty.set(true);
        }
    }
}

/// Dummy record for iterating over whole table.
struct Zero {}

impl Record for Zero {
    /// Always returns `Less`.
    fn compare(&self, _db: &DB, _data: &[u8]) -> Ordering {
        Ordering::Less
    }
}

/// Helper class to read byte data using ColInfo.
pub struct Access<'d, 'i> {
    pub data: &'d [u8],
    pub info: &'i ColInfo,
}

impl<'d, 'i> Access<'d, 'i> {
    /// Extract int from byte data for specified column.
    pub fn int(&self, colnum: usize) -> i64 {
        util::iget(self.data, self.info.off[colnum], self.info.siz(colnum)) as i64
    }

    /// Extract string from byte data for specified column.
    pub fn str(&self, db: &DB, colnum: usize) -> String {
        let off = self.info.off[colnum];
        let size = self.info.siz(colnum);
        let bytes = get_bytes(db, &self.data[off..], size).0;
        String::from_utf8(bytes).unwrap()
    }

    /// Extract Id from byte data.
    pub fn id(&self) -> u64 {
        util::getu64(self.data, 0)
    }
}

/// Helper class to write byte data using ColInfo.
pub struct WriteAccess<'d, 'i> {
    pub data: &'d mut [u8],
    pub info: &'i ColInfo,
}

impl<'d, 'i> WriteAccess<'d, 'i> {
    /// Save int to byte data.
    pub fn set_int(&mut self, colnum: usize, val: i64) {
        util::set(
            self.data,
            self.info.off[colnum],
            val as u64,
            self.info.siz(colnum),
        );
    }

    /// Extract int from byte data for column number colnum.
    pub fn int(&self, colnum: usize) -> i64 {
        util::get(self.data, self.info.off[colnum], self.info.siz(colnum)) as i64
    }

    /// Extract Id from byte data.
    pub fn id(&self) -> u64 {
        util::getu64(self.data, 0)
    }
}

/// Table name, column names/types and other calculated values for a table.
pub struct ColInfo {
    /// Table name.
    pub name: ObjRef,
    /// Map from column name to column number.
    pub colmap: BTreeMap<String, usize>,
    /// Column names.
    pub colnames: Vec<String>,
    /// Column types.
    pub typ: Vec<DataType>,
    /// Column offsets.
    pub off: Vec<usize>,
    /// Total data size, including Id.
    pub total: usize,
}

impl ColInfo {
    /// Construct an empty ColInfo struct with no columns.
    pub fn empty(name: ObjRef) -> Self {
        ColInfo {
            name,
            colmap: BTreeMap::new(),
            typ: Vec::new(),
            colnames: Vec::new(),
            off: Vec::new(),
            total: 8,
        }
    }

    /// Construct a new ColInfo struct using supplied list of column names and types.
    pub fn new(name: ObjRef, ct: &[(&str, DataType)]) -> Self {
        let mut result = Self::empty(name);
        for (n, t) in ct {
            result.add((*n).to_string(), *t);
        }
        result
    }

    /// Add a column. If the column already exists ( an error ) the result is true.
    pub fn add(&mut self, name: String, typ: DataType) -> bool {
        if self.colmap.contains_key(&name) {
            return true;
        }
        let cn = self.typ.len();
        self.typ.push(typ);
        let size = data_size(typ);
        self.off.push(self.total);
        self.total += size;
        self.colnames.push(name.clone());
        self.colmap.insert(name, cn);
        false
    }

    pub fn add_altered(&mut self, ci: &ColInfo, cnum: usize, actions: &[AlterCol]) -> bool {
        let cname = &ci.colnames[cnum];
        let mut typ = ci.typ[cnum];
        for act in actions {
            match act {
                AlterCol::Drop(name) => {
                    if name == cname {
                        return false;
                    }
                }
                AlterCol::Modify(name, dt) => {
                    if name == cname {
                        if data_kind(typ) != data_kind(*dt) {
                            panic!("Cannot change column data kind");
                        }
                        typ = *dt;
                    }
                }
                _ => {}
            }
        }
        self.add(cname.clone(), typ);
        true
    }

    /// Get a column number from a column name.
    /// usize::MAX is returned for "Id".
    pub fn get(&self, name: &str) -> Option<&usize> {
        if name == "Id" {
            Some(&usize::MAX)
        } else {
            self.colmap.get(name)
        }
    }

    /// Get the data size of specified column.
    fn siz(&self, col: usize) -> usize {
        data_size(self.typ[col])
    }

    /// Calculate the total data size for a list of index columns.
    fn index_key_size(&self, cols: &[usize]) -> usize {
        cols.iter().map(|cnum| self.siz(*cnum)).sum()
    }
} // impl ColInfo

/// Index information for creating an index.
pub struct IndexInfo {
    pub tname: ObjRef,
    pub iname: String,
    pub cols: Vec<usize>,
}

/// Row of Values, with type information.
#[derive(Clone)]
pub struct Row {
    pub id: i64,
    pub values: Vec<Value>,
    pub info: Rc<ColInfo>,
    pub codes: Vec<Code>,
}

impl Row {
    /// Construct a new row, values are initialised to defaults.
    pub fn new(info: Rc<ColInfo>) -> Self {
        let mut result = Row {
            id: 0,
            values: Vec::new(),
            info,
            codes: Vec::new(),
        };
        for t in &result.info.typ {
            result.values.push(Value::default(*t));
        }
        result
    }

    /// Calculate codes for current row values.
    pub fn encode(&mut self, db: &DB) {
        self.codes.clear();
        for (i, val) in self.values.iter().enumerate() {
            let size = data_size(self.info.typ[i]);
            let u = db.encode(val, size);
            self.codes.push(u);
        }
    }

    /// Delete current codes.
    pub fn delcodes(&self, db: &DB) {
        for u in &self.codes {
            if u.id != u64::MAX {
                db.delcode(*u);
            }
        }
    }

    /// Load the row values and codes from data.
    pub fn load(&mut self, db: &DB, data: &[u8]) {
        self.values.clear();
        self.codes.clear();
        self.id = util::getu64(data, 0) as i64;
        let mut off = 8;
        for typ in &self.info.typ {
            let (val, code) = Value::load(db, *typ, data, off);
            self.values.push(val);
            self.codes.push(code);
            off += data_size(*typ);
        }
    }
}

impl Record for Row {
    fn save(&self, data: &mut [u8]) {
        util::setu64(data, self.id as u64);
        let t = &self.info;
        let mut off = 8;
        for (i, typ) in t.typ.iter().enumerate() {
            self.values[i].save(*typ, data, off, self.codes[i]);
            off += data_size(*typ);
        }
    }

    fn compare(&self, _db: &DB, data: &[u8]) -> Ordering {
        let id = util::getu64(data, 0) as i64;
        self.id.cmp(&id)
    }
}

/// Row for inserting into an index.
pub struct IndexRow {
    pub tinfo: Rc<ColInfo>,
    pub cols: Rc<Vec<usize>>,
    pub keys: Vec<Value>,
    pub codes: Vec<Code>,
    pub rowid: i64,
}

impl IndexRow {
    // Construct IndexRow from Row.
    fn new(table: &Table, cols: Rc<Vec<usize>>, row: &Row) -> Self {
        let mut keys = Vec::new();
        let mut codes = Vec::new();
        if !row.codes.is_empty() {
            for c in &*cols {
                keys.push(row.values[*c].clone());
                codes.push(row.codes[*c]);
            }
        }
        Self {
            tinfo: table.info.clone(),
            cols,
            rowid: row.id,
            keys,
            codes,
        }
    }

    // Load IndexRow from data ( note: new codes are computed, as old codes may be deleted ).
    // Since it's unusual for long strings to be keys, code computation should be rare.
    fn load(&mut self, db: &DB, data: &[u8]) {
        self.rowid = util::getu64(data, 0) as i64;
        let mut off = 8;
        for col in &*self.cols {
            let typ = self.tinfo.typ[*col];
            let val = Value::load(db, typ, data, off).0;
            let size = data_size(typ);
            let code = db.encode(&val, size);
            self.keys.push(val);
            self.codes.push(code);
            off += size;
        }
    }
}

impl Record for IndexRow {
    fn compare(&self, db: &DB, data: &[u8]) -> Ordering {
        let mut ix = 0;
        let mut off = 8;
        loop {
            let typ = self.tinfo.typ[self.cols[ix]];
            // Could have special purpose Value method which compares instead of loading to save heap allocations.
            let val = Value::load(db, typ, data, off).0;
            let cf = val.cmp(&self.keys[ix]);
            if cf != Ordering::Equal {
                return cf;
            }
            ix += 1;
            off += data_size(typ);
            if ix == self.cols.len() {
                let rowid = util::getu64(data, 0) as i64;
                return self.rowid.cmp(&rowid);
            }
        }
    }

    fn save(&self, data: &mut [u8]) {
        util::setu64(data, self.rowid as u64);
        let mut off = 8;
        for (ix, k) in self.keys.iter().enumerate() {
            let typ = self.tinfo.typ[self.cols[ix]];
            k.save(typ, data, off, self.codes[ix]);
            off += data_size(typ);
        }
    }

    fn key(&self, db: &DB, data: &[u8]) -> Box<dyn Record> {
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

    fn drop_key(&self, db: &DB, data: &[u8]) {
        let mut off = 8;
        for col in &*self.cols {
            let typ = self.tinfo.typ[*col];
            let code = Value::load(db, typ, data, off).1;
            if code.id != u64::MAX {
                db.delcode(code);
            }
            off += data_size(typ);
        }
    }
}

/// Key for searching index.
pub struct IndexKey {
    pub tinfo: Rc<ColInfo>,
    pub cols: Rc<Vec<usize>>,
    pub key: Vec<Value>,
    pub def: Ordering,
}

impl IndexKey {
    fn new(table: &Table, cols: Rc<Vec<usize>>, key: Vec<Value>, def: Ordering) -> Self {
        Self {
            tinfo: table.info.clone(),
            key,
            cols,
            def,
        }
    }
}

impl Record for IndexKey {
    fn compare(&self, db: &DB, data: &[u8]) -> Ordering {
        let mut ix = 0;
        let mut off = 8;
        loop {
            if ix == self.key.len() {
                return self.def;
            }
            let typ = self.tinfo.typ[self.cols[ix]];
            let val = Value::load(db, typ, data, off).0;
            let cf = val.cmp(&self.key[ix]);
            if cf != Ordering::Equal {
                return cf;
            }
            ix += 1;
            off += data_size(typ);
        }
    }
}

/// State for fetching records using an index.
pub struct IndexScan {
    ixa: Asc,
    table: TablePtr,
    db: DB,
    cols: Rc<Vec<usize>>,
    keys: Vec<Value>,
}

impl IndexScan {
    fn keys_equal(&self, data: &[u8]) -> bool {
        let mut off = 8;
        for (ix, k) in self.keys.iter().enumerate() {
            let typ = self.table.info.typ[self.cols[ix]];
            let val = Value::load(&self.db, typ, data, off).0;
            let cf = val.cmp(k);
            if cf != Ordering::Equal {
                return false;
            }
            off += data_size(typ);
        }
        true
    }
}

impl Iterator for IndexScan {
    type Item = (PagePtr, usize);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some((pp, off)) = self.ixa.next() {
            let p = pp.borrow();
            let data = &p.data[off..];
            if !self.keys_equal(data) {
                return None;
            }
            let id = util::getu64(data, 0);
            return self.table.id_get(&self.db, id);
        }
        None
    }
}

/// State for fetching record with specified id.
pub struct IdScan {
    id: i64,
    table: TablePtr,
    db: DB,
    done: bool,
}

impl Iterator for IdScan {
    type Item = (PagePtr, usize);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.done {
            return None;
        }
        self.done = true;
        self.table.id_get(&self.db, self.id as u64)
    }
}

/// Gets the list of columns that are known from a WHERE condition.
fn get_known_cols(we: &Expr, kc: &mut SmallSet) {
    match &we.exp {
        ExprIs::Binary(Token::Equal, e1, e2) => {
            if e2.is_constant {
                if let ExprIs::ColName(_) = &e1.exp {
                    kc.insert(e1.col);
                }
            } else if e1.is_constant {
                if let ExprIs::ColName(_) = &e2.exp {
                    kc.insert(e2.col);
                }
            }
        }
        ExprIs::Binary(Token::And, e1, e2) => {
            get_known_cols(e1, kc);
            get_known_cols(e2, kc);
        }
        _ => {}
    }
}

/// Counts the number of index columns that are known.
fn covered(clist: &[usize], kc: &SmallSet) -> usize {
    let mut result = 0;
    for &c in clist {
        if !kc.contains(c) {
            break;
        }
        result += 1;
    }
    result
}

/// Get keys. Returns compiled bool expression ( taking into account conditions satisfied by index ).
fn get_keys(
    b: &Block,
    we: &mut Expr,
    cols: &mut SmallSet,
    keys: &mut BTreeMap<usize, CExpPtr<Value>>,
) -> Option<CExpPtr<bool>> {
    match &mut we.exp {
        ExprIs::Binary(Token::Equal, e1, e2) => {
            if e2.is_constant {
                if let ExprIs::ColName(_) = &e1.exp {
                    if cols.remove(e1.col) {
                        keys.insert(e1.col, c_value(b, e2));
                        return None;
                    }
                }
            } else if e1.is_constant {
                if let ExprIs::ColName(_) = &e2.exp {
                    if cols.remove(e2.col) {
                        keys.insert(e2.col, c_value(b, e1));
                        return None;
                    }
                }
            }
        }
        ExprIs::Binary(Token::And, e1, e2) => {
            let x1 = get_keys(b, e1, cols, keys);
            let x2 = get_keys(b, e2, cols, keys);

            return if let Some(c1) = x1 {
                if let Some(c2) = x2 {
                    Some(Box::new(cexp::And { c1, c2 }))
                } else {
                    Some(c1)
                }
            } else {
                x2
            };
        }
        _ => {}
    }
    return Some(c_bool(b, we));
}

/// Compare table rows.
pub fn row_compare(a: &[Value], b: &[Value], desc: &[bool]) -> Ordering {
    let mut ix = 0;
    loop {
        let cmp = a[ix].cmp(&b[ix]);
        if cmp != Ordering::Equal {
            if !desc[ix] {
                return cmp;
            };
            return if cmp == Ordering::Less {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        ix += 1;
        if ix == desc.len() {
            return Ordering::Equal;
        }
    }
}
