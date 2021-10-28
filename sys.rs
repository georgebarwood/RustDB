use crate::*;

/// Creates a schema in the database by writing to the system Schema table.
pub fn create_schema(db: &DB, name: &str)
{
  if let Some(_id) = get_schema(db, name)
  {
    panic!("Schema '{}' already exists", name);
  }
  let t = &db.sys_schema;
  let mut row = t.row();
  row.id = t.alloc_id() as i64;
  row.values[0] = Value::String(Rc::new(name.to_string()));
  t.insert(db, &mut row);
}

/// Create a new table in the database by writing to the system Table and Column tables.
pub fn create_table(db: &DB, info: &ColInfo)
{
  if let Some(_t) = get_table(db, &info.name)
  {
    panic!("Table {} already exists", info.name.to_str());
  }
  let tid = {
    let schema = &info.name.schema;
    if let Some(schema_id) = get_schema(db, schema)
    {
      let t = &db.sys_table;
      let mut row = t.row();
      // Columns are root, schema, name, is_view, definition, id_alloc
      row.id = t.alloc_id() as i64;
      row.values[0] = Value::Int(db.file.alloc_page() as i64);
      row.values[1] = Value::Int(schema_id);
      row.values[2] = Value::String(Rc::new(info.name.name.clone()));
      row.values[3] = Value::Bool(false);
      row.values[4] = Value::String(Rc::new(String::new()));
      row.values[5] = Value::Int(1);
      t.insert(db, &mut row);
      row.id
    }
    else
    {
      panic!("Schema not found [{}]", &schema);
    }
  };
  {
    let cnames = &info.colnames;
    let t = &db.sys_column;
    let mut row = t.row();
    row.values[0] = Value::Int(tid);
    for (num, typ) in info.typ.iter().enumerate()
    {
      // Columns are Table, Name, Type
      row.id = t.alloc_id();
      row.values[1] = Value::String(Rc::new(cnames[num].to_string()));
      row.values[2] = Value::Int(*typ as i64);
      t.insert(db, &mut row);
    }
  }
}

/// Create a new table index by writing to the system Index and IndexColumn tables.
pub fn create_index(db: &DB, info: &IndexInfo)
{
  if let Some(table) = db.get_table(&info.tname)
  {
    let root = db.file.alloc_page();
    let index_id = {
      let t = &db.sys_index;
      let mut row = t.row();
      // Columns are Root, Table, Name
      row.id = t.alloc_id() as i64;
      row.values[0] = Value::Int(root as i64);
      row.values[1] = Value::Int(table.id);
      row.values[2] = Value::String(Rc::new(info.iname.clone()));
      t.insert(db, &mut row);
      row.id
    };

    {
      let t = &db.sys_index_col;
      let mut row = t.row();
      for cnum in &info.cols
      {
        // Columns are Index, ColIndex
        row.id = t.alloc_id() as i64;
        row.values[0] = Value::Int(index_id);
        row.values[1] = Value::Int(*cnum as i64);
        t.insert(db, &mut row);
      }
    }
    // ToDo: initialise the index from table data.
    if root > 9
    {
      table.add_index(root, info.cols.clone());
    }
  }
  else
  {
    panic!("table not found: {}", &info.tname.to_str());
  }
}

/// Creates or alters a function in the database by saving the source into the Function system table.
pub fn create_function(db: &DB, name: &ObjRef, source: Rc<String>, alter: bool)
{
  if let Some(schema_id) = get_schema(db, &name.schema)
  {
    let t = db.get_table(&ObjRef::new("sys", "Function")).unwrap();
    if alter
    {
      // Columns are Schema(0), Name(1), Definition(2).
      let keys = vec![Value::Int(schema_id), Value::String(Rc::new(name.name.to_string()))];

      if let Some((p, off)) = t.ix_get(db, &[0, 1], keys)
      {
        let mut p = p.borrow_mut();
        let off = off + t.info.off[2];
        let (val, oldcode) = Value::load(db, STRING, &p.data, off);
        if val.str() != source
        {
          db.delcode(oldcode);
          let val = Value::String(source);
          let newcode = db.encode(&val);
          val.save(STRING, &mut p.data, off, newcode);
          p.dirty = true;
          db.functions_dirty.set(true);
        }
        return;
      }
      panic!("function {} not found", &name.to_str());
    }
    else
    {
      // Create new function.
      let mut row = t.row();
      // Columns are Schema, Name, Definition
      row.id = t.alloc_id() as i64;
      row.values[0] = Value::Int(schema_id);
      row.values[1] = Value::String(Rc::new(name.name.clone()));
      row.values[2] = Value::String(source);
      t.insert(db, &mut row);
    }
  }
  else
  {
    panic!("schema [{}] not found", &name.schema);
  }
}

/// Gets the id of a schema from a name.
fn get_schema(db: &DB, sname: &str) -> Option<i64>
{
  if let Some(id) = db.schemas.borrow().get(sname)
  {
    return Some(*id);
  }
  let t = &db.sys_schema;
  for (p, off) in t.scan(db)
  {
    let p = p.borrow();
    let a = t.access(&p, off);
    if a.str(db, 0) == sname
    {
      let id = a.id();
      db.schemas.borrow_mut().insert(sname.to_string(), id);
      return Some(a.id());
    }
  }
  None
}

/// Get id, root, id_alloc for specified table.
fn get_table0(db: &DB, name: &ObjRef) -> Option<(i64, i64, i64)>
{
  if let Some(schema_id) = get_schema(db, &name.schema)
  {
    let t = &db.sys_table;

    // Columns are root, schema, name, is_view, definition, id_alloc
    let keys = vec![Value::Int(schema_id), Value::String(Rc::new(name.name.to_string()))];

    if let Some((p, off)) = t.ix_get(db, &[1, 2], keys)
    {
      let p = p.borrow();
      let a = t.access(&p, off);
      return Some((a.id(), a.int(0), a.int(5)));
    }
  }
  None
}

/// Gets a table from the database.
pub(crate) fn get_table(db: &DB, name: &ObjRef) -> Option<TablePtr>
{
  if let Some((table_id, root, id_alloc)) = get_table0(db, name)
  {
    let mut info = ColInfo::empty(name.clone());

    // Load columns. Columns are Table, Name, Type
    let t = &db.sys_column;
    let key = Value::Int(table_id);
    for (p, off) in t.scan_key(db, 0, key)
    {
      let p = p.borrow();
      let a = t.access(&p, off);
      debug_assert!(a.int(0) == table_id);
      let cname = a.str(db, 1);
      let ctype = a.int(2) as DataType;
      info.add(cname, ctype);
    }
    let table = Table::new(table_id, root as u64, id_alloc, Rc::new(info));

    // Load indexes. Columns are Root, Table, Name.
    let t = &db.sys_index;
    let key = Value::Int(table_id);
    for (p, off) in t.scan_key(db, 1, key)
    {
      let p = p.borrow();
      let a = t.access(&p, off);
      debug_assert!(a.int(1) == table_id);
      let index_id = a.id();
      let root = a.int(0) as u64;
      let mut cols = Vec::new();
      let t = &db.sys_index_col;
      // Columns are Index, ColIndex
      let key = Value::Int(index_id);
      for (p, off) in t.scan_key(db, 0, key)
      {
        let p = p.borrow();
        let a = t.access(&p, off);
        debug_assert!(a.int(0) == index_id);
        let cnum = a.int(1) as usize;
        cols.push(cnum);
      }
      // println!( "got index root={}", root );
      table.add_index(root, cols);
    }
    db.publish_table(table.clone());
    // println!( "got table {:?}", name.to_str() );
    Some(table)
  }
  else
  {
    None
  }
}

/// Gets then parses a function from the database.
pub(crate) fn get_function(db: &DB, name: &ObjRef) -> Option<FunctionPtr>
{
  if let Some(schema_id) = get_schema(db, &name.schema)
  {
    let t = db.get_table(&ObjRef::new("sys", "Function")).unwrap();

    let keys = vec![Value::Int(schema_id), Value::String(Rc::new(name.name.to_string()))];

    if let Some((p, off)) = t.ix_get(db, &[0, 1], keys)
    {
      let p = p.borrow();
      let a = t.access(&p, off);
      let source = Rc::new(a.str(db, 2));
      let rptr = parse_function(db, source);
      db.functions.borrow_mut().insert(name.clone(), rptr.clone());
      // println!( "got function {:?}", name );
      return Some(rptr);
    }
  }
  None
}

/// Parse a function definition.
fn parse_function(db: &DB, source: Rc<String>) -> FunctionPtr
{
  let mut p = Parser::new(&source, db);
  p.parse_only = true;
  p.parse_function();
  Rc::new(Function {
    compiled: Cell::new(false),
    ilist: RefCell::new(Vec::new()),
    local_typ: p.b.local_typ,
    return_type: p.b.return_type,
    param_count: p.b.param_count,
    source,
  })
}

/// Update the alloc_id field for a table.
pub(crate) fn save_alloc(db: &DB, id: u64, val: i64)
{
  let t = &db.sys_table;
  let (p, off) = t.id_get(db, id).unwrap();
  let mut p = p.borrow_mut();
  let mut wa = t.write_access(&mut p, off);
  wa.set_int(5, val);
  p.dirty = true;
}

/// This is only needed to initialise system tables.
pub(crate) fn get_alloc(db: &DB, id: u64) -> i64
{
  let t = &db.sys_table;
  let (p, off) = t.id_get(db, id).unwrap();
  let p = p.borrow();
  let a = t.access(&p, off);
  a.int(5)
}
