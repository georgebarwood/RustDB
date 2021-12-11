use crate::*;

/// Create a schema in the database by writing to the system Schema table.
pub fn create_schema(db: &DB, name: &str) {
    if let Some(_id) = get_schema(db, name) {
        panic!("Schema '{}' already exists", name);
    }
    let t = &db.sys_schema;
    let mut row = t.row();
    row.id = t.alloc_id() as i64;
    row.values[0] = Value::String(Rc::new(name.to_string()));
    t.insert(db, &mut row);
}

/// Create a new table in the database by writing to the system Table and Column tables.
pub fn create_table(db: &DB, info: &ColInfo) {
    if let Some(_t) = get_table(db, &info.name) {
        panic!("Table {} already exists", info.name.str());
    }
    let tid = {
        let schema = &info.name.schema;
        if let Some(schema_id) = get_schema(db, schema) {
            let root = db.alloc_page();
            let t = &db.sys_table;
            let mut row = t.row();
            // Columns are root, schema, name, id_gen
            row.id = t.alloc_id() as i64;
            row.values[0] = Value::Int(root as i64);
            row.values[1] = Value::Int(schema_id);
            row.values[2] = Value::String(Rc::new(info.name.name.clone()));
            row.values[3] = Value::Int(1);
            t.insert(db, &mut row);
            row.id
        } else {
            panic!("Schema not found [{}]", &schema);
        }
    };
    {
        let cnames = &info.colnames;
        let t = &db.sys_column;
        let mut row = t.row();
        row.values[0] = Value::Int(tid);
        for (num, typ) in info.typ.iter().enumerate() {
            // Columns are Table, Name, Type
            row.id = t.alloc_id();
            row.values[1] = Value::String(Rc::new(cnames[num].to_string()));
            row.values[2] = Value::Int(*typ as i64);
            t.insert(db, &mut row);
        }
    }
}

/// Create a new table index by writing to the system Index and IndexColumn tables.
pub fn create_index(db: &DB, info: &IndexInfo) {
    if let Some(table) = db.get_table(&info.tname) {
        let root = db.alloc_page();
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
            for cnum in &info.cols {
                // Columns are Index, ColIndex
                row.id = t.alloc_id() as i64;
                row.values[0] = Value::Int(index_id);
                row.values[1] = Value::Int(*cnum as i64);
                t.insert(db, &mut row);
            }
        }
        if root > SYS_ROOT_LAST {
            table.add_index(root, info.cols.clone());
            table.init_index(db);
        }
    } else {
        panic!("table not found: {}", &info.tname.str());
    }
}

/// Create or alter a function in the database by saving the source into the Function system table.
pub fn create_function(db: &DB, name: &ObjRef, source: Rc<String>, alter: bool) {
    if let Some(schema_id) = get_schema(db, &name.schema) {
        let t = &db.sys_function;
        if alter {
            // Columns are Schema(0), Name(1), Definition(2).
            let keys = vec![
                Value::Int(schema_id),
                Value::String(Rc::new(name.name.to_string())),
            ];
            if let Some((pp, off)) = t.ix_get(db, keys, 0) {
                let p = &mut *pp.borrow_mut();
                let off = off + t.info.off[2];
                let (val, oldcode) = Value::load(db, BIGSTR, &p.data, off);
                if val.str() != source {
                    db.delcode(oldcode);
                    let val = Value::String(source);
                    let newcode = db.encode(&val, data_size(BIGSTR));
                    let data = Data::make_mut(&mut p.data);
                    val.save(BIGSTR, data, off, newcode);
                    t.file.set_dirty(p, &pp);
                    db.function_reset.set(true);
                }
                return;
            }
            panic!("function {} not found", &name.str());
        } else {
            if get_function_id(db, name).is_some() {
                panic!("function already exists");
            }
            // Create new function.
            let mut row = t.row();
            // Columns are Schema, Name, Definition
            row.id = t.alloc_id() as i64;
            row.values[0] = Value::Int(schema_id);
            row.values[1] = Value::String(Rc::new(name.name.clone()));
            row.values[2] = Value::String(source);
            t.insert(db, &mut row);
        }
    } else {
        panic!("schema [{}] not found", &name.schema);
    }
}

/// Get the id of a schema from a name.
pub fn get_schema(db: &DB, sname: &str) -> Option<i64> {
    if let Some(&id) = db.schemas.borrow().get(sname) {
        return Some(id);
    }
    let t = &db.sys_schema;
    let keys = vec![Value::String(Rc::new(sname.to_string()))];
    if let Some((pp, off)) = t.ix_get(db, keys, 0) {
        let p = &pp.borrow();
        let a = t.access(p, off);
        debug_assert!(a.str(db, 0) == sname);
        let id = a.id() as i64;
        db.schemas.borrow_mut().insert(sname.to_string(), id);
        return Some(id);
    }
    None
}

/// Get the id, root, id_gen for specified table.
fn get_table0(db: &DB, name: &ObjRef) -> Option<(i64, i64, i64)> {
    if let Some(schema_id) = get_schema(db, &name.schema) {
        let t = &db.sys_table;
        // Columns are root, schema, name, id_gen
        let keys = vec![
            Value::Int(schema_id),
            Value::String(Rc::new(name.name.to_string())),
        ];
        if let Some((pp, off)) = t.ix_get(db, keys, 0) {
            let p = &pp.borrow();
            let a = t.access(p, off);
            return Some((a.id() as i64, a.int(0), a.int(3)));
        }
    }
    None
}

pub fn get_index(db: &DB, tname: &ObjRef, iname: &str) -> (TablePtr, usize, u64) {
    if let Some(t) = get_table(db, tname) {
        // Loop through indexes. Columns are Root, Table, Name.
        let ixt = &db.sys_index;
        let key = Value::Int(t.id);
        for (ix, (pp, off)) in ixt.scan_key(db, key, 0).enumerate() {
            let p = &pp.borrow();
            let a = ixt.access(p, off);
            if a.str(db, 2) == iname {
                let id = a.id();
                return (t, ix, id);
            }
        }
        panic!("index {} not found", iname);
    } else {
        panic!("table {} not found", tname.str());
    }
}

/// Gets a table from the database.
pub fn get_table(db: &DB, name: &ObjRef) -> Option<TablePtr> {
    if let Some((table_id, root, id_gen)) = get_table0(db, name) {
        let mut info = ColInfo::empty(name.clone());
        // Get columns. Columns are Table, Name, Type
        let t = &db.sys_column;
        let key = Value::Int(table_id);
        for (pp, off) in t.scan_key(db, key, 0) {
            let p = &pp.borrow();
            let a = t.access(p, off);
            debug_assert!(a.int(0) == table_id);
            let cname = a.str(db, 1);
            let ctype = a.int(2) as DataType;
            info.add(cname, ctype);
        }
        let table = Table::new(table_id, root as u64, id_gen, Rc::new(info));
        // Get indexes. Columns are Root, Table, Name.
        let t = &db.sys_index;
        let key = Value::Int(table_id);
        for (pp, off) in t.scan_key(db, key, 0) {
            let p = &pp.borrow();
            let a = t.access(p, off);
            debug_assert!(a.int(1) == table_id);
            let index_id = a.id() as i64;
            let root = a.int(0) as u64;
            let mut cols = Vec::new();
            let t = &db.sys_index_col;
            // Columns are Index, ColIndex
            let key = Value::Int(index_id);
            for (pp, off) in t.scan_key(db, key, 0) {
                let p = &pp.borrow();
                let a = t.access(p, off);
                debug_assert!(a.int(0) == index_id);
                let cnum = a.int(1) as usize;
                cols.push(cnum);
            }
            table.add_index(root, cols);
        }
        db.publish_table(table.clone());
        Some(table)
    } else {
        None
    }
}

/// Get then parse a function from the database.
pub fn get_function(db: &DB, name: &ObjRef) -> Option<FunctionPtr> {
    if let Some(schema_id) = get_schema(db, &name.schema) {
        let t = &db.sys_function;
        let keys = vec![
            Value::Int(schema_id),
            Value::String(Rc::new(name.name.to_string())),
        ];
        if let Some((pp, off)) = t.ix_get(db, keys, 0) {
            let p = &pp.borrow();
            let a = t.access(p, off);
            let source = Rc::new(a.str(db, 2));
            let function = parse_function(db, source);
            db.functions
                .borrow_mut()
                .insert(name.clone(), function.clone());
            return Some(function);
        }
    }
    None
}

/// Get the id of a function.
pub fn get_function_id(db: &DB, name: &ObjRef) -> Option<i64> {
    if let Some(schema_id) = get_schema(db, &name.schema) {
        let t = &db.sys_function;
        let keys = vec![
            Value::Int(schema_id),
            Value::String(Rc::new(name.name.to_string())),
        ];
        if let Some((pp, off)) = t.ix_get(db, keys, 0) {
            let p = &pp.borrow();
            let a = t.access(p, off);
            return Some(a.id() as i64);
        }
    }
    None
}

/// Parse a function definition.
fn parse_function(db: &DB, source: Rc<String>) -> FunctionPtr {
    let mut p = Parser::new(&source, db);
    p.b.parse_only = true;
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

/// Get the IdGen field for a table. This is only needed to initialise system tables.
pub fn get_id_gen(db: &DB, id: u64) -> i64 {
    let t = &db.sys_table;
    let (pp, off) = t.id_get(db, id).unwrap();
    let p = &pp.borrow();
    let a = t.access(p, off);
    debug_assert!(a.id() == id);
    a.int(3)
}

/// Update IdGen field for a table.
pub fn save_id_gen(db: &DB, id: u64, val: i64) {
    let t = &db.sys_table;
    let (pp, off) = t.id_get(db, id).unwrap();
    let p = &mut pp.borrow_mut();
    let mut wa = t.write_access(p, off);
    debug_assert!(wa.id() == id);
    wa.set_int(3, val);
    t.file.set_dirty(p, &pp);
}

/// Update root page for table ( for ALTER TABLE ).
pub fn set_root(db: &DB, id: i64, new_root: u64) {
    let id = id as u64;
    let t = &db.sys_table;
    let (pp, off) = t.id_get(db, id).unwrap();
    let p = &mut pp.borrow_mut();
    let mut wa = t.write_access(p, off);
    debug_assert!(wa.id() == id);
    wa.set_int(0, new_root as i64);
    t.file.set_dirty(p, &pp);
}
