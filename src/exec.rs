use crate::*;
use Instruction::*;

/// Evaluation environment - stack of Values, references to DB and Transaction.
#[non_exhaustive]
pub struct EvalEnv<'r> {
    /// Stack of values, holds function parameters and local variables.
    pub stack: Vec<Value>,
    /// "Base Pointer" - stack index of current parameters and local variables.
    pub bp: usize,
    /// Pointer to Database.
    pub db: DB,
    /// Pointer to Transaction.
    pub tr: &'r mut dyn Transaction,
    /// Function call depth, prevents stack overflow.
    pub call_depth: usize,
}

impl<'r> EvalEnv<'r> {
    /// Construct a new EvalEnv.
    pub fn new(db: DB, tr: &'r mut dyn Transaction) -> Self {
        EvalEnv {
            stack: Vec::with_capacity(64),
            bp: 0,
            db,
            tr,
            call_depth: 0,
        }
    }

    /// Allocate and initialise local variables.
    pub fn alloc_locals(&mut self, dt: &[DataType], param_count: usize) {
        for d in dt.iter().skip(param_count) {
            let v = Value::default(*d);
            self.stack.push(v);
        }
    }

    /// Execute list of instructions.
    pub fn go(&mut self, ilist: &[Instruction]) {
        let n = ilist.len();
        let mut ip = 0;
        while ip < n {
            let i = &ilist[ip];
            ip += 1;
            match i {
                PushConst(x) => self.stack.push((*x).clone()),
                PushValue(e) => {
                    let v = e.eval(self, &[]);
                    self.stack.push(v);
                }
                PushLocal(x) => self.stack.push(self.stack[self.bp + *x].clone()),
                PopToLocal(x) => self.stack[self.bp + *x] = self.stack.pop().unwrap(),
                Jump(x) => ip = *x,
                JumpIfFalse(x, e) => {
                    if !e.eval(self, &[]) {
                        ip = *x;
                    }
                }
                Call(x) => self.call(x),
                Return => break,
                Throw => {
                    let s = self.pop_string();
                    panic!("{}", s);
                }
                Execute => self.execute(),
                DataOp(x) => self.exec_do(x),
                Select(cse) => self.select(cse),
                Set(cse) => self.set(cse),
                ForInit(for_id, cte) => self.for_init(*for_id, cte),
                ForNext(break_id, info) => {
                    if !self.for_next(info) {
                        ip = *break_id;
                    }
                }
                ForSortInit(for_id, cte) => self.for_sort_init(*for_id, cte),
                ForSortNext(break_id, info) => {
                    if !self.for_sort_next(info) {
                        ip = *break_id;
                    }
                }
                // Special push instructions ( optimisations )
                PushInt(e) => {
                    let v = e.eval(self, &[]);
                    self.stack.push(Value::Int(v));
                }
                PushFloat(e) => {
                    let v = e.eval(self, &[]);
                    self.stack.push(Value::Float(v));
                }
                PushBool(e) => {
                    let v = e.eval(self, &[]);
                    self.stack.push(Value::Bool(v));
                }
                // Assign instructions ( optimisations )
                AssignLocal(x, e) => {
                    let v = e.eval(self, &[]);
                    self.stack[self.bp + x] = v;
                }
                AppendLocal(x, e) => {
                    let v = e.eval(self, &[]);
                    self.stack[self.bp + x].append(&v);
                }
                IncLocal(x, e) => {
                    let v = e.eval(self, &[]);
                    self.stack[self.bp + x].inc(&v);
                }
                DecLocal(x, e) => {
                    let v = e.eval(self, &[]);
                    self.stack[self.bp + x].dec(&v);
                }
            }
        }
    } // end fn go

    /// Call a function.
    pub fn call(&mut self, r: &Function) {
        self.call_depth += 1;
        /*
            if let Some(n) = stacker::remaining_stack()
            {
              if n < 64 * 1024 { panic!("stack less than 64k call depth={}", self.call_depth) }
            }
            else
        */
        if self.call_depth > 500 {
            panic!("call depth limit of 500 reached");
        }
        let save_bp = self.bp;
        self.bp = self.stack.len() - r.param_count;
        self.alloc_locals(&r.local_typ, r.param_count);
        self.go(&r.ilist.borrow());
        let pop_count = r.local_typ.len();
        if pop_count > 0 {
            if r.return_type != NONE {
                if r.param_count == 0
                // function result already in correct position.
                {
                    self.discard(pop_count - 1);
                } else {
                    let result = self.stack[self.bp + r.param_count].clone();
                    self.discard(pop_count);
                    self.stack.push(result);
                }
            } else {
                self.discard(pop_count);
            }
        }
        self.bp = save_bp;
        self.call_depth -= 1;
    }

    /// Discard n items from stack.
    fn discard(&mut self, mut n: usize) {
        while n > 0 {
            self.stack.pop();
            n -= 1;
        }
    }

    /// Pop string from the stack.
    fn pop_string(&mut self) -> String {
        if let Value::String(s) = self.stack.pop().unwrap() {
            s.to_string()
        } else {
            panic!()
        }
    }

    /// Execute a ForInit instruction. Constructs For state and assigns it to local variable.
    fn for_init(&mut self, for_id: usize, cte: &CTableExpression) {
        let data_source = self.data_source(cte);
        let fs = util::new(ForState { data_source });
        self.stack[self.bp + for_id] = Value::For(fs);
    }

    /// Evaluate optional where expression.
    fn ok(&mut self, wher: &Option<CExpPtr<bool>>, data: &[u8]) -> bool {
        if let Some(w) = wher {
            w.eval(self, data)
        } else {
            true
        }
    }

    /// Execute a ForNext instruction. Fetches a record from underlying file that satisfies the where condition,
    /// evaluates the expressions and assigns the results to local variables.
    fn for_next(&mut self, info: &ForNextInfo) -> bool {
        loop {
            let next = if let Value::For(fs) = &self.stack[self.bp + info.for_id] {
                fs.borrow_mut().data_source.next()
            } else {
                panic!("jump into FOR loop");
            };
            if let Some((pp, off)) = next {
                let p = pp.borrow();
                let data = &p.data[off..];
                // Eval and check WHERE condition, eval expressions and assign to locals.
                if self.ok(&info.wher, data) {
                    for (i, a) in info.assigns.iter().enumerate() {
                        let val = info.exps[i].eval(self, data);
                        self.assign_local(a, val);
                    }
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    /// Execute ForSortInit instruction. Constructs sorted vector of rows.
    fn for_sort_init(&mut self, for_id: usize, cse: &CFromExpression) {
        let rows = self.get_temp(cse);
        self.stack[self.bp + for_id] = Value::ForSort(util::new(ForSortState { ix: 0, rows }));
    }

    /// Execute ForSortNext instruction. Assigns locals from current row, moves to next row.
    fn for_sort_next(&mut self, info: &(usize, usize, Assigns)) -> bool {
        let (for_id, orderbylen, assigns) = info;
        if let Value::ForSort(fs) = &self.stack[self.bp + for_id] {
            let fs = fs.clone();
            let mut fs = fs.borrow_mut();
            if fs.ix == fs.rows.len() {
                false
            } else {
                fs.ix += 1;
                let row = &fs.rows[fs.ix - 1];
                for (cn, a) in assigns.iter().enumerate() {
                    let val = row[orderbylen + cn].clone();
                    self.assign_local(a, val);
                }
                true
            }
        } else {
            panic!("jump into FOR loop");
        }
    }

    /// Execute SQL string.
    fn execute(&mut self) {
        let s = self.pop_string();
        // println!("EXECUTE {}",s);
        self.db.run(&s, self.tr);
    }

    /// Execute a data operation (DO).
    fn exec_do(&mut self, dop: &DO) {
        match dop {
            DO::Insert(tp, cols, values) => self.insert(tp.clone(), cols, values),
            DO::Update(assigns, from, wher) => self.update(assigns, from, wher),
            DO::Delete(from, wher) => self.delete(from, wher),

            DO::CreateSchema(name) => sys::create_schema(&self.db, name),
            DO::CreateTable(ti) => sys::create_table(&self.db, ti),
            DO::CreateIndex(x) => sys::create_index(&self.db, x),
            DO::CreateFunction(name, source, alter) => {
                sys::create_function(&self.db, name, source.clone(), *alter)
            }
            DO::DropSchema(name) => self.drop_schema(name),
            DO::DropTable(name) => self.drop_table(name),
            DO::DropFunction(name) => self.drop_function(name),
            DO::DropIndex(tname, iname) => self.drop_index(tname, iname),
            DO::AlterTable(tname, actions) => self.alter_table(tname, actions),
        }
    }

    /// Get list of record ids for DELETE/UPDATE.
    fn get_id_list(&mut self, te: &CTableExpression, w: &Option<CExpPtr<bool>>) -> Vec<u64> {
        let mut idlist = Vec::new();

        for (pp, off) in self.data_source(te) {
            let p = pp.borrow();
            let data = &p.data[off..];
            if self.ok(w, data) {
                idlist.push(util::getu64(data, 0));
            }
        }
        idlist
    }

    /// Execute INSERT operation.
    fn insert(&mut self, t: Rc<Table>, cols: &[usize], src: &CTableExpression) {
        if let CTableExpression::Values(x) = src {
            self.insert_values(t, cols, x);
        } else {
            panic!();
        }
    }

    /// Execute a DELETE operation.
    fn delete(&mut self, from: &CTableExpression, w: &Option<CExpPtr<bool>>) {
        let idlist = self.get_id_list(from, w);
        let t = from.table();
        let mut oldrow = t.row();
        for id in idlist {
            // Load oldrow so that any codes are deleted.
            if let Some((pp, off)) = t.id_get(&self.db, id) {
                let p = pp.borrow();
                let data = &p.data[off..];
                oldrow.load(&self.db, data);
            } else {
                unreachable!()
            }
            t.remove(&self.db, &oldrow);
        }
    }

    /// Execute an UPDATE operation.
    fn update(
        &mut self,
        assigns: &[(usize, CExpPtr<Value>)],
        from: &CTableExpression,
        w: &Option<CExpPtr<bool>>,
    ) {
        let idlist = self.get_id_list(from, w);
        let t = from.table();
        let mut oldrow = t.row();
        for id in idlist {
            if let Some((pp, off)) = t.id_get(&self.db, id) {
                let mut newrow = {
                    let p = pp.borrow();
                    let data = &p.data[off..];
                    oldrow.load(&self.db, data);
                    let mut newrow = oldrow.clone();
                    for (col, exp) in assigns {
                        newrow.values[*col] = exp.eval(self, data);
                    }
                    newrow
                };
                // Would be nice to optimise this to minimise re-indexing.
                t.remove(&self.db, &oldrow);
                t.insert(&self.db, &mut newrow);
            }
        }
    }

    /// Get DataSource from CTableExpression.
    fn data_source(&mut self, te: &CTableExpression) -> DataSource {
        match te {
            CTableExpression::Base(t) => Box::new(t.scan(&self.db)),
            CTableExpression::IdGet(t, idexp) => {
                let id = idexp.eval(self, &[]);
                Box::new(t.scan_id(&self.db, id))
            }
            CTableExpression::IxGet(t, val, index) => {
                let mut keys = Vec::new();
                for v in val {
                    keys.push(v.eval(self, &[]));
                }
                Box::new(t.scan_keys(&self.db, keys, *index))
            }
            _ => panic!(),
        }
    }

    /// Execute a SELECT operation.
    fn select(&mut self, cse: &CFromExpression) {
        if let Some(te) = &cse.from {
            let obl = cse.orderby.len();
            let mut temp = Vec::new(); // For sorting.
            for (pp, off) in self.data_source(te) {
                let p = pp.borrow();
                let data = &p.data[off..];
                if self.ok(&cse.wher, data) {
                    let mut values = Vec::new();
                    if obl > 0 {
                        // Push the sort keys.
                        for ce in &cse.orderby {
                            let val = ce.eval(self, data);
                            values.push(val);
                        }
                    }
                    for ce in &cse.exps {
                        let val = ce.eval(self, data);
                        values.push(val);
                    }
                    if obl > 0 {
                        // Save row for later sorting.
                        temp.push(values);
                    } else {
                        // Output directly.
                        self.tr.selected(&values);
                    }
                }
            }
            if obl > 0 {
                // Sort then output the rows.
                temp.sort_by(|a, b| table::row_compare(a, b, &cse.desc));
                for r in &temp {
                    self.tr.selected(&r[obl..]);
                }
            }
        } else {
            let mut values = Vec::new();
            for ce in &cse.exps {
                let val = ce.eval(self, &[]);
                values.push(val);
            }
            self.tr.selected(&values);
        }
    }

    /// Execute a SET operation.
    fn set(&mut self, cse: &CFromExpression) {
        if let Some(te) = &cse.from {
            for (pp, off) in self.data_source(te) {
                let p = pp.borrow();
                let data = &p.data[off..];
                if self.ok(&cse.wher, data) {
                    for (i, ce) in cse.exps.iter().enumerate() {
                        let val = ce.eval(self, data);
                        self.assign_local(&cse.assigns[i], val);
                    }
                    break; // Only one row is used for SET.
                }
            }
        } else {
            for (i, ce) in cse.exps.iter().enumerate() {
                let val = ce.eval(self, &[]);
                self.assign_local(&cse.assigns[i], val);
            }
        }
    }

    /// Assign or append to a local variable.
    fn assign_local(&mut self, a: &(usize, AssignOp), val: Value) {
        let var = &mut self.stack[self.bp + a.0];
        match a.1 {
            AssignOp::Assign => *var = val,
            AssignOp::Append => var.append(&val),
            AssignOp::Inc => var.inc(&val),
            AssignOp::Dec => var.dec(&val),
        }
    }

    /// Insert evaluated values into a table.
    fn insert_values(&mut self, table: Rc<Table>, ci: &[usize], vals: &[Vec<CExpPtr<Value>>]) {
        let mut row = Row::new(table.info.clone());
        for r in vals {
            row.id = 0;
            for (i, ce) in r.iter().enumerate() {
                let val = ce.eval(self, &[]);
                let cn = ci[i];
                if cn == usize::MAX {
                    if let Value::Int(v) = val {
                        row.id = v;
                    }
                } else {
                    row.values[cn] = val;
                }
            }
            if row.id == 0 {
                row.id = table.alloc_id(&self.db);
            } else {
                table.id_allocated(&self.db, row.id);
            }
            self.db.lastid.set(row.id);
            table.insert(&self.db, &mut row);
        }
    }

    /// Get sorted temporary table.
    fn get_temp(&mut self, cse: &CFromExpression) -> Vec<Vec<Value>> {
        if let Some(te) = &cse.from {
            let mut temp = Vec::new(); // For sorting.
            for (pp, off) in self.data_source(te) {
                let p = pp.borrow();
                let data = &p.data[off..];
                if self.ok(&cse.wher, data) {
                    let mut values = Vec::new();
                    for ce in &cse.orderby {
                        let val = ce.eval(self, data);
                        values.push(val);
                    }
                    for ce in &cse.exps {
                        let val = ce.eval(self, data);
                        values.push(val);
                    }
                    temp.push(values); // Save row for later sorting.
                }
            }
            // Sort the rows.
            temp.sort_by(|a, b| table::row_compare(a, b, &cse.desc));
            temp
        } else {
            panic!()
        }
    }

    fn drop_schema(&mut self, name: &str) {
        if let Some(sid) = sys::get_schema(&self.db, name) {
            let sql = format!("EXEC sys.DropSchema({})", sid);
            self.db.run(&sql, self.tr);
            self.db.schemas.borrow_mut().remove(name);
            self.db.function_reset.set(true);
        } else {
            panic!("Drop Schema not found {}", name);
        }
    }

    fn drop_table(&mut self, name: &ObjRef) {
        if let Some(t) = sys::get_table(&self.db, name) {
            let sql = format!("EXEC sys.DropTable({})", t.id);
            self.db.run(&sql, self.tr);
            self.db.tables.borrow_mut().remove(name);
            self.db.function_reset.set(true);
            t.free_pages(&self.db);
        } else {
            panic!("Drop Table not found {}", name.str());
        }
    }

    fn drop_function(&mut self, name: &ObjRef) {
        if let Some(fid) = sys::get_function_id(&self.db, name) {
            let sql = format!("DELETE FROM sys.Function WHERE Id = {}", fid);
            self.db.run(&sql, self.tr);
            self.db.function_reset.set(true);
        } else {
            panic!("Drop Function not found {}", name.str());
        }
    }

    fn drop_index(&mut self, tname: &ObjRef, iname: &str) {
        let (t, ix, id) = sys::get_index(&self.db, tname, iname);
        let sql = format!("EXEC sys.DropIndex({})", id);
        self.db.run(&sql, self.tr);
        self.db.tables.borrow_mut().remove(tname);
        self.db.function_reset.set(true);
        t.delete_index(&self.db, ix);
    }

    fn alter_table(&mut self, name: &ObjRef, actions: &[AlterCol]) {
        let db = &self.db;
        if let Some(t) = sys::get_table(db, name) {
            if t.ixlist.borrow().len() > 0 {
                panic!("alter table indexes have to be dropped first");
            }

            for act in actions {
                match act {
                    AlterCol::Modify(name, _) | AlterCol::Drop(name) => {
                        if !t.info.colmap.contains_key(name) {
                            panic!("column not found {}", name);
                        }
                    }
                    _ => {}
                }
                let sql = match act {
                    AlterCol::Add(name, typ) => format!(
                        "INSERT INTO sys.Column( Table, Name, Type ) VALUES ({}, '{}', {})",
                        t.id, name, typ
                    ),
                    AlterCol::Modify(name, typ) => format!(
                        "UPDATE sys.Column SET Type = {} WHERE Table = {} AND Name = '{}'",
                        typ, t.id, name
                    ),
                    AlterCol::Drop(name) => format!("EXEC sys.DropColumn({},'{}')", t.id, name),
                };
                db.run(&sql, self.tr);
            }

            let mut nci = ColInfo::empty(name.clone());
            let ci = &t.info;

            let mut colmap = Vec::new(); // colmap is columns that need to be copied from old to new table.
            for i in 0..ci.colnames.len() {
                if nci.add_altered(ci, i, actions) {
                    colmap.push(i);
                }
            }

            for act in actions {
                if let AlterCol::Add(name, typ) = act {
                    if nci.add(name.clone(), *typ) {
                        panic!("duplicate column name {}", name);
                    }
                }
            }
            let nci = Rc::new(nci);

            let root = db.alloc_page();
            let nt = Table::new(t.id, root, t.get_id_gen(db), nci);

            let mut oldrow = t.row();
            let mut newrow = nt.row();
            for (pp, off) in t.scan(db) {
                let p = pp.borrow();
                let data = &p.data[off..];
                oldrow.load(db, data);
                newrow.id = oldrow.id;
                for (i, j) in colmap.iter().enumerate() {
                    newrow.values[i] = oldrow.values[*j].clone();
                }
                nt.insert(db, &mut newrow);
            }
            let sql = format!("EXEC sys.ClearTable({})", t.id);
            db.run(&sql, self.tr);
            t.free_pages(db);
            sys::set_root(db, nt.id, root);

            db.tables.borrow_mut().remove(name);
            db.tables.borrow_mut().insert(name.clone(), nt);
            db.function_reset.set(true);
        } else {
            panic!("ALTER TABLE not found {}", name.str());
        }
    }
} // impl EvalEnv
