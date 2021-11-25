use crate::*;
use std::{mem, ops};
use Instruction::*;

/// Calculate various attributes such as data_type, is_constant etc.
pub fn c_check(b: &Block, e: &mut Expr) {
    if e.checked {
        return;
    }
    e.is_constant = true;
    match &mut e.exp {
        ExprIs::BuiltinCall(name, args) => {
            if let Some((dk, _cf)) = b.db.builtins.borrow().get(name) {
                e.data_type = *dk as DataType;
                for pe in args {
                    c_check(b, pe);
                    if !pe.is_constant {
                        e.is_constant = false;
                    }
                }
            } else {
                panic!("Unknown function {}", name);
            }
        }
        ExprIs::Binary(op, b1, b2) => {
            c_check(b, b1);
            c_check(b, b2);
            e.is_constant = b1.is_constant && b2.is_constant;
            let t1 = b1.data_type;
            let t2 = b2.data_type;
            if data_kind(t1) != data_kind(t2) && *op != Token::VBar {
                panic!("Binary op type mismatch")
            }
            e.data_type = match op {
                Token::Less
                | Token::LessEqual
                | Token::GreaterEqual
                | Token::Greater
                | Token::Equal
                | Token::NotEqual => BOOL,
                Token::And | Token::Or => {
                    if t1 != BOOL {
                        panic!("And/Or need bool operands")
                    }
                    BOOL
                }
                Token::Plus | Token::Times | Token::Minus | Token::Divide | Token::Percent => t1,
                Token::VBar => STRING,
                _ => panic!(),
            }
        }
        ExprIs::Local(x) => {
            e.data_type = b.local_typ[*x];
        }
        ExprIs::Const(x) => {
            e.data_type = match *x {
                Value::Bool(_) => BOOL,
                Value::Int(_) => BIGINT,
                Value::Float(_) => DOUBLE,
                Value::String(_) => STRING,
                Value::RcBinary(_) => BINARY,
                Value::ArcBinary(_) => BINARY,
                _ => NONE,
            }
        }
        ExprIs::Case(x, els) => {
            c_check(b, els);
            if !els.is_constant {
                e.is_constant = false;
            }
            e.data_type = els.data_type;
            for (w, t) in x {
                c_check(b, w);
                if !w.is_constant {
                    e.is_constant = false;
                }
                c_check(b, t);
                if !t.is_constant {
                    e.is_constant = false;
                }
                if data_kind(e.data_type) != data_kind(t.data_type) {
                    panic!("CASE branch type mismatch");
                }
            }
        }
        ExprIs::Not(x) => {
            c_check(b, x);
            e.is_constant = x.is_constant;
            e.data_type = BOOL;
        }
        ExprIs::Minus(x) => {
            c_check(b, x);
            e.is_constant = x.is_constant;
            e.data_type = x.data_type;
        }
        ExprIs::FuncCall(name, parms) => {
            let f = c_function(b, name);
            e.data_type = f.return_type;
            if parms.len() != f.param_count {
                panic!(
                    "function parameter count mismatch expected {} got {}",
                    f.param_count,
                    parms.len()
                );
            }
            for (i, a) in parms.iter_mut().enumerate() {
                c_check(b, a);
                let (t, et) = (data_kind(a.data_type), data_kind(f.local_typ[i]));
                if t != et {
                    panic!("function param type mismatch expected {:?} got {:?}", et, t);
                }
                if !a.is_constant {
                    e.is_constant = false;
                }
            }
        }
        ExprIs::ColName(x) => {
            e.is_constant = false;
            let (col, data_type) = name_to_colnum(b, x);
            e.col = col;
            e.data_type = data_type;
        }
        _ => panic!(),
    }
    e.checked = true;
}
/// Compile a call to a builtin function that returns a Value.
fn c_builtin_value(b: &Block, name: &str, args: &mut [Expr]) -> CExpPtr<Value> {
    if let Some((_dk, CompileFunc::Value(cf))) = b.db.builtins.borrow().get(name) {
        return cf(b, args);
    }
    panic!()
}
/// Compile an expression.
pub fn c_value(b: &Block, e: &mut Expr) -> CExpPtr<Value> {
    match b.kind(e) {
        DataKind::Bool => Box::new(cexp::BoolToVal { ce: c_bool(b, e) }),
        DataKind::Int => Box::new(cexp::IntToVal { ce: c_int(b, e) }),
        DataKind::Float => Box::new(cexp::FloatToVal { ce: c_float(b, e) }),
        _ => match &mut e.exp {
            ExprIs::ColName(x) => {
                let (off, typ) = name_to_col(b, x);
                match typ {
                    STRING => Box::new(cexp::ColumnString { off }),
                    BINARY => Box::new(cexp::ColumnBinary { off }),
                    _ => panic!(),
                }
            }
            ExprIs::Const(x) => Box::new(cexp::Const {
                value: (*x).clone(),
            }),
            ExprIs::Local(x) => Box::new(cexp::Local { num: *x }),
            ExprIs::Binary(op, b1, b2) => {
                let c1 = c_value(b, b1);
                let c2 = c_value(b, b2);
                match op {
                    Token::VBar => Box::new(cexp::Concat { c1, c2 }),
                    _ => panic!("Invalid operator {:?}", op),
                }
            }
            ExprIs::FuncCall(name, parms) => c_call(b, name, parms),
            ExprIs::Case(list, els) => c_case(b, list, els, c_value),
            ExprIs::BuiltinCall(name, parms) => c_builtin_value(b, name, parms),
            _ => panic!(),
        },
    }
}
/// Compile int expression.
pub fn c_int(b: &Block, e: &mut Expr) -> CExpPtr<i64> {
    if b.kind(e) != DataKind::Int {
        panic!("Integer type expected")
    }
    match &mut e.exp {
        ExprIs::ColName(x) => {
            let (off, typ) = name_to_col(b, x);
            match data_size(typ) {
                8 => Box::new(cexp::ColumnI64 { off }),
                4 => Box::new(cexp::ColumnI32 { off }),
                2 => Box::new(cexp::ColumnI16 { off }),
                1 => Box::new(cexp::ColumnI8 { off }),
                _ => panic!(),
            }
        }
        ExprIs::Const(Value::Int(b)) => Box::new(cexp::Const::<i64> { value: *b }),
        ExprIs::Local(num) => Box::new(cexp::Local { num: *num }),
        ExprIs::Binary(op, b1, b2) => c_arithmetic(b, *op, b1, b2, c_int),
        ExprIs::Minus(x) => Box::new(cexp::Minus::<i64> { ce: c_int(b, x) }),
        ExprIs::Case(w, e) => c_case(b, w, e, c_int),
        ExprIs::FuncCall(n, a) => Box::new(cexp::ValToInt {
            ce: c_call(b, n, a),
        }),
        ExprIs::BuiltinCall(n, a) => c_builtin_int(b, n, a),
        _ => panic!(),
    }
}
/// Compile float expression.
pub fn c_float(b: &Block, e: &mut Expr) -> CExpPtr<f64> {
    if b.kind(e) != DataKind::Float {
        panic!("Float type expected")
    }
    match &mut e.exp {
        ExprIs::ColName(x) => {
            let (off, typ) = name_to_col(b, x);
            match data_size(typ) {
                8 => Box::new(cexp::ColumnF64 { off }),
                4 => Box::new(cexp::ColumnF32 { off }),
                _ => panic!(),
            }
        }
        ExprIs::Local(num) => Box::new(cexp::Local { num: *num }),
        ExprIs::Binary(op, b1, b2) => c_arithmetic(b, *op, b1, b2, c_float),
        ExprIs::Minus(x) => Box::new(cexp::Minus::<f64> { ce: c_float(b, x) }),
        ExprIs::Case(w, e) => c_case(b, w, e, c_float),
        ExprIs::FuncCall(n, a) => Box::new(cexp::ValToFloat {
            ce: c_call(b, n, a),
        }),
        ExprIs::BuiltinCall(n, a) => c_builtin_float(b, n, a),
        _ => panic!(),
    }
}
/// Compile bool expression.
pub fn c_bool(b: &Block, e: &mut Expr) -> CExpPtr<bool> {
    if b.kind(e) != DataKind::Bool {
        panic!("Bool type expected")
    }
    match &mut e.exp {
        ExprIs::ColName(x) => {
            let (off, _typ) = name_to_col(b, x);
            Box::new(cexp::ColumnBool { off })
        }
        ExprIs::Const(Value::Bool(b)) => Box::new(cexp::Const::<bool> { value: *b }),
        ExprIs::Local(x) => Box::new(cexp::Local { num: *x }),
        ExprIs::Binary(op, b1, b2) => {
            if *op == Token::Or || *op == Token::And {
                let c1 = c_bool(b, b1);
                let c2 = c_bool(b, b2);
                match op {
                    Token::Or => Box::new(cexp::Or { c1, c2 }),
                    Token::And => Box::new(cexp::And { c1, c2 }),
                    _ => panic!(),
                }
            } else {
                match b.kind(b1) {
                    DataKind::Bool => c_compare(b, *op, b1, b2, c_bool),
                    DataKind::Int => c_compare(b, *op, b1, b2, c_int),
                    DataKind::Float => c_compare(b, *op, b1, b2, c_float),
                    _ => c_compare(b, *op, b1, b2, c_value),
                }
            }
        }
        ExprIs::Not(x) => Box::new(cexp::Not { ce: c_bool(b, x) }),
        ExprIs::FuncCall(name, parms) => Box::new(cexp::ValToBool {
            ce: c_call(b, name, parms),
        }),
        ExprIs::Case(list, els) => c_case(b, list, els, c_bool),
        _ => panic!(),
    }
}
/// Compile arithmetic.
fn c_arithmetic<T>(
    b: &Block,
    op: Token,
    e1: &mut Expr,
    e2: &mut Expr,
    cexp: fn(&Block, &mut Expr) -> CExpPtr<T>,
) -> CExpPtr<T>
where
    T: 'static
        + ops::Add<Output = T>
        + ops::Sub<Output = T>
        + ops::Mul<Output = T>
        + ops::Div<Output = T>
        + ops::Rem<Output = T>,
{
    let c1 = cexp(b, e1);
    let c2 = cexp(b, e2);
    match op {
        Token::Plus => Box::new(cexp::Add::<T> { c1, c2 }),
        Token::Minus => Box::new(cexp::Sub::<T> { c1, c2 }),
        Token::Times => Box::new(cexp::Mul::<T> { c1, c2 }),
        Token::Divide => Box::new(cexp::Div::<T> { c1, c2 }),
        Token::Percent => Box::new(cexp::Rem::<T> { c1, c2 }),
        _ => panic!(),
    }
}
/// Compile comparison.
fn c_compare<T>(
    b: &Block,
    op: Token,
    e1: &mut Expr,
    e2: &mut Expr,
    cexp: fn(&Block, &mut Expr) -> CExpPtr<T>,
) -> CExpPtr<bool>
where
    T: 'static + std::cmp::PartialOrd,
{
    let c1 = cexp(b, e1);
    let c2 = cexp(b, e2);
    match op {
        Token::Equal => Box::new(cexp::Equal::<T> { c1, c2 }),
        Token::NotEqual => Box::new(cexp::NotEqual::<T> { c1, c2 }),
        Token::Less => Box::new(cexp::Less::<T> { c1, c2 }),
        Token::Greater => Box::new(cexp::Greater::<T> { c1, c2 }),
        Token::LessEqual => Box::new(cexp::LessEqual::<T> { c1, c2 }),
        Token::GreaterEqual => Box::new(cexp::GreaterEqual::<T> { c1, c2 }),
        _ => panic!(),
    }
}
/// Compile CASE Expression.
fn c_case<T>(
    b: &Block,
    wes: &mut [(Expr, Expr)],
    els: &mut Expr,
    cexp: fn(&Block, &mut Expr) -> CExpPtr<T>,
) -> CExpPtr<T>
where
    T: 'static,
{
    let mut whens = Vec::new();
    for (be, ve) in wes {
        let cb = c_bool(b, be);
        let v = cexp(b, ve);
        whens.push((cb, v));
    }
    let els = cexp(b, els);
    Box::new(cexp::Case::<T> { whens, els })
}
/// Compile a call to a builtin function that returns an integer.
fn c_builtin_int(b: &Block, name: &str, args: &mut [Expr]) -> CExpPtr<i64> {
    if let Some((_dk, CompileFunc::Int(cf))) = b.db.builtins.borrow().get(name) {
        return cf(b, args);
    }
    panic!()
}
/// Compile a call to a builtin function that returns a float.
fn c_builtin_float(b: &Block, name: &str, args: &mut [Expr]) -> CExpPtr<f64> {
    if let Some((_dk, CompileFunc::Float(cf))) = b.db.builtins.borrow().get(name) {
        return cf(b, args);
    }
    panic!()
}

/// Compile UPDATE statement.
pub fn c_update(
    b: &mut Block,
    tname: &ObjRef,
    assigns: &mut Vec<(String, Expr)>,
    wher: &mut Option<Expr>,
) {
    let t = c_table(b, tname);
    let from = CTableExpression::Base(t.clone());
    let save = mem::replace(&mut b.from, Some(from));
    let mut se = Vec::new();
    for (name, exp) in assigns.iter_mut() {
        if let Some(cnum) = t.info.colmap.get(name) {
            let exp = c_value(b, exp);
            se.push((*cnum, exp));
        } else {
            panic!("update column name not found");
        }
    }
    let (w, index_from) = c_where(b, Some(t), wher);
    let mut from = mem::replace(&mut b.from, save);
    if index_from.is_some() {
        from = index_from;
    }
    b.dop(DO::Update(se, from.unwrap(), w));
}

/// Compile DELETE statement.
pub fn c_delete(b: &mut Block, tname: &ObjRef, wher: &mut Option<Expr>) {
    let t = c_table(b, tname);
    let from = Some(CTableExpression::Base(t.clone()));
    let save = mem::replace(&mut b.from, from);
    let (w, index_from) = c_where(b, Some(t), wher);
    let mut from = mem::replace(&mut b.from, save);
    if index_from.is_some() {
        from = index_from;
    }
    b.dop(DO::Delete(from.unwrap(), w));
}

/// Compile SelectExpression to CSelectExpression.
pub fn c_select(b: &mut Block, mut x: SelectExpression) -> CSelectExpression {
    let mut from = x.from.map(|mut te| c_te(b, &mut te));
    let table = match &from {
        Some(CTableExpression::Base(t)) => Some(t.clone()),
        _ => None,
    };
    // Is the save necessary?
    let save = mem::replace(&mut b.from, from);
    let mut exps = Vec::new();
    for (i, e) in x.exps.iter_mut().enumerate() {
        exps.push(c_value(b, e));
        if !x.assigns.is_empty() {
            // Check data kind of assigned local matches data kind of expression.
            let (lnum, _) = x.assigns[i];
            let ek = data_kind(b.local_typ[lnum]);
            let ak = data_kind(e.data_type);
            if ek != ak {
                panic!("cannot assign {:?} to {:?}", ak, ek);
            }
        }
    }
    let (wher, index_from) = c_where(b, table, &mut x.wher);
    let mut orderby = Vec::new();
    let mut desc = Vec::new();
    for (e, a) in &mut x.orderby {
        let e = c_value(b, e);
        orderby.push(e);
        desc.push(*a);
    }
    from = mem::replace(&mut b.from, save);
    if index_from.is_some() {
        from = index_from;
    }
    CSelectExpression {
        colnames: x.colnames,
        assigns: x.assigns,
        exps,
        from,
        wher,
        orderby,
        desc,
    }
}

/// Compile WHERE clause, using table index if possible.
pub fn c_where(
    b: &Block,
    table: Option<TablePtr>,
    wher: &mut Option<Expr>,
) -> (Option<CExpPtr<bool>>, Option<CTableExpression>) {
    if let Some(we) = wher {
        if b.kind(we) != DataKind::Bool {
            panic!("WHERE expression must be bool")
        }
        if let Some(table) = table {
            table.index_from(b, we)
        } else {
            (Some(c_bool(b, we)), None)
        }
    } else {
        (None, None)
    }
}

/// Compile a TableExpression to CTableExpression.
pub fn c_te(b: &Block, te: &mut TableExpression) -> CTableExpression {
    match te {
        TableExpression::Values(x) => {
            let mut cm = Vec::new();
            for r in x {
                let mut cr = Vec::new();
                for e in r {
                    let ce = c_value(b, e);
                    cr.push(ce);
                }
                cm.push(cr);
            }
            CTableExpression::Values(cm)
        }
        TableExpression::Base(x) => {
            let t = c_table(b, x);
            CTableExpression::Base(t)
        }
    }
}
/// Look for named table in database.
pub fn c_table(b: &Block, name: &ObjRef) -> TablePtr {
    if let Some(t) = b.db.get_table(name) {
        t
    } else {
        panic!("table {} not found", name.str())
    }
}
/// Compile named function (if it is if not already compiled ).
pub fn c_function(b: &Block, name: &ObjRef) -> FunctionPtr {
    if let Some(r) = b.db.get_function(name) {
        let (compiled, src) = { (r.compiled.get(), r.source.clone()) };
        if !compiled {
            r.compiled.set(true);
            let mut p = Parser::new(&src, &b.db);
            p.function_name = Some(name);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                p.parse_function();
            }));
            if let Err(x) = result {
                r.compiled.set(false);
                std::panic::panic_any(if let Some(e) = x.downcast_ref::<SqlError>() {
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
                });
            }
            *r.ilist.borrow_mut() = p.b.ilist;
        }
        r
    } else {
        panic!("function {} not found", name.str())
    }
}
/// Lookup the column offset and DataType of a named column.
pub fn name_to_col(b: &Block, name: &str) -> (usize, DataType) {
    if let Some(CTableExpression::Base(t)) = &b.from {
        let info = &t.info;
        if let Some(num) = info.get(name) {
            let colnum = *num;
            if colnum == usize::MAX {
                return (0, BIGINT);
            }
            return (info.off[colnum], info.typ[colnum]);
        }
    }
    panic!("Name '{}' not found", name)
}
/// Lookup the column number and DataType of a named column.
pub fn name_to_colnum(b: &Block, name: &str) -> (usize, DataType) {
    if let Some(CTableExpression::Base(t)) = &b.from {
        let info = &t.info;
        if let Some(num) = info.get(name) {
            let colnum = *num;
            if colnum == usize::MAX {
                return (colnum, BIGINT);
            }
            return (colnum, info.typ[colnum]);
        }
    }
    panic!("Name '{}' not found", name)
}
/// Compile ExprCall to CExpPtr<Value>, checking parameter types.
pub fn c_call(b: &Block, name: &ObjRef, parms: &mut Vec<Expr>) -> CExpPtr<Value> {
    let fp = c_function(b, name);
    let mut pv = Vec::new();
    let mut pk = Vec::new();
    for e in parms {
        pk.push(b.kind(e));
        let ce = c_value(b, e);
        pv.push(ce);
    }
    b.check_types(&fp, &pk);
    Box::new(cexp::Call { fp, pv })
}
/// Generate code to evaluate expression and push the value onto the stack.
pub fn push(b: &mut Block, e: &mut Expr) -> DataKind {
    if b.parse_only {
        return DataKind::None;
    }
    let k = b.kind(e);
    match &mut e.exp {
        ExprIs::Const(x) => {
            b.add(PushConst((*x).clone()));
        }
        ExprIs::Binary(_, _, _) => match k {
            DataKind::Int => {
                let ce = c_int(b, e);
                b.add(PushInt(ce));
            }
            DataKind::Float => {
                let ce = c_float(b, e);
                b.add(PushFloat(ce));
            }
            DataKind::Bool => {
                let ce = c_bool(b, e);
                b.add(PushBool(ce));
            }
            _ => {
                let ce = c_value(b, e);
                b.add(PushValue(ce));
            }
        },
        ExprIs::FuncCall(name, parms) => {
            let rp = c_function(b, name);
            {
                for e in parms.iter_mut() {
                    push(b, e);
                }
            }
            b.add(Call(rp));
        }
        ExprIs::Local(x) => {
            b.add(PushLocal(*x));
        }
        _ => {
            let ce = c_value(b, e);
            b.add(PushValue(ce));
        }
    }
    k
}

/// Compile FOR statement.
pub fn c_for(b: &mut Block, se: SelectExpression, start_id: usize, break_id: usize, for_id: usize) {
    let mut cse = c_select(b, se);
    let orderbylen = cse.orderby.len();
    if orderbylen == 0 {
        b.add(ForInit(for_id, Box::new(cse.from.unwrap())));
        b.set_jump(start_id);
        let info = Box::new(ForNextInfo {
            for_id,
            assigns: cse.assigns,
            exps: cse.exps,
            wher: cse.wher,
        });
        b.add(ForNext(break_id, info));
    } else {
        let assigns = mem::take(&mut cse.assigns);
        b.add(ForSortInit(for_id, Box::new(cse)));
        b.set_jump(start_id);
        let info = Box::new((for_id, orderbylen, assigns));
        b.add(ForSortNext(break_id, info));
    }
}
