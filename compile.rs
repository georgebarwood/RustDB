use crate::*;
use std::{mem, ops};
use Instruction::*;

/// Compiled expression which yields type T when evaluated.
pub trait CExp<T> {
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> T;
}
/// Pointer to CExp.
pub type CExpPtr<T> = Box<dyn CExp<T>>;
/// Function that compiles a builtin function call ( see Database::register ).
#[derive(Clone, Copy)]
pub enum CompileFunc {
    Value(fn(&Parser, &mut [Expr]) -> CExpPtr<Value>),
    Int(fn(&Parser, &mut [Expr]) -> CExpPtr<i64>),
    Float(fn(&Parser, &mut [Expr]) -> CExpPtr<f64>),
}
/// Calculate various attributes such as data_type, is_constant etc.
fn check(p: &Parser, e: &mut Expr) {
    if e.checked {
        return;
    }
    e.is_constant = true;
    match &mut e.exp {
        ExprIs::BuiltinCall(name, args) => {
            if let Some((dk, _cf)) = p.db.builtins.borrow().get(name) {
                e.data_type = *dk as DataType;
                for pe in args {
                    check(p, pe);
                    if !pe.is_constant {
                        e.is_constant = false;
                    }
                }
            } else {
                panic!("Unknown function {}", name);
            }
        }
        ExprIs::Binary(op, b1, b2) => {
            check(p, b1);
            check(p, b2);
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
            e.data_type = p.b.local_typ[*x];
        }
        ExprIs::Const(x) => {
            e.data_type = match *x {
                Value::Bool(_) => BOOL,
                Value::Int(_) => BIGINT,
                Value::Float(_) => DOUBLE,
                Value::String(_) => STRING,
                Value::Binary(_) => BINARY,
                _ => NONE,
            }
        }
        ExprIs::Case(x, els) => {
            check(p, els);
            if !els.is_constant {
                e.is_constant = false;
            }
            e.data_type = els.data_type;
            for (w, t) in x {
                check(p, w);
                if !w.is_constant {
                    e.is_constant = false;
                }
                check(p, t);
                if !t.is_constant {
                    e.is_constant = false;
                }
                if data_kind(e.data_type) != data_kind(t.data_type) {
                    panic!("CASE branch type mismatch");
                }
            }
        }
        ExprIs::Not(x) => {
            check(p, x);
            e.is_constant = x.is_constant;
            e.data_type = BOOL;
        }
        ExprIs::Minus(x) => {
            check(p, x);
            e.is_constant = x.is_constant;
            e.data_type = x.data_type;
        }
        ExprIs::FuncCall(name, parms) => {
            let f = function_look(p, name);
            e.data_type = f.return_type;
            if parms.len() != f.param_count {
                panic!(
                    "function parameter count mismatch expected {} got {}",
                    f.param_count,
                    parms.len()
                );
            }
            for (i, a) in parms.iter_mut().enumerate() {
                check(p, a);
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
            let (col, data_type) = name_to_colnum(p, x);
            e.col = col;
            e.data_type = data_type;
        }
        _ => panic!(),
    }
    e.checked = true;
}
/// Get DataType of an expression.
fn get_type(p: &Parser, e: &mut Expr) -> DataType {
    check(p, e);
    e.data_type
}
/// Get DataKind of an expression.
pub fn get_kind(p: &Parser, e: &mut Expr) -> DataKind {
    check(p, e);
    data_kind(e.data_type)
}
/// Compile a call to a builtin function that returns a Value.
fn c_builtin_value(p: &Parser, name: &str, args: &mut [Expr]) -> CExpPtr<Value> {
    if let Some((_dk, CompileFunc::Value(cf))) = p.db.builtins.borrow().get(name) {
        return cf(p, args);
    }
    panic!()
}
/// Compile an expression.
pub fn c_value(p: &Parser, e: &mut Expr) -> CExpPtr<Value> {
    match get_kind(p, e) {
        DataKind::Bool => Box::new(cexp::BoolToVal { ce: c_bool(p, e) }),
        DataKind::Int => Box::new(cexp::IntToVal { ce: c_int(p, e) }),
        DataKind::Float => Box::new(cexp::FloatToVal { ce: c_float(p, e) }),
        _ => match &mut e.exp {
            ExprIs::ColName(x) => {
                let (off, typ) = name_to_col(p, x);
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
                let c1 = c_value(p, b1);
                let c2 = c_value(p, b2);
                match op {
                    Token::VBar => Box::new(cexp::Concat { c1, c2 }),
                    _ => panic!(),
                }
            }
            ExprIs::FuncCall(name, parms) => c_call(p, name, parms),
            ExprIs::Case(list, els) => c_case(p, list, els, c_value),
            ExprIs::BuiltinCall(name, parms) => c_builtin_value(p, name, parms),
            _ => panic!(),
        },
    }
}
/// Compile int expression.
pub fn c_int(p: &Parser, e: &mut Expr) -> CExpPtr<i64> {
    if get_kind(p, e) != DataKind::Int {
        panic!("Integer type expected")
    }
    match &mut e.exp {
        ExprIs::ColName(x) => {
            let (off, typ) = name_to_col(p, x);
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
        ExprIs::Binary(op, b1, b2) => c_arithmetic(p, *op, b1, b2, c_int),
        ExprIs::Minus(x) => Box::new(cexp::Minus::<i64> { ce: c_int(p, x) }),
        ExprIs::Case(w, e) => c_case(p, w, e, c_int),
        ExprIs::FuncCall(n, a) => Box::new(cexp::ValToInt {
            ce: c_call(p, n, a),
        }),
        ExprIs::BuiltinCall(n, a) => c_builtin_int(p, n, a),
        _ => panic!(),
    }
}
/// Compile float expression.
pub fn c_float(p: &Parser, e: &mut Expr) -> CExpPtr<f64> {
    if get_kind(p, e) != DataKind::Float {
        panic!("Float type expected")
    }
    match &mut e.exp {
        ExprIs::ColName(x) => {
            let (off, typ) = name_to_col(p, x);
            match data_size(typ) {
                8 => Box::new(cexp::ColumnF64 { off }),
                4 => Box::new(cexp::ColumnF32 { off }),
                _ => panic!(),
            }
        }
        ExprIs::Local(num) => Box::new(cexp::Local { num: *num }),
        ExprIs::Binary(op, b1, b2) => c_arithmetic(p, *op, b1, b2, c_float),
        ExprIs::Minus(x) => Box::new(cexp::Minus::<f64> { ce: c_float(p, x) }),
        ExprIs::Case(w, e) => c_case(p, w, e, c_float),
        ExprIs::FuncCall(n, a) => Box::new(cexp::ValToFloat {
            ce: c_call(p, n, a),
        }),
        ExprIs::BuiltinCall(n, a) => c_builtin_float(p, n, a),
        _ => panic!(),
    }
}
/// Compile bool expression.
pub fn c_bool(p: &Parser, e: &mut Expr) -> CExpPtr<bool> {
    if get_kind(p, e) != DataKind::Bool {
        panic!("Bool type expected")
    }
    match &mut e.exp {
        ExprIs::ColName(x) => {
            let (off, _typ) = name_to_col(p, x);
            Box::new(cexp::ColumnBool { off })
        }
        ExprIs::Const(Value::Bool(b)) => Box::new(cexp::Const::<bool> { value: *b }),
        ExprIs::Local(x) => Box::new(cexp::Local { num: *x }),
        ExprIs::Binary(op, b1, b2) => {
            if *op == Token::Or || *op == Token::And {
                let c1 = c_bool(p, b1);
                let c2 = c_bool(p, b2);
                match op {
                    Token::Or => Box::new(cexp::Or { c1, c2 }),
                    Token::And => Box::new(cexp::And { c1, c2 }),
                    _ => panic!(),
                }
            } else {
                match get_kind(p, b1) {
                    DataKind::Bool => c_compare(p, *op, b1, b2, c_bool),
                    DataKind::Int => c_compare(p, *op, b1, b2, c_int),
                    DataKind::Float => c_compare(p, *op, b1, b2, c_float),
                    _ => c_compare(p, *op, b1, b2, c_value),
                }
            }
        }
        ExprIs::Not(x) => Box::new(cexp::Not { ce: c_bool(p, x) }),
        ExprIs::FuncCall(name, parms) => Box::new(cexp::ValToBool {
            ce: c_call(p, name, parms),
        }),
        ExprIs::Case(list, els) => c_case(p, list, els, c_bool),
        _ => panic!(),
    }
}
/// Compile arithmetic.
fn c_arithmetic<T>(
    p: &Parser,
    op: Token,
    e1: &mut Expr,
    e2: &mut Expr,
    cexp: fn(&Parser, &mut Expr) -> CExpPtr<T>,
) -> CExpPtr<T>
where
    T: 'static
        + ops::Add<Output = T>
        + ops::Sub<Output = T>
        + ops::Mul<Output = T>
        + ops::Div<Output = T>
        + ops::Rem<Output = T>,
{
    let c1 = cexp(p, e1);
    let c2 = cexp(p, e2);
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
    p: &Parser,
    op: Token,
    e1: &mut Expr,
    e2: &mut Expr,
    cexp: fn(&Parser, &mut Expr) -> CExpPtr<T>,
) -> CExpPtr<bool>
where
    T: 'static + std::cmp::PartialOrd,
{
    let c1 = cexp(p, e1);
    let c2 = cexp(p, e2);
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
    p: &Parser,
    wes: &mut [(Expr, Expr)],
    els: &mut Expr,
    cexp: fn(&Parser, &mut Expr) -> CExpPtr<T>,
) -> CExpPtr<T>
where
    T: 'static,
{
    let mut whens = Vec::new();
    for (be, ve) in wes {
        let b = c_bool(p, be);
        let v = cexp(p, ve);
        whens.push((b, v));
    }
    let els = cexp(p, els);
    Box::new(cexp::Case::<T> { whens, els })
}
/// Compile a call to a builtin function that returns an integer.
fn c_builtin_int(p: &Parser, name: &str, args: &mut [Expr]) -> CExpPtr<i64> {
    if let Some((_dk, CompileFunc::Int(cf))) = p.db.builtins.borrow().get(name) {
        return cf(p, args);
    }
    panic!()
}
/// Compile a call to a builtin function that returns a float.
fn c_builtin_float(p: &Parser, name: &str, args: &mut [Expr]) -> CExpPtr<f64> {
    if let Some((_dk, CompileFunc::Float(cf))) = p.db.builtins.borrow().get(name) {
        return cf(p, args);
    }
    panic!()
}

/// Compile UPDATE statement.
pub(crate) fn c_update(
    p: &mut Parser,
    tname: &ObjRef,
    assigns: &mut Vec<(String, Expr)>,
    wher: &mut Option<Expr>,
) {
    let t = table_look(p, tname);
    let from = CTableExpression::Base(t.clone());
    let save = mem::replace(&mut p.from, Some(from));
    let mut se = Vec::new();
    for (name, exp) in assigns.iter_mut() {
        if let Some(cnum) = t.info.colmap.get(name) {
            let exp = c_value(p, exp);
            se.push((*cnum, exp));
        } else {
            panic!("update column name not found");
        }
    }
    let (w, index_from) = c_where(p, Some(t), wher);
    let mut from = mem::replace(&mut p.from, save);
    if index_from.is_some() {
        from = index_from;
    }
    p.dop(DO::Update(se, from.unwrap(), w));
}

/// Complete DELETE statement.
pub(crate) fn c_delete(p: &mut Parser, tname: &ObjRef, wher: &mut Option<Expr>) {
    let t = table_look(p, tname);
    let from = Some(CTableExpression::Base(t.clone()));
    let save = mem::replace(&mut p.from, from);
    let (w, index_from) = c_where(p, Some(t), wher);
    let mut from = mem::replace(&mut p.from, save);
    if index_from.is_some() {
        from = index_from;
    }
    p.dop(DO::Delete(from.unwrap(), w));
}

/// Compile SelectExpression to CSelectExpression.
pub(crate) fn c_select(p: &mut Parser, mut x: SelectExpression) -> CSelectExpression {
    let mut from = x.from.map(|mut te| c_te(p, &mut te));
    let table = match &from {
        Some(CTableExpression::Base(t)) => Some(t.clone()),
        _ => None,
    };
    // Is the save necessary?
    let save = mem::replace(&mut p.from, from);
    let mut exps = Vec::new();
    for (i, e) in x.exps.iter_mut().enumerate() {
        exps.push(c_value(p, e));
        if !x.assigns.is_empty() {
            // Check data kind of assigned local matches data kind of expression.
            let (lnum, _) = x.assigns[i];
            let ek = data_kind(p.b.local_typ[lnum]);
            let ak = data_kind(e.data_type);
            if ek != ak {
                panic!("cannot assign {:?} to {:?}", ak, ek);
            }
        }
    }
    let (wher, index_from) = c_where(p, table, &mut x.wher);
    let mut orderby = Vec::new();
    let mut desc = Vec::new();
    for (e, a) in &mut x.orderby {
        let e = c_value(p, e);
        orderby.push(e);
        desc.push(*a);
    }
    from = mem::replace(&mut p.from, save);
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
    p: &Parser,
    table: Option<TablePtr>,
    wher: &mut Option<Expr>,
) -> (Option<CExpPtr<bool>>, Option<CTableExpression>) {
    if let Some(we) = wher {
        if get_kind(p, we) != DataKind::Bool {
            panic!("WHERE expression must be bool")
        }
        if let Some(table) = table {
            table.index_from(p, we)
        } else {
            (Some(c_bool(p, we)), None)
        }
    } else {
        (None, None)
    }
}

/// Compile a TableExpression to CTableExpression.
pub(crate) fn c_te(p: &Parser, te: &mut TableExpression) -> CTableExpression {
    match te {
        TableExpression::Values(x) => {
            let mut cm = Vec::new();
            for r in x {
                let mut cr = Vec::new();
                for e in r {
                    let ce = c_value(p, e);
                    cr.push(ce);
                }
                cm.push(cr);
            }
            CTableExpression::Values(cm)
        }
        TableExpression::Base(x) => {
            let t = table_look(p, x);
            CTableExpression::Base(t)
        }
    }
}
/// Look for named table in database.
pub(crate) fn table_look(p: &Parser, name: &ObjRef) -> TablePtr {
    if let Some(t) = p.db.get_table(name) {
        t
    } else {
        panic!("table {} not found", name.str())
    }
}
/// Look for named function in database and compile it if not already compiled.
pub(crate) fn function_look(p: &Parser, name: &ObjRef) -> FunctionPtr {
    if let Some(r) = p.db.get_function(name) {
        let (compiled, src) = { (r.compiled.get(), r.source.clone()) };
        if !compiled {
            r.compiled.set(true);
            let mut p = Parser::new(&src, &p.db);
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
pub(crate) fn name_to_col(p: &Parser, name: &str) -> (usize, DataType) {
    if let Some(CTableExpression::Base(t)) = &p.from {
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
pub(crate) fn name_to_colnum(p: &Parser, name: &str) -> (usize, DataType) {
    if let Some(CTableExpression::Base(t)) = &p.from {
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
pub(crate) fn c_call(p: &Parser, name: &ObjRef, parms: &mut Vec<Expr>) -> CExpPtr<Value> {
    let fp: FunctionPtr = function_look(p, name);
    let mut pv: Vec<CExpPtr<Value>> = Vec::new();
    let mut pt: Vec<DataType> = Vec::new();
    for e in parms {
        let t: DataType = get_type(p, e);
        pt.push(t);
        let ce = c_value(p, e);
        pv.push(ce);
    }
    p.check_types(&fp, &pt);
    Box::new(cexp::Call { fp, pv })
}
/// Generate code to evaluate expression and push the value onto the stack.
pub(crate) fn push(p: &mut Parser, e: &mut Expr) -> DataType {
    if p.parse_only {
        return NONE;
    }
    let t = get_type(p, e);
    match &mut e.exp {
        ExprIs::Const(x) => {
            p.add(PushConst((*x).clone()));
        }
        ExprIs::Binary(_, _, _) => match data_kind(t) {
            DataKind::Int => {
                let ce = c_int(p, e);
                p.add(PushInt(ce));
            }
            DataKind::Float => {
                let ce = c_float(p, e);
                p.add(PushFloat(ce));
            }
            DataKind::Bool => {
                let ce = c_bool(p, e);
                p.add(PushBool(ce));
            }
            _ => {
                let ce = c_value(p, e);
                p.add(PushValue(ce));
            }
        },
        ExprIs::FuncCall(name, parms) => {
            let rp = function_look(p, name);
            {
                for e in parms.iter_mut() {
                    push(p, e);
                }
            }
            p.add(Call(rp));
        }
        ExprIs::Local(x) => {
            p.add(PushLocal(*x));
        }
        _ => {
            let ce = c_value(p, e);
            p.add(PushValue(ce));
        }
    }
    t
}
