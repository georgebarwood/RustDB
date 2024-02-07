use crate::{get_bytes, util, CExp, CExpPtr, EvalEnv, Function, Rc, Value};

/// Function call.
pub(crate) struct Call {
    pub fp: Rc<Function>,
    pub pv: Vec<CExpPtr<Value>>,
}

impl CExp<Value> for Call {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        for exp in &self.pv {
            let v = exp.eval(e, d);
            e.stack.push(v);
        }
        e.call(&self.fp);
        e.stack.pop().unwrap()
    }
}

pub(crate) struct Case<T> {
    pub whens: Vec<(CExpPtr<bool>, CExpPtr<T>)>,
    pub els: CExpPtr<T>,
}

impl<T> CExp<T> for Case<T> {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        for (b, v) in &self.whens {
            if b.eval(e, d) {
                return v.eval(e, d);
            }
        }
        self.els.eval(e, d)
    }
}

pub(crate) struct Concat(pub CExpPtr<Value>, pub CExpPtr<Value>);

impl CExp<Value> for Concat {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        let mut s1: Value = self.0.eval(e, d);
        let s2: Rc<String> = self.1.eval(e, d).str();
        // Append to existing string if not shared.
        if let Value::String(s) = &mut s1 {
            if let Some(ms) = Rc::get_mut(s) {
                ms.push_str(&s2);
                return s1;
            }
        }
        let s1 = s1.str();
        let mut s = String::with_capacity(s1.len() + s2.len());
        s.push_str(&s1);
        s.push_str(&s2);
        Value::String(Rc::new(s))
    }
}

pub(crate) struct BinConcat(pub CExpPtr<Value>, pub CExpPtr<Value>);

impl CExp<Value> for BinConcat {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        let mut b1 = self.0.eval(e, d);
        let b2 = self.1.eval(e, d).bin();
        // Append to existing bytes if not shared.
        if let Value::RcBinary(b) = &mut b1 {
            if let Some(mb) = Rc::get_mut(b) {
                mb.extend_from_slice(&b2);
                return b1;
            }
        }
        let b1 = b1.bin();
        let mut b = Vec::with_capacity(b1.len() + b2.len());
        b.extend_from_slice(&b1);
        b.extend_from_slice(&b2);
        Value::RcBinary(Rc::new(b))
    }
}

pub(crate) struct Or(pub CExpPtr<bool>, pub CExpPtr<bool>);

impl CExp<bool> for Or {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) || self.1.eval(e, d)
    }
}

pub(crate) struct And(pub CExpPtr<bool>, pub CExpPtr<bool>);

impl CExp<bool> for And {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) && self.1.eval(e, d)
    }
}

pub(crate) struct Minus<T>(pub CExpPtr<T>);

impl<T> CExp<T> for Minus<T>
where
    T: std::ops::Neg<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        -self.0.eval(e, d)
    }
}

pub(crate) struct Not(pub CExpPtr<bool>);

impl CExp<bool> for Not {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        !self.0.eval(e, d)
    }
}

pub(crate) struct Add<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<T> for Add<T>
where
    T: std::ops::Add<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.0.eval(e, d) + self.1.eval(e, d)
    }
}

pub(crate) struct Sub<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<T> for Sub<T>
where
    T: std::ops::Sub<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.0.eval(e, d) - self.1.eval(e, d)
    }
}

pub(crate) struct Mul<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<T> for Mul<T>
where
    T: std::ops::Mul<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.0.eval(e, d) * self.1.eval(e, d)
    }
}

pub(crate) struct Div<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<T> for Div<T>
where
    T: std::ops::Div<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.0.eval(e, d) / self.1.eval(e, d)
    }
}

pub(crate) struct Rem<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<T> for Rem<T>
where
    T: std::ops::Rem<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.0.eval(e, d) % self.1.eval(e, d)
    }
}

pub(crate) struct Equal<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<bool> for Equal<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) == self.1.eval(e, d)
    }
}

pub(crate) struct NotEqual<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<bool> for NotEqual<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) != self.1.eval(e, d)
    }
}

pub(crate) struct Less<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<bool> for Less<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) < self.1.eval(e, d)
    }
}

pub(crate) struct Greater<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<bool> for Greater<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) > self.1.eval(e, d)
    }
}

pub(crate) struct LessEqual<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<bool> for LessEqual<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) <= self.1.eval(e, d)
    }
}

pub(crate) struct GreaterEqual<T>(pub CExpPtr<T>, pub CExpPtr<T>);

impl<T> CExp<bool> for GreaterEqual<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.0.eval(e, d) >= self.1.eval(e, d)
    }
}

pub(crate) struct ColumnI64 {
    pub off: usize,
}

impl CExp<i64> for ColumnI64 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        util::getu64(data, self.off) as i64
    }
}

pub(crate) struct ColumnI {
    pub off: usize,
    pub size: usize,
}

impl CExp<i64> for ColumnI {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        util::iget(data, self.off, self.size)
    }
}

pub(crate) struct ColumnI8 {
    pub off: usize,
}

impl CExp<i64> for ColumnI8 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        data[self.off] as i8 as i64
    }
}

pub(crate) struct ColumnF64 {
    pub off: usize,
}

impl CExp<f64> for ColumnF64 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> f64 {
        util::getf64(data, self.off)
    }
}

pub(crate) struct ColumnF32 {
    pub off: usize,
}

impl CExp<f64> for ColumnF32 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> f64 {
        util::getf32(data, self.off) as f64
    }
}

pub(crate) struct ColumnBool {
    pub off: usize,
}

impl CExp<bool> for ColumnBool {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> bool {
        data[self.off] & 1 != 0
    }
}

pub(crate) struct ColumnString {
    pub off: usize,
    pub size: usize,
}

impl CExp<Value> for ColumnString {
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> Value {
        let bytes = get_bytes(&ee.db, &data[self.off..], self.size).0;
        let str = String::from_utf8(bytes).unwrap();
        Value::String(Rc::new(str))
    }
}

pub(crate) struct ColumnBinary {
    pub off: usize,
    pub size: usize,
}

impl CExp<Value> for ColumnBinary {
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> Value {
        let bytes = get_bytes(&ee.db, &data[self.off..], self.size).0;
        Value::RcBinary(Rc::new(bytes))
    }
}

pub(crate) struct Local(pub usize);

impl CExp<f64> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> f64 {
        if let Value::Float(v) = e.stack[e.bp + self.0] {
            v
        } else {
            unsafe_panic!()
        }
    }
}
impl CExp<i64> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> i64 {
        if let Value::Int(v) = e.stack[e.bp + self.0] {
            v
        } else {
            unsafe_panic!()
        }
    }
}

impl CExp<bool> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> bool {
        if let Value::Bool(v) = e.stack[e.bp + self.0] {
            v
        } else {
            unsafe_panic!()
        }
    }
}

impl CExp<Value> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> Value {
        e.stack[e.bp + self.0].clone()
    }
}

pub(crate) struct Const<T>(pub T);

impl<T> CExp<T> for Const<T>
where
    T: Clone,
{
    fn eval(&self, _e: &mut EvalEnv, _d: &[u8]) -> T {
        self.0.clone()
    }
}
pub(crate) struct ValToInt(pub CExpPtr<Value>);

impl CExp<i64> for ValToInt {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64 {
        if let Value::Int(x) = self.0.eval(e, d) {
            return x;
        }
        unsafe_panic!();
    }
}

pub(crate) struct ValToFloat(pub CExpPtr<Value>);

impl CExp<f64> for ValToFloat {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> f64 {
        if let Value::Float(x) = self.0.eval(e, d) {
            return x;
        }
        unsafe_panic!();
    }
}

pub(crate) struct ValToBool(pub CExpPtr<Value>);

impl CExp<bool> for ValToBool {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        if let Value::Bool(x) = self.0.eval(e, d) {
            return x;
        }
        unsafe_panic!();
    }
}

pub(crate) struct IntToVal(pub CExpPtr<i64>);

impl CExp<Value> for IntToVal {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        Value::Int(self.0.eval(e, d))
    }
}

pub(crate) struct FloatToVal(pub CExpPtr<f64>);

impl CExp<Value> for FloatToVal {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        Value::Float(self.0.eval(e, d))
    }
}

pub(crate) struct BoolToVal(pub CExpPtr<bool>);

impl CExp<Value> for BoolToVal {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        Value::Bool(self.0.eval(e, d))
    }
}
