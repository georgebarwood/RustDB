use crate::{get_bytes, panic, util, CExp, CExpPtr, EvalEnv, FunctionPtr, Rc, Value};

pub struct Call {
    pub fp: FunctionPtr,
    pub pv: Vec<CExpPtr<Value>>,
}
impl CExp<Value> for Call {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        for exp in &self.pv {
            let v = exp.eval(e, d);
            e.stack.push(v);
        }
        e.call(&*self.fp);
        e.stack.pop().unwrap()
    }
}
pub struct Case<T> {
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
pub struct Concat {
    pub c1: CExpPtr<Value>,
    pub c2: CExpPtr<Value>,
}
impl CExp<Value> for Concat {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        let mut s1: Value = self.c1.eval(e, d);
        let s2: Rc<String> = self.c2.eval(e, d).str();
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
pub struct Or {
    pub c1: CExpPtr<bool>,
    pub c2: CExpPtr<bool>,
}
impl CExp<bool> for Or {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) || self.c2.eval(e, d)
    }
}
pub struct And {
    pub c1: CExpPtr<bool>,
    pub c2: CExpPtr<bool>,
}
impl CExp<bool> for And {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) && self.c2.eval(e, d)
    }
}
pub struct Add<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<T> for Add<T>
where
    T: std::ops::Add<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.c1.eval(e, d) + self.c2.eval(e, d)
    }
}
pub struct Sub<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<T> for Sub<T>
where
    T: std::ops::Sub<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.c1.eval(e, d) - self.c2.eval(e, d)
    }
}
pub struct Minus<T> {
    pub ce: CExpPtr<T>,
}
impl<T> CExp<T> for Minus<T>
where
    T: std::ops::Neg<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        -self.ce.eval(e, d)
    }
}
pub struct Not {
    pub ce: CExpPtr<bool>,
}
impl CExp<bool> for Not {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        !self.ce.eval(e, d)
    }
}
pub struct Mul<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<T> for Mul<T>
where
    T: std::ops::Mul<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.c1.eval(e, d) * self.c2.eval(e, d)
    }
}
pub struct Div<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<T> for Div<T>
where
    T: std::ops::Div<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.c1.eval(e, d) / self.c2.eval(e, d)
    }
}
pub struct Rem<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<T> for Rem<T>
where
    T: std::ops::Rem<Output = T>,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> T {
        self.c1.eval(e, d) % self.c2.eval(e, d)
    }
}
pub struct Equal<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<bool> for Equal<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) == self.c2.eval(e, d)
    }
}
pub struct NotEqual<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<bool> for NotEqual<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) != self.c2.eval(e, d)
    }
}
pub struct Less<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<bool> for Less<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) < self.c2.eval(e, d)
    }
}
pub struct Greater<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<bool> for Greater<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) > self.c2.eval(e, d)
    }
}
pub struct LessEqual<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<bool> for LessEqual<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) <= self.c2.eval(e, d)
    }
}
pub struct GreaterEqual<T> {
    pub c1: CExpPtr<T>,
    pub c2: CExpPtr<T>,
}
impl<T> CExp<bool> for GreaterEqual<T>
where
    T: std::cmp::PartialOrd,
{
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        self.c1.eval(e, d) >= self.c2.eval(e, d)
    }
}
pub struct ColumnI64 {
    pub off: usize,
}
impl CExp<i64> for ColumnI64 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        util::getu64(data, self.off) as i64
    }
}
pub struct ColumnI32 {
    pub off: usize,
}
impl CExp<i64> for ColumnI32 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        util::get(data, self.off, 4) as i32 as i64
    }
}
pub struct ColumnI16 {
    pub off: usize,
}
impl CExp<i64> for ColumnI16 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        util::get(data, self.off, 2) as i16 as i64
    }
}
pub struct ColumnI8 {
    pub off: usize,
}
impl CExp<i64> for ColumnI8 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> i64 {
        data[self.off] as i8 as i64
    }
}
pub struct ColumnF64 {
    pub off: usize,
}
impl CExp<f64> for ColumnF64 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> f64 {
        util::getf64(data, self.off) as f64
    }
}
pub struct ColumnF32 {
    pub off: usize,
}
impl CExp<f64> for ColumnF32 {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> f64 {
        util::getf32(data, self.off) as f64
    }
}
pub struct ColumnBool {
    pub off: usize,
}
impl CExp<bool> for ColumnBool {
    fn eval(&self, _e: &mut EvalEnv, data: &[u8]) -> bool {
        data[self.off] & 1 != 0
    }
}
pub struct ColumnString {
    pub off: usize,
}
impl CExp<Value> for ColumnString {
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> Value {
        let bytes = get_bytes(&ee.db, &data[self.off..]).0;
        let str = String::from_utf8(bytes).unwrap();
        Value::String(Rc::new(str))
    }
}
pub struct ColumnBinary {
    pub off: usize,
}
impl CExp<Value> for ColumnBinary {
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> Value {
        let bytes = get_bytes(&ee.db, &data[self.off..]).0;
        Value::Binary(Rc::new(bytes))
    }
}
pub struct Local {
    pub num: usize,
}
impl CExp<f64> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> f64 {
        if let Value::Float(v) = e.stack[e.bp + self.num] {
            v
        } else {
            panic!()
        }
    }
}
impl CExp<i64> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> i64 {
        if let Value::Int(v) = e.stack[e.bp + self.num] {
            v
        } else {
            panic!()
        }
    }
}
impl CExp<bool> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> bool {
        if let Value::Bool(v) = e.stack[e.bp + self.num] {
            v
        } else {
            panic!()
        }
    }
}
impl CExp<Value> for Local {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> Value {
        e.stack[e.bp + self.num].clone()
    }
}
pub struct Const<T> {
    pub value: T,
}
impl<T> CExp<T> for Const<T>
where
    T: Clone,
{
    fn eval(&self, _e: &mut EvalEnv, _d: &[u8]) -> T {
        self.value.clone()
    }
}
pub struct ValToInt {
    pub ce: CExpPtr<Value>,
}
impl CExp<i64> for ValToInt {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64 {
        if let Value::Int(x) = self.ce.eval(e, d) {
            return x;
        }
        panic!();
    }
}
pub struct ValToFloat {
    pub ce: CExpPtr<Value>,
}
impl CExp<f64> for ValToFloat {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> f64 {
        if let Value::Float(x) = self.ce.eval(e, d) {
            return x;
        }
        panic!();
    }
}
pub struct ValToBool {
    pub ce: CExpPtr<Value>,
}
impl CExp<bool> for ValToBool {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> bool {
        if let Value::Bool(x) = self.ce.eval(e, d) {
            return x;
        }
        panic!();
    }
}
pub struct IntToVal {
    pub ce: CExpPtr<i64>,
}
impl CExp<Value> for IntToVal {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        Value::Int(self.ce.eval(e, d))
    }
}
pub struct FloatToVal {
    pub ce: CExpPtr<f64>,
}
impl CExp<Value> for FloatToVal {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        Value::Float(self.ce.eval(e, d))
    }
}
pub struct BoolToVal {
    pub ce: CExpPtr<bool>,
}
impl CExp<Value> for BoolToVal {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        Value::Bool(self.ce.eval(e, d))
    }
}
