use crate::*;

/// Registers builtin functions - called from `Database`::new.
pub fn register_builtins(db: &DB)
{
  let list = [
    ("ARG", DataKind::String, CompileFunc::Value(c_arg)),
    ("GLOBAL", DataKind::Int, CompileFunc::Int(c_global)),
    ("REPLACE", DataKind::String, CompileFunc::Value(c_replace)),
    ("SUBSTRING", DataKind::String, CompileFunc::Value(c_substring)),
    ("LEN", DataKind::Int, CompileFunc::Int(c_len)),
    ("PARSEINT", DataKind::Int, CompileFunc::Int(c_parse_int)),
    ("PARSEFLOAT", DataKind::Float, CompileFunc::Float(c_parse_float)),
    ("PARSEDECIMAL", DataKind::Decimal, CompileFunc::Decimal(c_parse_decimal)),
    ("EXCEPTION", DataKind::String, CompileFunc::Value(c_exception)),
    ("LASTID", DataKind::Int, CompileFunc::Int(c_lastid)),
  ];

  for (name, typ, cf) in list
  {
    db.register(name, typ, cf);
  }
}

/// Check number and kinds of arguments.
fn check_types(p: &Parser, args: &mut [Expr], dk: &[DataKind])
{
  if args.len() != dk.len()
  {
    panic!("Wrong number of args");
  }
  for (i, e) in args.iter_mut().enumerate()
  {
    let k = get_kind(p, e);
    if k != dk[i]
    {
      panic!(
        "Builtin function arg {} type mismatch expected {:?} got {:?}",
        i + 1,
        dk[i],
        k
      );
    }
  }
}

/////////////////////////////

/// Compile call to EXCEPTION().
fn c_exception(p: &Parser, args: &mut [Expr]) -> CExpPtr<Value>
{
  check_types(p, args, &[]);
  Box::new(Exception {})
}

struct Exception {}

impl CExp<Value> for Exception
{
  fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> Value
  {
    let err = e.qy.get_error();
    Value::String(Rc::new(err))
  }
}

/////////////////////////////

/// Compile call to LEN.
fn c_len(p: &Parser, args: &mut [Expr]) -> CExpPtr<i64>
{
  check_types(p, args, &[DataKind::String]);
  let s = c_value(p, &mut args[0]);
  Box::new(Len { s })
}

struct Len
{
  s: CExpPtr<Value>,
}

impl CExp<i64> for Len
{
  fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64
  {
    let s = self.s.eval(e, d).str();
    s.len() as i64
  }
}

/////////////////////////////

/// Compile call to LASTID.
fn c_lastid(p: &Parser, args: &mut [Expr]) -> CExpPtr<i64>
{
  check_types(p, args, &[]);
  Box::new(LastId {})
}

struct LastId {}

impl CExp<i64> for LastId
{
  fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64
  {
    ee.db.lastid.get()
  }
}

/////////////////////////////

/// Compile call to GLOBAL.
fn c_global(p: &Parser, args: &mut [Expr]) -> CExpPtr<i64>
{
  check_types(p, args, &[DataKind::Int]);
  let x = c_int(p, &mut args[0]);
  Box::new(Global { x })
}

struct Global
{
  x: CExpPtr<i64>,
}

impl CExp<i64> for Global
{
  fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64
  {
    let x = self.x.eval(ee, d);
    ee.qy.global(x)
  }
}

/////////////////////////////

/// Compile call to PARSEINT.
fn c_parse_int(p: &Parser, args: &mut [Expr]) -> CExpPtr<i64>
{
  check_types(p, args, &[DataKind::String]);
  let s = c_value(p, &mut args[0]);
  Box::new(ParseInt { s })
}

struct ParseInt
{
  s: CExpPtr<Value>,
}

impl CExp<i64> for ParseInt
{
  fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64
  {
    let s = self.s.eval(e, d).str();
    s.parse().unwrap()
  }
}

/////////////////////////////

/// Compile call to PARSEDECIMAL.
fn c_parse_decimal(p: &Parser, args: &mut [Expr]) -> CExpPtr<i64>
{
  check_types(p, args, &[DataKind::String, DataKind::Int]);
  let s = c_value(p, &mut args[0]);
  let t = c_int(p, &mut args[1]);
  Box::new(ParseDecimal { s, t })
}

struct ParseDecimal
{
  s: CExpPtr<Value>,
  t: CExpPtr<i64>,
}

impl CExp<i64> for ParseDecimal
{
  fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64
  {
    let s = self.s.eval(e, d).str();
    let _t = self.t.eval(e, d);
    s.parse().unwrap()
  }
}

/////////////////////////////

/// Compile call to PARSEFLOAT.
fn c_parse_float(p: &Parser, args: &mut [Expr]) -> CExpPtr<f64>
{
  check_types(p, args, &[DataKind::String]);
  let s = c_value(p, &mut args[0]);
  Box::new(ParseFloat { s })
}

struct ParseFloat
{
  s: CExpPtr<Value>,
}

impl CExp<f64> for ParseFloat
{
  fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> f64
  {
    let s = self.s.eval(e, d).str();
    s.parse().unwrap()
  }
}

/////////////////////////////

/// Compile call to REPLACE.
fn c_replace(p: &Parser, args: &mut [Expr]) -> CExpPtr<Value>
{
  check_types(p, args, &[DataKind::String, DataKind::String, DataKind::String]);
  let s = c_value(p, &mut args[0]);
  let pat = c_value(p, &mut args[1]);
  let sub = c_value(p, &mut args[2]);
  Box::new(Replace { s, pat, sub })
}

struct Replace
{
  s: CExpPtr<Value>,
  pat: CExpPtr<Value>,
  sub: CExpPtr<Value>,
}

impl CExp<Value> for Replace
{
  fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value
  {
    let s = self.s.eval(e, d).str().to_string();
    let pat = self.pat.eval(e, d).str().to_string();
    let sub = self.sub.eval(e, d).str();
    let result = s.replace(&pat, &sub);
    Value::String(Rc::new(result))
  }
}

/////////////////////////////

/// Compile call to SUBSTRING.
fn c_substring(p: &Parser, args: &mut [Expr]) -> CExpPtr<Value>
{
  check_types(p, args, &[DataKind::String, DataKind::Int, DataKind::Int]);
  let s = c_value(p, &mut args[0]);
  let f = c_int(p, &mut args[1]);
  let n = c_int(p, &mut args[2]);
  Box::new(Substring { s, f, n })
}

struct Substring
{
  s: CExpPtr<Value>,
  f: CExpPtr<i64>,
  n: CExpPtr<i64>,
}

impl CExp<Value> for Substring
{
  fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value
  {
    let s = self.s.eval(ee, d).str();
    let f = self.f.eval(ee, d) as usize - 1;
    let n = self.n.eval(ee, d) as usize;
    let mut lim = s.len();
    if lim > f + n
    {
      lim = f + n;
    }
    let result = s[f..lim].to_string();
    Value::String(Rc::new(result))
  }
}

/////////////////////////////

/// Compile call to ARG.
fn c_arg(p: &Parser, args: &mut [Expr]) -> CExpPtr<Value>
{
  check_types(p, args, &[DataKind::Int, DataKind::String]);
  let k = c_int(p, &mut args[0]);
  let s = c_value(p, &mut args[1]);
  Box::new(Arg { k, s })
}

struct Arg
{
  k: CExpPtr<i64>,
  s: CExpPtr<Value>,
}

impl CExp<Value> for Arg
{
  fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value
  {
    let k = self.k.eval(ee, d);
    let s = self.s.eval(ee, d).str();
    let result = ee.qy.arg(k, &s);
    Value::String(result)
  }
}
