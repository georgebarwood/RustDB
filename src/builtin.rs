use crate::{
    c_int, c_value, Block, BuiltinMap, CExp, CExpPtr, CompileFunc, DataKind, EvalEnv, Expr, Rc,
    Value,
};

/// Add builtin functions to specified [BuiltinMap].
pub fn standard_builtins(map: &mut BuiltinMap) {
    let list = [
        ("ARG", DataKind::String, CompileFunc::Value(c_arg)),
        ("HEADER", DataKind::Int, CompileFunc::Int(c_header)),
        ("STATUSCODE", DataKind::Int, CompileFunc::Int(c_status_code)),
        ("FILEATTR", DataKind::String, CompileFunc::Value(c_fileattr)),
        (
            "FILECONTENT",
            DataKind::Binary,
            CompileFunc::Value(c_filecontent),
        ),
        ("GLOBAL", DataKind::Int, CompileFunc::Int(c_global)),
        ("CONTAINS", DataKind::Int, CompileFunc::Int(c_contains)),
        ("REPLACE", DataKind::String, CompileFunc::Value(c_replace)),
        (
            "SUBSTRING",
            DataKind::String,
            CompileFunc::Value(c_substring),
        ),
        (
            "BINSUBSTRING",
            DataKind::Binary,
            CompileFunc::Value(c_binsubstring),
        ),
        ("LEN", DataKind::Int, CompileFunc::Int(c_len)),
        ("BINLEN", DataKind::Int, CompileFunc::Int(c_bin_len)),
        ("PARSEINT", DataKind::Int, CompileFunc::Int(c_parse_int)),
        (
            "PARSEFLOAT",
            DataKind::Float,
            CompileFunc::Float(c_parse_float),
        ),
        (
            "EXCEPTION",
            DataKind::String,
            CompileFunc::Value(c_exception),
        ),
        ("LASTID", DataKind::Int, CompileFunc::Int(c_lastid)),
        ("ALLOCPAGE", DataKind::Int, CompileFunc::Int(c_allocpage)),
        #[cfg(feature = "pack")]
        ("REPACKFILE", DataKind::Int, CompileFunc::Int(c_repackfile)),
        #[cfg(feature = "verify")]
        ("VERIFYDB", DataKind::String, CompileFunc::Value(c_verifydb)),
        #[cfg(feature = "renumber")]
        ("RENUMBER", DataKind::Int, CompileFunc::Int(c_renumber)),
        ("BINTOSTR", DataKind::String, CompileFunc::Value(c_bintostr)),
    ];
    for (name, typ, cf) in list {
        map.insert(name.to_string(), (typ, cf));
    }
}
/// Check number and kinds of arguments.
pub fn check_types(b: &Block, args: &mut [Expr], dk: &[DataKind]) {
    if args.len() != dk.len() {
        panic!("wrong number of args");
    }
    for (i, e) in args.iter_mut().enumerate() {
        let k = b.kind(e);
        if k != dk[i] {
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
fn c_exception(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[]);
    Box::new(Exception {})
}
struct Exception {}
impl CExp<Value> for Exception {
    fn eval(&self, e: &mut EvalEnv, _d: &[u8]) -> Value {
        let err = e.tr.get_error();
        Value::String(Rc::new(err))
    }
}
/////////////////////////////
/// Compile call to LEN.
fn c_len(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::String]);
    let s = c_value(b, &mut args[0]);
    Box::new(Len { s })
}
struct Len {
    s: CExpPtr<Value>,
}
impl CExp<i64> for Len {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64 {
        let s = self.s.eval(e, d).str();
        s.len() as i64
    }
}
/////////////////////////////
/// Compile call to BINLEN.
fn c_bin_len(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::Binary]);
    let bv = c_value(b, &mut args[0]);
    Box::new(BinLen { bv })
}
struct BinLen {
    bv: CExpPtr<Value>,
}
impl CExp<i64> for BinLen {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64 {
        let x = self.bv.eval(e, d);
        x.bina().len() as i64
    }
}
/////////////////////////////
/// Compile call to LASTID.
fn c_lastid(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(LastId {})
}
struct LastId {}
impl CExp<i64> for LastId {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        ee.db.lastid.get()
    }
}
/////////////////////////////
/// Compile call to ALLOCPAGE.
fn c_allocpage(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(AllocPage {})
}
struct AllocPage {}
impl CExp<i64> for AllocPage {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        ee.db.alloc_page() as i64
    }
}
/////////////////////////////
/// Compile call to GLOBAL.
fn c_global(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::Int]);
    let x = c_int(b, &mut args[0]);
    Box::new(Global { x })
}
struct Global {
    x: CExpPtr<i64>,
}
impl CExp<i64> for Global {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let x = self.x.eval(ee, d);
        ee.tr.global(x)
    }
}
/////////////////////////////
/// Compile call to PARSEINT.
fn c_parse_int(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::String]);
    let s = c_value(b, &mut args[0]);
    Box::new(ParseInt { s })
}
struct ParseInt {
    s: CExpPtr<Value>,
}
impl CExp<i64> for ParseInt {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64 {
        let s = self.s.eval(e, d).str();
        s.parse().unwrap_or(0)
    }
}
/////////////////////////////
/// Compile call to PARSEFLOAT.
fn c_parse_float(b: &Block, args: &mut [Expr]) -> CExpPtr<f64> {
    check_types(b, args, &[DataKind::String]);
    let s = c_value(b, &mut args[0]);
    Box::new(ParseFloat { s })
}
struct ParseFloat {
    s: CExpPtr<Value>,
}
impl CExp<f64> for ParseFloat {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> f64 {
        let s = self.s.eval(e, d).str();
        s.parse().unwrap()
    }
}
/////////////////////////////
/// Compile call to CONTAINS.
fn c_contains(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::String, DataKind::String]);
    let s = c_value(b, &mut args[0]);
    let pat = c_value(b, &mut args[1]);
    Box::new(Contains { s, pat })
}
struct Contains {
    s: CExpPtr<Value>,
    pat: CExpPtr<Value>,
}
impl CExp<i64> for Contains {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> i64 {
        let s = self.s.eval(e, d).str().to_string();
        let pat = self.pat.eval(e, d).str().to_string();
        match s.find(&pat) {
            Some(u) => u as i64,
            None => -1,
        }
    }
}
/////////////////////////////
/// Compile call to REPLACE.
fn c_replace(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(
        b,
        args,
        &[DataKind::String, DataKind::String, DataKind::String],
    );
    let s = c_value(b, &mut args[0]);
    let pat = c_value(b, &mut args[1]);
    let sub = c_value(b, &mut args[2]);
    Box::new(Replace { s, pat, sub })
}
struct Replace {
    s: CExpPtr<Value>,
    pat: CExpPtr<Value>,
    sub: CExpPtr<Value>,
}
impl CExp<Value> for Replace {
    fn eval(&self, e: &mut EvalEnv, d: &[u8]) -> Value {
        let s = self.s.eval(e, d).str().to_string();
        let pat = self.pat.eval(e, d).str().to_string();
        let sub = self.sub.eval(e, d).str();
        let result = s.replace(&pat, &sub);
        Value::String(Rc::new(result))
    }
}
/////////////////////////////
/// Compile call to SUBSTRING.
fn c_substring(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::String, DataKind::Int, DataKind::Int]);
    let s = c_value(b, &mut args[0]);
    let f = c_int(b, &mut args[1]);
    let n = c_int(b, &mut args[2]);
    Box::new(Substring { s, f, n })
}
struct Substring {
    s: CExpPtr<Value>,
    f: CExpPtr<i64>,
    n: CExpPtr<i64>,
}
impl CExp<Value> for Substring {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let s = self.s.eval(ee, d).str();
        let f = self.f.eval(ee, d) as usize - 1;
        let mut n = self.n.eval(ee, d) as usize;

        let s = &s[f..];
        let mut chars = s.char_indices();
        let end;
        loop {
            if let Some((x, _)) = chars.next() {
                if n == 0 {
                    end = x;
                    break;
                }
                n -= 1;
            } else {
                end = s.len();
                break;
            }
        }
        let result = s[0..end].to_string();
        Value::String(Rc::new(result))
    }
}

/////////////////////////////
/// Compile call to BINSUBSTRING.
fn c_binsubstring(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Binary, DataKind::Int, DataKind::Int]);
    let s = c_value(b, &mut args[0]);
    let f = c_int(b, &mut args[1]);
    let n = c_int(b, &mut args[2]);
    Box::new(BinSubstring { s, f, n })
}
struct BinSubstring {
    s: CExpPtr<Value>,
    f: CExpPtr<i64>,
    n: CExpPtr<i64>,
}
impl CExp<Value> for BinSubstring {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let s = self.s.eval(ee, d).bin();
        let f = self.f.eval(ee, d) as usize - 1;
        let n = self.n.eval(ee, d) as usize;
        let mut lim = s.len();
        if lim > f + n {
            lim = f + n;
        }
        let result = s[f..lim].to_vec();
        Value::RcBinary(Rc::new(result))
    }
}

/////////////////////////////
/// Compile call to ARG.
fn c_arg(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Int, DataKind::String]);
    let k = c_int(b, &mut args[0]);
    let s = c_value(b, &mut args[1]);
    Box::new(Arg { k, s })
}
struct Arg {
    k: CExpPtr<i64>,
    s: CExpPtr<Value>,
}
impl CExp<Value> for Arg {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let k = self.k.eval(ee, d);
        let s = self.s.eval(ee, d).str();
        let result = ee.tr.arg(k, &s);
        Value::String(result)
    }
}

/////////////////////////////
/// Compile call to HEADER.
fn c_header(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::String, DataKind::String]);
    let n = c_value(b, &mut args[0]);
    let v = c_value(b, &mut args[1]);
    Box::new(Header { n, v })
}
struct Header {
    n: CExpPtr<Value>,
    v: CExpPtr<Value>,
}
impl CExp<i64> for Header {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let n = self.n.eval(ee, d).str();
        let v = self.v.eval(ee, d).str();
        ee.tr.header(&n, &v);
        0
    }
}

/////////////////////////////
/// Compile call to STATUSCODE.
fn c_status_code(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::Int]);
    let code = c_int(b, &mut args[0]);
    Box::new(StatusCode { code })
}
struct StatusCode {
    code: CExpPtr<i64>,
}
impl CExp<i64> for StatusCode {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let code = self.code.eval(ee, d);
        ee.tr.status_code(code);
        0
    }
}

/////////////////////////////
/// Compile call to FILEATTR.
fn c_fileattr(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Int, DataKind::Int]);
    let k = c_int(b, &mut args[0]);
    let x = c_int(b, &mut args[1]);
    Box::new(FileAttr { k, x })
}
struct FileAttr {
    k: CExpPtr<i64>,
    x: CExpPtr<i64>,
}
impl CExp<Value> for FileAttr {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let k = self.k.eval(ee, d);
        let x = self.x.eval(ee, d);
        let result = ee.tr.file_attr(k, x);
        Value::String(result)
    }
}

/////////////////////////////
/// Compile call to FILECONTENT.
fn c_filecontent(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Int]);
    let k = c_int(b, &mut args[0]);
    Box::new(FileContent { k })
}
struct FileContent {
    k: CExpPtr<i64>,
}
impl CExp<Value> for FileContent {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let k = self.k.eval(ee, d);
        let result = ee.tr.file_content(k);
        Value::ArcBinary(result)
    }
}

/////////////////////////////
/// Compile call to REPACKFILE.
#[cfg(feature = "pack")]
fn c_repackfile(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(
        b,
        args,
        &[DataKind::Int, DataKind::String, DataKind::String],
    );
    let k = c_int(b, &mut args[0]);
    let s = c_value(b, &mut args[1]);
    let n = c_value(b, &mut args[2]);
    Box::new(RepackFile { k, s, n })
}
#[cfg(feature = "pack")]
struct RepackFile {
    k: CExpPtr<i64>,
    s: CExpPtr<Value>,
    n: CExpPtr<Value>,
}
#[cfg(feature = "pack")]
impl CExp<i64> for RepackFile {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let k = self.k.eval(ee, d);
        let s = self.s.eval(ee, d).str();
        let n = self.n.eval(ee, d).str();
        ee.db.repack_file(k, &s, &n)
    }
}

#[cfg(feature = "verify")]
/// SQL to load every table ( required for database::verify to work correctly ).
const LOADALLTABLES: &str = "  
  DECLARE sid int, sname string, tname string
  FOR sid = Id, sname = Name FROM sys.Schema
  BEGIN
    FOR tname = Name FROM sys.Table WHERE Schema = sid
    BEGIN
      EXECUTE( 'IF false SELECT Id FROM ' | sys.Dot( sname, tname ) )
    END
  END";

#[cfg(feature = "verify")]
/////////////////////////////
/// Compile call to VERIFYDB.
fn c_verifydb(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[]);
    Box::new(VerifyDb {})
}

#[cfg(feature = "verify")]
struct VerifyDb {}

#[cfg(feature = "verify")]
impl CExp<Value> for VerifyDb {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> Value {
        ee.db.run(LOADALLTABLES, ee.tr);
        let s = ee.db.verify();
        Value::String(Rc::new(s))
    }
}

#[cfg(feature = "renumber")]
/////////////////////////////
/// Compile call to RENUMBER.
fn c_renumber(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(Renumber {})
}

#[cfg(feature = "renumber")]
struct Renumber {}

#[cfg(feature = "renumber")]
impl CExp<i64> for Renumber {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        ee.db.run(LOADALLTABLES, ee.tr);
        ee.db.renumber();
        0
    }
}

/// Compile call to BINTOSTR.
fn c_bintostr(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Binary]);
    let bytes = c_value(b, &mut args[0]);
    Box::new(Bintostr { bytes })
}
/// Compiled call to BINTOSTR.
struct Bintostr {
    bytes: CExpPtr<Value>,
}
impl CExp<Value> for Bintostr {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let bytes = self.bytes.eval(ee, d);
        Value::String(Rc::new(String::from_utf8(bytes.bina().to_vec()).unwrap()))
    }
}
