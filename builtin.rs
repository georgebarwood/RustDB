use std::{ rc::Rc };
use crate::{ DB, Value, sql::{DK,Expr,data_kind}, 
  sqlparse::{Parser}, compile::{CExp,CExpPtr}, eval::EvalEnv,
  compile::{calc_type,cexp_value,cexp_int,CompileFunc} };

/// Registers builtin functions - called from Database::new.
pub fn register_builtins( db: &DB )
{
  let list = 
  [ 
    ( "ARG", DK::String, CompileFunc::Value( c_arg ) ),
    ( "GLOBAL", DK::Int, CompileFunc::Int( c_global ) ),
    ( "REPLACE", DK::String, CompileFunc::Value( c_replace ) ),
    ( "SUBSTRING", DK::String, CompileFunc::Value( c_substring ) ),
    ( "LEN", DK::Int, CompileFunc::Int( c_len ) ),
    ( "PARSEINT", DK::Int, CompileFunc::Int( c_parse_int ) ),
    ( "EXCEPTION", DK::String, CompileFunc::Value( c_exception ) ) 
  ];

  for ( name, typ, cf ) in list
  {
    db.register( name, typ, cf );
  }
}

/// Check number and kinds of arguments.
fn check_types( p: &Parser, args: &[Expr], dk: &[DK] )
{
  if args.len() != dk.len() { panic!( "Wrong number of args" ); }
  for (i,e) in args.iter().enumerate()
  {
    let k = data_kind( calc_type(p,e) );
    if  k != dk[i] 
    {
      panic!( "Builtin function arg {} type mismatch expected {:?} got {:?}", i+1, dk[i], k ); 
    }
  }
}

/////////////////////////////

/// Compile call to EXCEPTION().
fn c_exception( p: &Parser, args: &[Expr] ) -> CExpPtr<Value>
{
  check_types( p, args, &[] );
  Box::new( Exception{} ) 
} 

struct Exception{}

impl CExp<Value> for Exception
{
  fn eval( &self, e: &mut EvalEnv, _d: &[u8] ) -> Value
  {
    let err = e.qy.get_error();
    Value::String( Rc::new( err ) )
  }
}

/////////////////////////////

/// Compile call to LEN.
fn c_len( p: &Parser, args: &[Expr] ) -> CExpPtr<i64>
{
  check_types( p, args, &[ DK::String ] );
  let s = cexp_value( p, &args[0] );
  Box::new( Len{ s } ) 
} 

struct Len
{
  s: CExpPtr<Value>,
}

impl CExp<i64> for Len
{
  fn eval( &self, e: &mut EvalEnv, d: &[u8] ) -> i64
  {
    let s = self.s.eval( e, d ).str();
    s.len() as i64
  }
}

/////////////////////////////

/// Compile call to GLOBAL.
fn c_global( p: &Parser, args: &[Expr] ) -> CExpPtr<i64>
{
  check_types( p, args, &[ DK::Int ] );
  let x = cexp_int( p, &args[0] );
  Box::new( Global{ x } ) 
} 

struct Global
{
  x: CExpPtr<i64>,
}

impl CExp<i64> for Global
{
  fn eval( &self, ee: &mut EvalEnv, d: &[u8] ) -> i64
  {
    let x = self.x.eval( ee, d );
    ee.qy.global(x)
  }
}

/////////////////////////////

/// Compile call to PARSEINT.
fn c_parse_int( p: &Parser, args: &[Expr] ) -> CExpPtr<i64>
{
  check_types( p, args, &[ DK::String ] );
  let s = cexp_value( p, &args[0] );
  Box::new( ParseInt{ s } ) 
} 

struct ParseInt
{
  s: CExpPtr<Value>,
}

impl CExp<i64> for ParseInt
{
  fn eval( &self, e: &mut EvalEnv, d: &[u8] ) -> i64
  {
    let s = self.s.eval( e, d ).str();
    s.parse().unwrap()
  }
}



/////////////////////////////

/// Compile call to REPLACE.
fn c_replace( p: &Parser, args: &[Expr] ) -> CExpPtr<Value>
{
  check_types( p, args, &[ DK::String, DK::String, DK::String ] );
  let s = cexp_value( p, &args[0] );
  let pat = cexp_value( p, &args[1] );
  let sub = cexp_value( p, &args[2] );
  Box::new( Replace{ s, pat, sub } ) 
} 

struct Replace
{
  s: CExpPtr<Value>,
  pat: CExpPtr<Value>,
  sub: CExpPtr<Value>
}

impl CExp<Value> for Replace
{
  fn eval( &self, e: &mut EvalEnv, d: &[u8] ) -> Value
  {
    let s = self.s.eval( e, d ).str().to_string();
    let pat = self.pat.eval( e, d ).str().to_string();
    let sub = self.sub.eval( e, d ).str();
    let result = s.replace( &pat, &sub );
    Value::String( Rc::new( result ) )
  }
}

/////////////////////////////

/// Compile call to SUBSTRING.
fn c_substring( p: &Parser, args: &[Expr] ) -> CExpPtr<Value>
{
  check_types( p, args, &[ DK::String, DK::Int, DK::Int ] );
  let s = cexp_value( p, &args[0] );
  let f = cexp_int( p, &args[1] );
  let n = cexp_int( p, &args[2] );
  Box::new( Substring{ s, f, n } ) 
} 

struct Substring
{
  s: CExpPtr<Value>,
  f: CExpPtr<i64>,
  n: CExpPtr<i64>
}

impl CExp<Value> for Substring
{
  fn eval( &self, ee: &mut EvalEnv, d: &[u8] ) -> Value
  {
    let s = self.s.eval( ee, d ).str();
    let f = self.f.eval( ee, d ) as usize - 1;
    let n = self.n.eval( ee, d ) as usize;
    let mut lim = s.len();
    if lim > f+n { lim = f+n; } 
    let result = s[f..lim].to_string();
    Value::String( Rc::new( result ) )
  }
}

/////////////////////////////

/// Compile call to ARG.
fn c_arg( p: &Parser, args: &[Expr] ) -> CExpPtr<Value>
{
  check_types( p, args, &[ DK::Int, DK::String ] );
  let k = cexp_int( p, &args[0] );
  let s = cexp_value( p, &args[1] );
  Box::new( Arg{ k, s } ) 
} 

struct Arg
{
  k: CExpPtr<i64>,
  s: CExpPtr<Value>
}

impl CExp<Value> for Arg
{
  fn eval( &self, ee: &mut EvalEnv, d: &[u8] ) -> Value
  {
    let k = self.k.eval( ee, d );
    let s = self.s.eval( ee, d ).str();
    let result = ee.qy.arg( k, &s );
    Value::String( result )
  }
}