use std::{mem,ops};

use crate::{ Value, sqlparse::Parser, cexp, sql::*, 
  run::{Inst,FunctionPtr,CSelectExpression,CTableExpression}, 
  eval::EvalEnv, table::TablePtr 
};

/// Compiled expression which yields type T when evaluated.
pub trait CExp<T>
{
  fn eval( &self, ee: &mut EvalEnv, data: &[u8] ) -> T;
}

/// Pointer to CExp.
pub type CExpPtr <T> = Box< dyn CExp<T> >;

/// Function that compiles a builtin function call ( see Database::register ).
#[derive(Clone,Copy)]
pub enum CompileFunc
{
  Value( fn(&Parser,&mut [Expr]) -> CExpPtr<Value> ),
  Int( fn(&Parser,&mut [Expr]) -> CExpPtr<i64> ),
  Float( fn(&Parser,&mut [Expr]) -> CExpPtr<f64> ),
}

/// Calculate various attributes such as data_type, is_constant etc.
fn check( p: &Parser, e: &mut Expr )
{
  if e.checked 
  { 
    return; 
  }
  e.is_constant = true;
  match &mut e.exp
  {
    ExprIs::BuiltinCall( name, args ) =>
    {
      if let Some( (dk,_cf) ) = p.db.builtins.borrow().get( name )
      {
        e.data_type = *dk as DataType;
        // e.cf = Some(*cf);
        for pe in args
        { 
          check( p, pe );
          if !pe.is_constant { e.is_constant = false; }
        }
      }
      else { panic!( "Unknown function {}", name ); }
    }
    ExprIs::Binary( op, b1, b2 ) => 
    {
      check( p, b1 );
      check( p, b2 );
      e.is_constant = b1.is_constant && b2.is_constant;
 
      let t1 = b1.data_type;
      let t2 = b2.data_type;
 
      if data_kind(t1) != data_kind(t2) && *op != Token::VBar 
      { 
        panic!( "Binary op type mismatch" ) 
      }
      e.data_type = match op
      {
        Token::Less | Token::LessEqual | Token::GreaterEqual | Token::Greater
          | Token::Equal | Token::NotEqual 
        => BOOL,
        Token::And | Token::Or 
        => 
        {
          if t1 != BOOL { panic!( "And/Or need bool operands" ) }
          BOOL
        }
        Token::Plus | Token::Times | Token::Minus | Token::Divide | Token::Percent => t1,
        Token::VBar => STRING,
        _ => panic!()
      }
    }
    ExprIs::Local( x ) => 
    {
      e.data_type = p.b.local_types[ *x ];
    }
    ExprIs::Const( x ) =>  
    {
      e.data_type = match *x 
      {
        Value::Bool(_) => BOOL,
        Value::Int(_) => BIGINT,
        Value::Float(_) => DOUBLE,
        Value::String(_) => STRING,
        Value::Binary(_) => BINARY,
        _ => NONE,
      }
    }
    ExprIs::Number(_) =>
    {
      e.data_type = BIGINT;
    }
    ExprIs::Case( x,els ) => 
    {
      check( p, els );
      if !els.is_constant { e.is_constant = false; }
      e.data_type = els.data_type;
      for (w,t) in x
      {
        check( p, w );
        if !w.is_constant { e.is_constant = false; }
        check( p, t );
        if !t.is_constant { e.is_constant = false; }
        if data_kind(e.data_type) != data_kind(t.data_type)
        {
          panic!( "CASE branch type mismatch" );
        }
      }
    }
    ExprIs::Not(x) =>
    {
      check( p, x );
      e.is_constant = x.is_constant;
      e.data_type = BOOL;
    }
    ExprIs::Minus(x) => 
    {
      check( p, x );
      e.is_constant = x.is_constant;
      e.data_type = x.data_type; 
    } 
    ExprIs::FuncCall( name, parms ) => 
    {
      e.data_type = rlook( p, name ).return_type;
      for a in parms
      {
        check( p, a );
        if !a.is_constant { e.is_constant = false; }
      }
    }
    ExprIs::Name(x) => 
    {
      e.is_constant = false;
      let ( col, data_type ) = name_to_colnum( p, x );
      e.col = col;
      e.data_type = data_type;
    }
    _ => panic!()
  }
  e.checked = true;
}

/// Get DataType of an expression.
fn get_type( p: &Parser, e: &mut Expr ) -> DataType
{ 
  check( p, e );
  e.data_type 
}

/// Get DataKind of an expression.
pub fn get_kind( p: &Parser, e: &mut Expr ) -> DataKind
{ 
  check( p, e );
  data_kind( e.data_type ) 
}

/// Compile a call to a builtin function that returns a Value.
fn compile_builtin_value( p: &Parser, name: &str, args: &mut [Expr] ) -> CExpPtr<Value>
{
  if let Some( (_dk,CompileFunc::Value(cf)) ) = p.db.builtins.borrow().get( name )
  {
    return cf(p,args);
  }
  panic!()
}

/// Compile an expression.
pub fn cexp_value( p: &Parser, e: &mut Expr ) -> CExpPtr<Value>
{
  match get_kind( p, e )
  {
    DataKind::Bool => Box::new( cexp::BoolToVal{ ce: cexp_bool( p, e ) } ),   
    DataKind::Int => Box::new( cexp::IntToVal{ ce: cexp_int( p, e ) } ),
    DataKind::Float => Box::new( cexp::FloatToVal{ ce: cexp_float( p, e ) } ),
    DataKind::Decimal => Box::new( cexp::IntToVal{ ce: cexp_decimal( p, e ) } ),
    _ =>
    {
      match &mut e.exp 
      {
        ExprIs::Const( x ) => Box::new( cexp::Const{ value: (*x).clone() } ),
        ExprIs::Local( x ) => Box::new( cexp::Local{ num: *x } ),
        ExprIs::Binary( op, b1, b2 ) =>
        {
          let c1 = cexp_value( p, b1 );
          let c2 = cexp_value( p, b2 );
          match op
          {        
            Token::VBar => Box::new( cexp::Concat{ c1, c2 } ),
            _ => panic!()
          }
        }   
        ExprIs::FuncCall( name, parms ) => compile_call( p, name, parms ),
        ExprIs::Name( x ) =>
        {
          let (off,typ) = name_to_col( p, x );
          match typ
          {
            STRING => Box::new( cexp::ColumnString{ off } ),
            BINARY => Box::new( cexp::ColumnBinary{ off } ),
            _ => panic!()
          }
        }
        ExprIs::Case( list, els ) => { compile_case( p, list, els, cexp_value ) }
        ExprIs::BuiltinCall( name, parms ) => { compile_builtin_value( p, name, parms ) }
        _ => panic!()
      }
    }
  }
}

/// Compile int expression.
pub fn cexp_int( p: &Parser, e: &mut Expr ) -> CExpPtr<i64>
{   
  if get_kind( p, e ) != DataKind::Int { panic!( "Integer type expected" ) }
  match &mut e.exp 
  {
    ExprIs::Name( x ) => 
    {
      let (off,typ) = name_to_col( p, x );
      match data_size(typ)
      {
        8 => Box::new( cexp::ColumnI64{ off } ),
        4 => Box::new( cexp::ColumnI32{ off } ),
        2 => Box::new( cexp::ColumnI16{ off } ),
        1 => Box::new( cexp::ColumnI8{ off } ),
        _ => panic!()
      }
    }
    ExprIs::Number( x ) => Box::new( cexp::Const{ value: *x } ),
    ExprIs::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    ExprIs::Binary( op, b1, b2 ) => compile_arithmetic( p, *op, b1, b2, cexp_int ),
    ExprIs::Minus( u ) => Box::new( cexp::Minus::<i64>{ ce: cexp_int( p, u ) } ),
    ExprIs::FuncCall( name, parms ) => Box::new( cexp::ValToInt{ ce: compile_call(p,name,parms) } ),  
    ExprIs::Case( list, els ) => compile_case( p, list, els, cexp_int ),
    ExprIs::BuiltinCall( name, parms ) => compile_builtin_int( p, name, parms ),
    _ => panic!()
  }
}

/// Compile float expression.
pub fn cexp_float( p: &Parser, e: &mut Expr ) -> CExpPtr<f64>
{   
  if get_kind( p, e ) != DataKind::Float { panic!( "Float type expected" ) }
  match &mut e.exp 
  {
    ExprIs::Name( x ) => 
    {
      let (off,typ) = name_to_col( p, x );
      match data_size(typ)
      {
        8 => Box::new( cexp::ColumnF64{ off } ),
        4 => Box::new( cexp::ColumnF32{ off } ),
        _ => panic!()
      }
    }
    ExprIs::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    ExprIs::Binary( op, b1, b2 ) => compile_arithmetic( p, *op, b1, b2, cexp_float ),
    ExprIs::Minus( u ) => Box::new( cexp::Minus::<f64>{ ce: cexp_float( p, u ) } ),
    ExprIs::FuncCall( name, parms ) => Box::new( cexp::ValToFloat{ ce: compile_call(p,name,parms) } ),  
    ExprIs::Case( list, els ) => compile_case( p, list, els, cexp_float ),
    ExprIs::BuiltinCall( name, parms ) => compile_builtin_float( p, name, parms ),
    _ => panic!()
  }
}

/// Compile bool expression.
pub fn cexp_bool( p: &Parser, e: &mut Expr ) -> CExpPtr<bool>
{   
  if get_kind( p, e ) != DataKind::Bool { panic!( "Bool type expected" ) }
  match &mut e.exp 
  {
    ExprIs::Name( x ) => 
    {
      let (off,_typ) = name_to_col( p, x );
      Box::new( cexp::ColumnBool{ off } )
    }
    ExprIs::Const( x ) => 
    { 
      if let Value::Bool(b) = *x
      {
        Box::new( cexp::Const::<bool>{ value: b } )
      }
      else 
      {
        panic!()
      }
    }
    ExprIs::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    ExprIs::Binary( op, b1, b2 ) =>
    {
      if *op == Token::Or || *op == Token::And
      {
        let c1 = cexp_bool( p, b1 );
        let c2 = cexp_bool( p, b2 );
        match op
        {  
          Token::Or => Box::new( cexp::Or{ c1, c2 } ),  
          Token::And => Box::new( cexp::And{ c1, c2 } ),  
          _ => panic!()
        }
      }
      else
      {
        match get_kind( p, b1 )
        {      
          DataKind::Bool => compile_compare( p, *op, b1, b2, cexp_bool ),
          DataKind::Int => compile_compare( p, *op, b1, b2, cexp_int ),
          DataKind::Float => compile_compare( p, *op, b1, b2, cexp_float ),
          _ => compile_compare( p, *op, b1, b2, cexp_value )
        }
      }
    }
    ExprIs::Not( x ) => Box::new( cexp::Not{ ce: cexp_bool(p,x) } ),
    ExprIs::FuncCall( name,parms ) => Box::new( cexp::ValToBool{ ce: compile_call(p,name,parms) } ),
    ExprIs::Case( list, els ) => compile_case( p, list, els, cexp_bool ), 
    _ => panic!()
  }
}

/// Compile decimal expression.
pub fn cexp_decimal( p: &Parser, e: &mut Expr ) -> CExpPtr<i64>
{   
  if get_kind( p, e ) != DataKind::Decimal { panic!( "Decimal type expected" ) }
  match &mut e.exp 
  {
    ExprIs::Name( x ) => 
    {
      let (off,typ) = name_to_col( p, x );
      let n = data_size(typ);
      Box::new( cexp::ColumnDecimal{ off, n } )
    }
    ExprIs::Number( x ) => Box::new( cexp::Const{ value: *x } ),
    ExprIs::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    ExprIs::Binary( op, b1, b2 ) =>
    {
      let c1 = cexp_decimal( p, b1 );
      let c2 = cexp_decimal( p, b2 );
      match op
      {        
        Token::Plus => Box::new( cexp::Add::<i64>{ c1, c2 } ),
        Token::Minus => Box::new( cexp::Sub::<i64>{ c1, c2 } ),
        Token::Times => Box::new( cexp::Mul::<i64>{ c1, c2 } ),
        Token::Divide => Box::new( cexp::Div::<i64>{ c1, c2 } ), 
        Token::Percent => Box::new( cexp::Rem::<i64>{ c1, c2 } ),
        _ => panic!()
      }
    }   
    ExprIs::Minus( u ) => 
    {
      let ce = cexp_decimal( p, u );
      Box::new( cexp::Minus::<i64>{ ce } )
    }
    ExprIs::FuncCall( name, parms ) => Box::new( cexp::ValToInt{ ce: compile_call(p,name,parms) } ),  
    ExprIs::Case( list, els ) => compile_case( p, list, els, cexp_decimal ),
    _ => panic!()
  }
}

/// Compile arithmetic.
fn compile_arithmetic<T>
( 
  p: &Parser, 
  op:Token, 
  e1: &mut Expr, 
  e2: &mut Expr, 
  cexp: fn(&Parser,&mut Expr) -> CExpPtr<T> 
) -> CExpPtr<T>
where T: 'static + ops::Add<Output=T> + ops::Sub<Output=T> 
  + ops::Mul<Output=T> + ops::Div<Output=T> + ops::Rem<Output=T>
{
  let c1 = cexp( p, e1 );
  let c2 = cexp( p, e2 );
  match op
  {
    Token::Plus => Box::new( cexp::Add::<T>{ c1, c2 } ),
    Token::Minus => Box::new( cexp::Sub::<T>{ c1, c2 } ),
    Token::Times => Box::new( cexp::Mul::<T>{ c1, c2 } ),
    Token::Divide => Box::new( cexp::Div::<T>{ c1, c2 } ), 
    Token::Percent => Box::new( cexp::Rem::<T>{ c1, c2 } ),
    _ => panic!()
  }
}

/// Compile comparison.
fn compile_compare<T>
( 
  p: &Parser, 
  op:Token, 
  e1: &mut Expr, 
  e2: &mut Expr, 
  cexp: fn(&Parser,&mut Expr) -> CExpPtr<T> 
) -> CExpPtr<bool>
where T: 'static + std::cmp::PartialOrd
{
  let c1 = cexp( p, e1 );
  let c2 = cexp( p, e2 );
  match op
  {        
    Token::Equal => Box::new( cexp::Equal::<T>{ c1, c2 } ),
    Token::NotEqual => Box::new( cexp::NotEqual::<T>{ c1, c2 } ),
    Token::Less => Box::new( cexp::Less::<T>{ c1, c2 } ),
    Token::Greater => Box::new( cexp::Greater::<T>{ c1, c2 } ),
    Token::LessEqual => Box::new( cexp::LessEqual::<T>{ c1, c2 } ),
    Token::GreaterEqual => Box::new( cexp::GreaterEqual::<T>{ c1, c2 } ),
    _ => panic!()
  } 
}

/// Compile CASE Expression.
fn compile_case<T>
( 
  p: &Parser, 
  wes: &mut [(Expr,Expr)], 
  els: &mut Expr, 
  cexp: fn(&Parser,&mut Expr) -> CExpPtr<T> 
) -> CExpPtr<T> 
where T:'static
{
  let mut whens = Vec::new();
  for ( be, ve ) in wes
  {
    let b = cexp_bool( p, be );
    let v = cexp( p, ve );
    whens.push( (b,v) );
  }
  let els = cexp(p,els);
  Box::new( cexp::Case::<T>{ whens, els } )
}

/// Compile a call to a builtin function that returns an integer.
fn compile_builtin_int( p: &Parser, name: &str, args: &mut [Expr] ) -> CExpPtr<i64>
{
  if let Some( (_dk,CompileFunc::Int(cf)) ) = p.db.builtins.borrow().get( name )
  {
    return cf(p,args);
  }
  panic!()
}

/// Compile a call to a builtin function that returns a float.
fn compile_builtin_float( p: &Parser, name: &str, args: &mut [Expr] ) -> CExpPtr<f64>
{
  if let Some( (_dk,CompileFunc::Float(cf)) ) = p.db.builtins.borrow().get( name )
  {
    return cf(p,args);
  }
  panic!()
}

/// Compile SelectExpression to CSelectExpression.
pub(crate) fn compile_select( p: &mut Parser, mut x: SelectExpression ) -> CSelectExpression
{
  let mut from = x.from.map(|mut te| compile_te( p, &mut te ));

  let table = match &from
  {
    Some( CTableExpression::Base( t ) ) => Some(t.clone()),
    _ => None
  };
  let mut index_from = None;

  // Is the save necessary?
  let save = mem::replace( &mut p.from, from );

  let mut exps = Vec::new();
  for mut e in x.exps
  {
    exps.push( cexp_value( p, &mut e ) ); 
  }
  let wher =
  {
     if let Some(we) = &mut x.wher
     {
       if get_kind( p, we ) != DataKind::Bool { panic!("WHERE expression must be bool") }
       
       if let Some(table) = table
       {
         index_from = table.index_from( p, we );
       }
       if index_from.is_some() { None }
       else { Some( cexp_bool(p,we) ) }
     }
     else { None }
  };

  let mut orderby = Vec::new();
  let mut desc = Vec::new();
  for (e,a) in &mut x.orderby
  {
    let e = cexp_value( p, e );
    orderby.push( e );
    desc.push( *a );
  }

  from = mem::replace( &mut p.from, save ); 
  if index_from.is_some() { from = index_from; }

  CSelectExpression
  { colnames: x.colnames, assigns: x.assigns,
    exps, from, wher, orderby, desc
  }
}

/// Compile a TableExpression to CTableExpression.
pub(crate) fn compile_te( p: &Parser, te: &mut TableExpression ) -> CTableExpression
{
  match te
  {
    TableExpression::Values( x ) =>
    {
      let mut cm = Vec::new();
      for r in x
      {
        let mut cr = Vec::new();
        for e in r
        {
          let ce = cexp_value( p, e );
          cr.push( ce );
        }
        cm.push( cr );
      }
      CTableExpression::Values( cm )
    }
    TableExpression::Base( x ) =>
    {
      let t = tlook( p, x );
      CTableExpression::Base( t )
    }
  }
}

/// Look for named table in database.
pub(crate) fn tlook( p: &Parser, name: &ObjRef ) -> TablePtr
{
  if let Some( t ) = p.db.get_table( name ) { t }
  else { panic!( "table {} not found", name.to_str() ) }
}

/// Look for named function in database and compile it if not already compiled.
pub(crate) fn rlook( p: &Parser, name: &ObjRef ) -> FunctionPtr
{
  if let Some( r ) = p.db.get_function( name ) 
  { 
    let (compiled,src) =
    {
      ( r.compiled.get(), r.source.clone() )
    }; 

    if !compiled
    {
      r.compiled.set( true );
      let mut p = Parser::new( &src, &p.db );
      p.function_name = Some(name);
      let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe( || 
      { 
        p.parse_function();
      }));
      if let Err(x) = result
      {
        r.compiled.set( false );
        std::panic::panic_any
        (
          if let Some(e) = x.downcast_ref::<SqlError>()
          {
            SqlError{ msg:e.msg.clone(), line: e.line, column:e.column, rname:e.rname.clone() }
          }
          else if let Some(s) = x.downcast_ref::<&str>()
          {
            p.make_error(s.to_string())
          }
          else if let Some(s) = x.downcast_ref::<String>()
          {
            p.make_error(s.to_string())
          }
          else
          {
            p.make_error("unrecognised/unexpected error".to_string())
          }
        );
      }
      *r.ilist.borrow_mut() = p.b.ilist;
    }
    r
  }
  else { panic!( "function {} not found", name.to_str() ) }
}

/// Lookup the column offset and DataType of a named column.
pub(crate) fn name_to_col( p: &Parser, name: &str ) -> (usize,DataType)
{
  if let Some( CTableExpression::Base( t ) ) = &p.from
  {
    let info = &t.info;
    if let Some(num) = info.get( name )
    { 
      let colnum = *num;
      if colnum == usize::MAX { return ( 0, BIGINT ); }
      return ( info.off[colnum], info.types[colnum] ); 
    }
  }
  panic!( "Name '{}' not found", name )
}

/// Lookup the column number and DataType of a named column.
pub(crate) fn name_to_colnum( p: &Parser, name: &str ) -> (usize,DataType)
{
  if let Some( CTableExpression::Base( t ) ) = &p.from
  {
    let info = &t.info;
    if let Some(num) = info.get( name )
    { 
      let colnum = *num;
      if colnum == usize::MAX { return ( 0, BIGINT ); }
      return ( colnum, info.types[colnum] ); 
    }
  }
  panic!( "Name '{}' not found", name )
}

/// Compile ExprCall to CExpPtr<Value>, checking parameter types.
pub(crate) fn compile_call( p: &Parser, name: &ObjRef, parms: &mut Vec<Expr> ) -> CExpPtr<Value>
{
  let rp : FunctionPtr = rlook( p, name ); 

  let mut pv : Vec<CExpPtr<Value>> = Vec::new();
  let mut pt : Vec<DataType> = Vec::new();
  for e in parms
  {
    let t : DataType = get_type( p, e );
    pt.push( t );
    let ce = cexp_value( p, e );
    pv.push( ce );
  }

  p.check_types( &rp, &pt );
  Box::new( cexp::Call{ rp, pv } )
}

/// Generate code to evaluate expression and push the value onto the stack.
pub(crate) fn push( p: &mut Parser, e: &mut Expr ) -> DataType
{
  if p.parse_only { return NONE; }

  let t = get_type( p, e );
  match &mut e.exp 
  {
    ExprIs::Number( x ) => { p.add( Inst::PushIntConst( *x ) ); }
    ExprIs::Const( x ) => { p.add( Inst::PushConst( (*x).clone() ) ); }
    ExprIs::Binary( _,_,_ ) => 
    {
      match data_kind( t )
      {
        DataKind::Int => 
        {
          let ce = cexp_int( p, e ); 
          p.add( Inst::PushInt( ce ) ); 
        }
        DataKind::Bool => 
        { 
          let ce = cexp_bool( p, e ); 
          p.add( Inst::PushBool( ce ) ); 
        }
        _ =>
        {
          let ce = cexp_value( p, e ); 
          p.add( Inst::PushValue(ce) );             
        }
      }
    }
    ExprIs::FuncCall( name, parms ) => 
    {
      let rp = rlook( p, name );
      { 
        if rp.param_count != parms.len() { panic!( "Param count mismatch" ) }
        for (pnum,e) in parms.iter_mut().enumerate()
        {
          let et = data_kind( push( p, e ) );
          let ft = data_kind( rp.local_types[ pnum ] );
          if  ft != et { panic!( "Param type mismatch expected {:?} got {:?}", ft, et ); }
        }
      }
      p.add( Inst::Call( rp ) );
    }
    ExprIs::Local( x ) => { p.add( Inst::PushLocal( *x ) ); }
    _ => 
    { 
      let ce = cexp_value( p, e ); 
      p.add( Inst::PushValue(ce) );
    }
  }
  t
}
