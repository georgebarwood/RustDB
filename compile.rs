use std::mem;
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
pub enum CompileFunc
{
  Value( fn(&Parser,&[Expr]) -> CExpPtr<Value> ),
  Int( fn(&Parser,&[Expr]) -> CExpPtr<i64> ),
  Float( fn(&Parser,&[Expr]) -> CExpPtr<f64> ),
}

/// Calculate DataType of an expression.
pub fn calc_type( p: &Parser, e: &Expr ) -> DataType
{  
  // Function calculates type and also checks binary operands have same type.
  match e
  {
    Expr::BuiltinCall( name, _args ) =>
    {
      if let Some( (dk,_cf) ) = p.db.builtins.borrow().get( name )
      {
        *dk as DataType
      }
      else { panic!( "Unknown function {}", name ); }
    }
    Expr::Local( x ) => p.b.local_types[ *x ],
    Expr::Const( x ) =>  match *x 
    {
      Value::Bool(_) => BOOL,
      Value::Int(_) => BIGINT,
      Value::Float(_) => DOUBLE,
      Value::String(_) => STRING,
      Value::Binary(_) => BINARY,
      _ => NONE,
    }
    Expr::Number(_) => BIGINT,
    Expr::Case( (_x,els) ) => calc_type( p, els ),
    Expr::Binary( (op, b1, b2) ) => 
    {
      let t1 = calc_type( p, b1 );
      let t2 = calc_type( p, b2 );
      if data_kind(t1) != data_kind(t2) && *op != Token::VBar 
      { panic!( "Binary op type mismatch" ) }
      match op
      {
        Token::Less | Token::LessEqual | Token::GreaterEqual | Token::Greater
          | Token::Equal | Token::NotEqual 
        => BOOL,
        Token::And | Token::Or 
        => 
        {
          if t1 != BOOL { p.err( "And/Or need bool operands" ); }
          BOOL
        }
        Token::Plus | Token::Times | Token::Minus | Token::Divide | Token::Percent => t1,
        Token::VBar => STRING,
        _ => panic!( "Unknown operator {:?}", op )
      }
    }
    Expr::Not(_) => BOOL,
    Expr::Minus(x) => calc_type( p, x ),  
    Expr::FuncCall(x) => rlook( p, &x.name ).return_type,
    Expr::Name(x) => name_to_col( p, x ).1,
    _ => NONE,
  }
}

/// Compile a call to a builtin function that returns a Value.
fn compile_builtin_value( p: &Parser, name: &str, args: &[Expr] ) -> CExpPtr<Value>
{
  if let Some( (_dk,CompileFunc::Value(cf)) ) = p.db.builtins.borrow().get( name )
  {
    return cf(p,args);
  }
  panic!()
}

/// Compile a call to a builtin function that returns an integer.
fn compile_builtin_int( p: &Parser, name: &str, args: &[Expr] ) -> CExpPtr<i64>
{
  if let Some( (_dk,CompileFunc::Int(cf)) ) = p.db.builtins.borrow().get( name )
  {
    return cf(p,args);
  }
  panic!()
}

/// Compile a call to a builtin function that returns a float.
fn compile_builtin_float( p: &Parser, name: &str, args: &[Expr] ) -> CExpPtr<f64>
{
  if let Some( (_dk,CompileFunc::Float(cf)) ) = p.db.builtins.borrow().get( name )
  {
    return cf(p,args);
  }
  panic!()
}

/// Compile an expression.
pub fn cexp_value( p: &Parser, e: &Expr ) -> CExpPtr<Value>
{   
  let typ = calc_type( p, e );
  match data_kind( typ )
  {
    DataKind::Bool => 
    {
      let ce = cexp_bool( p, e );
      Box::new( cexp::BoolToVal{ ce } )
    }      
    DataKind::Int => 
    {
      let ce = cexp_int( p, e );
      Box::new( cexp::IntToVal{ ce } )
    }
    DataKind::Float => 
    {
      let ce = cexp_float( p, e );
      Box::new( cexp::FloatToVal{ ce } )
    }
    DataKind::Decimal =>
    {
      let ce = cexp_decimal( p, e );
      Box::new( cexp::IntToVal{ ce } )
    }
    _ =>
    {
      match e 
      {
        Expr::Const( x ) => Box::new( cexp::Const{ value: (*x).clone() } ),
        Expr::Local( x ) => Box::new( cexp::Local{ num: *x } ),
        Expr::Binary( ( op, b1, b2 ) ) =>
        {
          let c1 = cexp_value( p, b1 );
          let c2 = cexp_value( p, b2 );
          match op
          {        
            Token::VBar => Box::new( cexp::Concat{ c1, c2 } ),
            _ => panic!( "Unknown operator for {:?} : '{:?}'", typ, op )
          }
        }   
        Expr::FuncCall( x ) => compile_call( p, x ),
        Expr::Name( x ) =>
        {
          let (off,typ) = name_to_col( p, x );
          match typ
          {
            STRING => Box::new( cexp::ColumnString{ off } ),
            _ => panic!()
          }
        }
        Expr::Case( (list, def) ) => { compile_case( p, list, def, cexp_value ) }
        Expr::BuiltinCall( name, parms ) => { compile_builtin_value( p, name, parms ) }
        _ => { panic!( "ToDo" ) }
      }
    }
  }
}

/// Compile decimal expression.
pub fn cexp_decimal( p: &Parser, e: &Expr ) -> CExpPtr<i64>
{   
  if data_kind( calc_type( p, e ) ) != DataKind::Decimal { p.err( "Decimal type expected" ); }
  match e 
  {
    Expr::Name( x ) => 
    {
      let (off,typ) = name_to_col( p, x );
      let n = data_size(typ);
      Box::new( cexp::ColumnDecimal{ off, n } )
    }
    Expr::Number( x ) => Box::new( cexp::Const{ value: *x } ),
    Expr::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    Expr::Binary( ( op, b1, b2 ) ) =>
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
        _ => panic!( "ToDo cexp_int unknown op {:?}", op )
      }
    }   
    Expr::Minus( u ) => 
    {
      let ce = cexp_decimal( p, u );
      Box::new( cexp::Minus::<i64>{ ce } )
    }
    Expr::FuncCall( x ) => Box::new( cexp::ValToInt{ ce: compile_call(p,x) } ),  
    Expr::Case( (list, def) ) => { compile_case( p, list, def, cexp_decimal ) }
    _ => { panic!("ToDo" ) }
  }
}

/// Compile int expression.
pub fn cexp_int( p: &Parser, e: &Expr ) -> CExpPtr<i64>
{   
  if data_kind( calc_type( p, e ) ) != DataKind::Int { p.err( "Integer type expected" ); }
  match e 
  {
    Expr::Name( x ) => 
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
    Expr::Number( x ) => Box::new( cexp::Const{ value: *x } ),
    Expr::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    Expr::Binary( ( op, b1, b2 ) ) =>
    {
      let c1 = cexp_int( p, b1 );
      let c2 = cexp_int( p, b2 );
      match op
      {        
        Token::Plus => Box::new( cexp::Add::<i64>{ c1, c2 } ),
        Token::Minus => Box::new( cexp::Sub::<i64>{ c1, c2 } ),
        Token::Times => Box::new( cexp::Mul::<i64>{ c1, c2 } ),
        Token::Divide => Box::new( cexp::Div::<i64>{ c1, c2 } ), 
        Token::Percent => Box::new( cexp::Rem::<i64>{ c1, c2 } ),
        _ => { println!("op={:?}", op); panic!( "ToDo cexp_int unknown op" ) }
      }
    }   
    Expr::Minus( u ) => 
    {
      let ce = cexp_int( p, u );
      Box::new( cexp::Minus::<i64>{ ce } )
    }
    Expr::FuncCall( x ) => Box::new( cexp::ValToInt{ ce: compile_call(p,x) } ),  
    Expr::Case( (list, def) ) => { compile_case( p, list, def, cexp_int ) },
    Expr::BuiltinCall( name, parms ) => 
    { 
      compile_builtin_int( p, name, parms )
    }
    _ => { panic!("ToDo") }
  }
}

/// Compile float expression.
pub fn cexp_float( p: &Parser, e: &Expr ) -> CExpPtr<f64>
{   
  if data_kind( calc_type( p, e ) ) != DataKind::Float { p.err( "Float type expected" ); }
  match e 
  {
    Expr::Name( x ) => 
    {
      let (off,typ) = name_to_col( p, x );
      match data_size(typ)
      {
        8 => Box::new( cexp::ColumnF64{ off } ),
        4 => Box::new( cexp::ColumnF32{ off } ),
        _ => panic!()
      }
    }
    Expr::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    Expr::Binary( ( op, b1, b2 ) ) =>
    {
      let c1 = cexp_float( p, b1 );
      let c2 = cexp_float( p, b2 );
      match op
      {        
        Token::Plus => Box::new( cexp::Add::<f64>{ c1, c2 } ),
        Token::Minus => Box::new( cexp::Sub::<f64>{ c1, c2 } ),
        Token::Times => Box::new( cexp::Mul::<f64>{ c1, c2 } ),
        Token::Divide => Box::new( cexp::Div::<f64>{ c1, c2 } ), 
        Token::Percent => Box::new( cexp::Rem::<f64>{ c1, c2 } ),
        _ => { println!("op={:?}", op); panic!( "ToDo cexp_int unknown op" ) }
      }
    }   
    Expr::Minus( u ) => 
    {
      let ce = cexp_float( p, u );
      Box::new( cexp::Minus::<f64>{ ce } )
    }
    Expr::FuncCall( x ) => Box::new( cexp::ValToFloat{ ce: compile_call(p,x) } ),  
    Expr::Case( (list, def) ) => { compile_case( p, list, def, cexp_float ) },
    Expr::BuiltinCall( name, parms ) => 
    { 
      compile_builtin_float( p, name, parms )
    }
    _ => { panic!("ToDo") }
  }
}

/// Compile bool expression.
pub fn cexp_bool( p: &Parser, e: &Expr ) -> CExpPtr<bool>
{   
  if data_kind( calc_type( p, e ) ) != DataKind::Bool { p.err( "Bool type expected" ); }
  match e 
  {
    Expr::Name( x ) => 
    {
      let (off,_typ) = name_to_col( p, x );
      Box::new( cexp::ColumnBool{ off } )
    }
    Expr::Const( x ) => 
    { 
      if let Value::Bool(b) = *x
      {
        Box::new( cexp::Const::<bool>{ value: b } )
      }
      else 
      {
        panic!( "bool const" )
      }
    }
    Expr::Local( x ) => Box::new( cexp::Local{ num: *x } ),
    Expr::Binary( ( op, b1, b2 ) ) =>
    {
      let t = data_kind( calc_type( p, b1 ) );
      if t == DataKind::Bool
      {
        let c1 = cexp_bool( p, b1 );
        let c2 = cexp_bool( p, b2 );
        match op
        {  
          Token::Or => Box::new( cexp::Or{ c1, c2 } ),  
          Token::And => Box::new( cexp::And{ c1, c2 } ),   
          Token::Equal => Box::new( cexp::Equal::<bool>{ c1, c2 } ),
          Token::NotEqual => Box::new( cexp::NotEqual::<bool>{ c1, c2 } ),
          Token::Less => Box::new( cexp::Less::<bool>{ c1, c2 } ),
          Token::Greater => Box::new( cexp::Greater::<bool>{ c1, c2 } ),
          Token::LessEqual => Box::new( cexp::LessEqual::<bool>{ c1, c2 } ),
          Token::GreaterEqual => Box::new( cexp::GreaterEqual::<bool>{ c1, c2 } ),
          _ => { p.err( "ToDo cexp_bool unknown bool op" ); }
        }          
      }
      else if t == DataKind::Int
      {
        let c1 = cexp_int( p, b1 );
        let c2 = cexp_int( p, b2 );
        match op
        {        
          Token::Equal => Box::new( cexp::Equal::<i64>{ c1, c2 } ),
          Token::NotEqual => Box::new( cexp::NotEqual::<i64>{ c1, c2 } ),
          Token::Less => Box::new( cexp::Less::<i64>{ c1, c2 } ),
          Token::Greater => Box::new( cexp::Greater::<i64>{ c1, c2 } ),
          Token::LessEqual => Box::new( cexp::LessEqual::<i64>{ c1, c2 } ),
          Token::GreaterEqual => Box::new( cexp::GreaterEqual::<i64>{ c1, c2 } ),
          _ => { p.err( "ToDo cexp_bool unknown int op" ); }
        } 
      }
      else if t == DataKind::Float
      {
        let c1 = cexp_float( p, b1 );
        let c2 = cexp_float( p, b2 );
        match op
        {        
          Token::Equal => Box::new( cexp::Equal::<f64>{ c1, c2 } ),
          Token::NotEqual => Box::new( cexp::NotEqual::<f64>{ c1, c2 } ),
          Token::Less => Box::new( cexp::Less::<f64>{ c1, c2 } ),
          Token::Greater => Box::new( cexp::Greater::<f64>{ c1, c2 } ),
          Token::LessEqual => Box::new( cexp::LessEqual::<f64>{ c1, c2 } ),
          Token::GreaterEqual => Box::new( cexp::GreaterEqual::<f64>{ c1, c2 } ),
          _ => { p.err( "ToDo cexp_bool unknown float op" ); }
        } 
      }
      else
      {
        let c1 = cexp_value( p, b1 );
        let c2 = cexp_value( p, b2 );
        match op
        {        
          Token::Equal => Box::new( cexp::Equal::<Value>{ c1, c2 } ),
          Token::NotEqual => Box::new( cexp::NotEqual::<Value>{ c1, c2 } ),
          Token::Less => Box::new( cexp::Less::<Value>{ c1, c2 } ),
          Token::Greater => Box::new( cexp::Greater::<Value>{ c1, c2 } ),
          Token::LessEqual => Box::new( cexp::LessEqual::<Value>{ c1, c2 } ),
          Token::GreaterEqual => Box::new( cexp::GreaterEqual::<Value>{ c1, c2 } ),
          _ => { p.err( "ToDo cexp_bool unknown op" ); }
        } 
      }
    }
    Expr::Not( x ) => Box::new( cexp::Not{ ce: cexp_bool(p,x) } ),
    Expr::FuncCall( x ) => Box::new( cexp::ValToBool{ ce: compile_call(p,x) } ),
    Expr::Case( (list, def) ) => { compile_case( p, list, def, cexp_bool ) }  
    _ => 
    {
      panic!( "cexp_bool unknown expression" )
    }
  }
}

/// Compile CASE Expression.
fn compile_case<T>
( 
  p: &Parser, 
  wes: &[(Expr,Expr)], 
  els: &Expr, 
  cexp: fn(&Parser,&Expr) -> CExpPtr<T> 
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

/// Compile SelectExpression to CSelectExpression.
pub(crate) fn compile_select( p: &mut Parser, x: SelectExpression ) -> CSelectExpression
{
  let mut from = x.from.map(|te| compile_te( p, &te ));
  let save = mem::replace( &mut p.from, from );

  let mut exps = Vec::new();
  for e in x.exps
  {
    exps.push( cexp_value( p, &e ) ); 
  }
  let wher = x.wher.map(|we| cexp_bool( p, &we ));

  let mut orderby = Vec::new();
  let mut desc = Vec::new();
  for (e,a) in &x.orderby
  {
    let e = cexp_value( p, e );
    orderby.push( e );
    desc.push( *a );
  }

  from = mem::replace( &mut p.from, save ); 

  CSelectExpression
  { 
    colnames: x.colnames,
    assigns: x.assigns,
    exps,
    from,
    wher, 
    orderby,
    desc
  }
}

/*
/// Compile SET statement.
pub(crate) fn compile_set( p: &mut Parser, x: &SelectExpression ) -> CSelectExpression
{
  let mut from = x.from.map(|te| compile_te( p, &te ));
  let save = mem::replace( &mut p.from, from );

  for (pnum,e) in x.exps.iter().enumerate()
  {
    let t = push( p, e );
    if data_kind( t ) != data_kind( p.b.local_types[ x.assigns[ pnum ] ] )
    {
      p.err( "assign type mismatch" );
    }
    p.add( Inst::PopToLocal( x.assigns[ pnum ] ) );
  }

  let wher = x.wher.map(|we| cexp_bool( p, &we ));
  from = mem::replace( &mut p.from, save ); 
}
*/

/// Compile a TableExpression to CTableExpression.
pub(crate) fn compile_te( p: &Parser, te: &TableExpression ) -> CTableExpression
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
  if let Some( t ) = p.db.load_table( name ) { t }
  else { panic!( "table {} not found", name.to_str() ) }
}

/// Look for named function in database and compile it if not already compiled.
pub(crate) fn rlook( p: &Parser, name: &ObjRef ) -> FunctionPtr
{
  if let Some( r ) = p.db.load_function( name ) 
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

/// Compile ExprCall to CExpPtr<Value>, checking parameter types.
pub(crate) fn compile_call( p: &Parser, x: &ExprCall ) -> CExpPtr<Value>
{
  let rp : FunctionPtr = rlook( p, &x.name ); 

  let mut pv : Vec<CExpPtr<Value>> = Vec::new();
  let mut pt : Vec<DataType> = Vec::new();
  for e in &x.parms
  {
    let t : DataType = calc_type( p, e );
    pt.push( t );
    let ce = match data_kind( t )
    {
      DataKind::Int => int_to_val( p, e ),
      DataKind::Bool => bool_to_val( p, e ),
      _ => cexp_value( p, e )
    };
    pv.push( ce );
  }

  p.check_types( &rp, &pt );
  Box::new( cexp::Call{ rp, pv } )
}

/// Convert compiled integer expression to value expression.
fn int_to_val( p: &Parser, e: &Expr ) -> CExpPtr<Value>
{
  Box::new( cexp::IntToVal{ ce: cexp_int( p, e ) } )
}

/// Convert compiled bool expression to value expression.
fn bool_to_val( p: &Parser, e: &Expr ) -> CExpPtr<Value>
{
  Box::new( cexp::BoolToVal{ ce: cexp_bool( p, e ) } )
}

/// Generate code to evaluate expression and push the value onto the stack.
pub(crate) fn push( p: &mut Parser, e: &Expr ) -> DataType
{
  if p.parse_only { return NONE; }

  let t = calc_type( p, e );
  match e 
  {
    Expr::Number( x ) => { p.add( Inst::PushIntConst( *x ) ); }
    Expr::Const( x ) => { p.add( Inst::PushConst( (*x).clone() ) ); }
    Expr::Binary( _ ) => 
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
    Expr::FuncCall( x ) => 
    {
      let rp = rlook( p, &x.name );
      { 
        if rp.param_count != x.parms.len() { p.err( "Param count mismatch" ); }
        for (pnum,e) in x.parms.iter().enumerate()
        {
          let et = data_kind( push( p, e ) );
          let ft = data_kind( rp.local_types[ pnum ] );
          if  ft != et { panic!( "Param type mismatch expected {:?} got {:?}", ft, et ); }
        }
      }
      p.add( Inst::Call( rp ) );
    }
    Expr::Local( x ) => { p.add( Inst::PushLocal( *x ) ); }
    _ => 
    { 
      let ce = cexp_value( p, e ); 
      p.add( Inst::PushValue(ce) );
    }
  }
  t
}
