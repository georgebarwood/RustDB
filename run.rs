use std::{rc::Rc, cell::{Cell,RefCell}, cmp::Ordering};
use core::fmt::Debug;
use crate::
{  
  Value, sf::Asc, 
  sql::{DK,DataType,ObjRef,data_kind,IndexInfo}, 
  compile::CExpPtr, 
  table::{TablePtr,TableInfo} 
};

/// Instruction.
pub enum Inst
{
  Jump(usize),
  JumpIfFalse(usize,CExpPtr<bool>),
  Return,
  Throw,
  Execute,
  PopToLocal(usize),
  ForInit(usize,Box<CTableExpression>),
  ForNext(usize,Box<ForNextInfo>),
  ForSortInit(usize,Box<CSelectExpression>),
  ForSortNext(usize,Box<(usize,usize,Vec<usize>)>),
  DataOp(Box<DO>),
  PushValue(CExpPtr<Value>),
  Call(RoutinePtr),
  Select( Box<CSelectExpression> ),
  Set( Box<CSelectExpression> ),
  // Special push instructions ( optimisations )
  PushInt(CExpPtr<i64>),
  _PushFloat(CExpPtr<f64>),
  PushBool(CExpPtr<bool>),
  PushIntConst(i64),
  PushConst(Value),
  PushLocal(usize),
}

/// State for FOR loop (non-sorted case).
pub struct ForState
{
  pub asc: Asc,
}

impl Debug for ForState
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> 
  {
    f.debug_struct("For")
      // .field("row", &self.row)
      .finish()
  }
}

/// State for FOR loop (sorted case).
pub struct ForSortState
{
  pub ix: usize,
  pub rows: Vec<Vec<Value>>
}

impl Debug for ForSortState
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> 
  {
    f.debug_struct("ForSort")
      // .field("row", &self.row)
      .finish()
  }
}

/// Info for ForNext Inst.
pub struct ForNextInfo
{
  pub for_id: usize,
  pub assigns: Vec<usize>,
  pub exps: Vec<CExpPtr<Value>>,
  pub wher: Option<CExpPtr<bool>>
}

/// Get the default Value for a DataType.
pub fn default( t: DataType ) -> Value
{
  match data_kind(t)
  {
    DK::Bool => Value::Bool( false ),
    DK::Float => Value::Float(0.0),
    DK::String => Value::String( Rc::new( String::new() ) ),
    DK::Binary => Value::Binary( Rc::new( Vec::new() ) ),
    _ => Value::Int(0)
  }    
}

impl std::cmp::Ord for Value 
{
  fn cmp(&self, other: &Self) -> std::cmp::Ordering 
  {
    let mut result = std::cmp::Ordering::Equal;
    match self
    {
      Value::String(s1) => 
        if let Value::String(s2) = other
        {
          result = s1.cmp(s2);
        }
      Value::Int(x1) =>
        if let Value::Int(x2) = other
        {
          result = x1.cmp(x2);
        }  
      _ => { panic!() }
    }
    // println!( "cmp {:?} to {:?} result = {:?}", self, other, result );
    result
  }
}

impl PartialOrd for Value 
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> 
  {
    let mut result = std::cmp::Ordering::Equal;
    if let Value::String(s1) = self
    {
      if let Value::String(s2) = other
      {
        result = s1.cmp(s2);
      }
    }
    Some(result)
  }
}

impl PartialEq for Value 
{
  fn eq(&self, other: &Self) -> bool 
  {
    if let Some(eq) = self.partial_cmp( other ) 
    { 
      eq == std::cmp::Ordering::Equal 
    }
    else
    {
      false
    }
  }
}

impl Eq for Value
{
}

/// Compare table rows.
pub fn compare( a: &[Value], b: &[Value], desc: &[bool] ) -> Ordering
{
  let mut ix = 0;
  loop
  {
    let cmp = a[ix].cmp( &b[ix] );
    if cmp != Ordering::Equal
    {
      if !desc[ix] { return cmp };
      return if cmp == Ordering::Less { Ordering::Greater } else { Ordering::Less };
    }
    ix += 1;
    if ix == desc.len() { return Ordering::Equal; }
  }
}

/// Compiled Table Expression.
pub enum CTableExpression
{
  // Select( SelectExpression ),
  Base( TablePtr ),
  Values( Vec<Vec<CExpPtr<Value>>> )
}

/// Compiled Select Expression.
pub struct CSelectExpression
{
  pub colnames: Vec<String>,
  pub assigns: Vec<usize>, 
  pub exps: Vec<CExpPtr<Value>>, 
  pub from: Option<CTableExpression>,
  pub wher: Option<CExpPtr<bool>>,
  pub orderby: Vec<CExpPtr<Value>>,
  pub desc: Vec<bool>,
}

/// Database Operation
pub enum DO
{
  CreateTable( Rc<TableInfo> ),
  CreateIndex( IndexInfo ),
  CreateSchema( String ),
  CreateRoutine( ObjRef, Rc<String>, bool ),
  CreateView( ObjRef, bool, String ),
  AlterTable( ObjRef, Vec<AlterAction> ),
  RenameSchema( String, String ),
  Renasysble( ObjRef, ObjRef ),
  RenameView( ObjRef, ObjRef ),
  RenameProcedure( ObjRef, ObjRef ),
  RenameFunction( ObjRef, ObjRef ),
  DropSchema( String ),
  DropTable( ObjRef ),
  DropView( ObjRef ),
  DropIndex( ObjRef, String ),
  DropProcedure( ObjRef ),
  DropFunction( ObjRef ),
  Insert( TablePtr, Vec<usize>, CTableExpression ),
  Update( TablePtr, Vec<(usize,CExpPtr<Value>)>, CExpPtr<bool> ),
  Delete( TablePtr, CExpPtr<bool> )
}

/// Actions for altering columns of a table.
pub enum AlterAction
{
  Add( String, DataType ),
  Drop( String ),
  Rename( String, String ),
  Modify( String, DataType )
}

/// Function or Procedure body.
///
/// When a CREATE FUNCTION or CREATE ROUTINE statement is executed,
/// the Routine is inserted into the database, but the ilist is not 
/// created. The source has been parsed and checked for syntax correctness
/// but type checking is delayed until the first call to the Routine is compiled.
/// At that point type checking is performed and instructions are generated.
pub struct Routine
{
  pub param_count: usize,
  pub return_type: DataType,
  pub local_types: Vec<DataType>,
  pub source: Rc<String>,
  pub ilist: RefCell<Vec<Inst>>, // Valid when compiled is true.
  pub compiled: Cell<bool>,
}

impl Debug for Routine
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> 
  {
    f.debug_struct("Routine")
      // .field("compiled", &self.compiled)
      // .field("source", &self.source)
      .finish()
  }
}

/// ```Rc<Routine>```
pub type RoutinePtr = Rc<Routine>;

