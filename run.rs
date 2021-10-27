use crate::*; 
use core::fmt::Debug;
 
/// Iterator that yields references to page data.
pub type DataSource = Box<dyn Iterator<Item = (PagePtr,usize)>>;

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
  ForSortNext(usize,Box<(usize,usize,Assigns)>),
  DataOp(Box<DO>),
  PushValue(CExpPtr<Value>),
  Call(FunctionPtr),
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
  pub(crate) data_source: DataSource
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
  pub(crate) ix: usize,
  pub(crate) rows: Vec<Vec<Value>>
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
  pub(crate) for_id: usize,
  pub(crate) assigns: Assigns,
  pub(crate) exps: Vec<CExpPtr<Value>>,
  pub(crate) wher: Option<CExpPtr<bool>>
}

/// Get the default Value for a DataType.
pub fn default( t: DataType ) -> Value
{
  match data_kind(t)
  {
    DataKind::Bool => Value::Bool( false ),
    DataKind::Float => Value::Float(0.0),
    DataKind::String => Value::String( Rc::new( String::new() ) ),
    DataKind::Binary => Value::Binary( Rc::new( Vec::new() ) ),
    _ => Value::Int(0)
  }    
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
  IdGet( TablePtr, CExpPtr<i64> ),
  IxGet( TablePtr, CExpPtr<Value>, usize ),
  Values( Vec<Vec<CExpPtr<Value>>> )
}

/// Compiled Select Expression.
pub struct CSelectExpression
{
  pub colnames: Vec<String>,
  pub(crate) assigns: Assigns, 
  pub(crate) exps: Vec<CExpPtr<Value>>, 
  pub(crate) from: Option<CTableExpression>,
  pub(crate) wher: Option<CExpPtr<bool>>,
  pub(crate) orderby: Vec<CExpPtr<Value>>,
  pub(crate) desc: Vec<bool>,
}

/// Database Operation
pub enum DO
{
  CreateTable( ColInfo ),
  CreateIndex( IndexInfo ),
  CreateSchema( String ),
  CreateFunction( ObjRef, Rc<String>, bool ),
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

/// Compiled Function.
///
/// When a CREATE FUNCTION statement is executed,
/// the Function is inserted into the database, but the ilist is not 
/// created. The source has been parsed and checked for syntax correctness
/// but type checking is delayed until the first call to the Function is compiled.
/// At that point type checking is performed and instructions are generated.
pub struct Function
{
  pub param_count: usize,
  pub return_type: DataType,
  pub local_typ: Vec<DataType>,
  pub source: Rc<String>,
  pub ilist: RefCell<Vec<Inst>>, // Valid when compiled is true.
  pub compiled: Cell<bool>,
}

impl Debug for Function
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> 
  {
    f.debug_struct("Function")
      // .field("compiled", &self.compiled)
      // .field("source", &self.source)
      .finish()
  }
}

/// ```Rc<Function>```
pub type FunctionPtr = Rc<Function>;

