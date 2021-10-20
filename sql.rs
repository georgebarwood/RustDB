use std::{ collections::HashMap };
use crate::{ sqlparse, run::Inst, Value };

/// Holds routine name, line, column and message.
pub struct SqlError
{
  pub rname: String,
  pub line: usize,
  pub column: usize,
  pub msg: String,
}

/// Function call ( function name and parameter expressions ).
pub struct ExprCall
{
  pub name: ObjRef,
  pub parms: Vec<Expr>
}

/// Table Expression ( not yet type-checked or compiled against database ).
pub enum TableExpression
{
  // Select( SelectExpression ),
  Base( ObjRef ),
  Values( Vec<Vec<Expr>> )
}

/// Select Expression ( not yet compiled ).
pub struct SelectExpression
{
  pub colnames: Vec<String>,
  pub assigns: Vec<usize>, 
  pub exps: Vec<Expr>, 
  pub from: Option<Box<TableExpression>>,
  pub wher: Option<Expr>,
  pub orderby: Vec<(Expr,bool)>
}

/// Parsing token.
#[derive(Debug,PartialEq,PartialOrd,Clone,Copy)]
pub enum Token { /* Note: order is significant */
  Less, LessEqual, GreaterEqual, Greater, Equal, NotEqual, In,
  Plus, Minus, Times, Divide, Percent, VBar, And, Or,
  Id, Number, Decimal, Hex, String, LBra, RBra, Comma, Colon, Dot, Exclamation, Unknown, EndOfFile
}

pub(crate) const PRECEDENCE : [i8;15 ] = [ 10, 10, 10, 10, 10, 10, 10, 20, 20, 30, 30, 30, 15, 8, 5 ];

/// Scalar Expression ( uncompiled ).
pub enum Expr 
{
  // cf https://docs.rs/syn/0.15.44/syn/enum.Expr.html
  Local(usize),
  Number(i64),
  Const(Value),
  Binary( (Token,Box<Expr>,Box<Expr>) ),
  Not(Box<Expr>),
  Minus(Box<Expr>),  
  FuncCall(ExprCall),
  List(Vec<Expr>),
  Name(String),
  Case((Vec<(Expr,Expr)>,Box<Expr>)),
  ScalarSelect(Box<SelectExpression>),
  BuiltinCall(String,Vec<Expr>),
}

/// Object reference ( Schema.Name ).
#[derive(Debug,PartialEq, PartialOrd,Eq,Hash,Clone)]
pub struct ObjRef
{
  pub schema:String,
  pub name:String
}

impl ObjRef
{
  pub fn new( s: &str, n: &str ) -> Self
  {
    Self{ schema: s.to_string(), name: n.to_string() }
  }
  /// Used for error messages.
  pub fn to_str( &self ) -> String
  {
    "[".to_string() + &self.schema + "].[" + &self.name + "]"
  } 
}

/// Index information ( not yet in use ).
pub struct IndexInfo
{
  pub schema: String,
  pub tname: String,
  pub iname: String,
  pub cols: Vec<String>
}

/// Binary, String, Int, Float, Bool, Decimal.
#[derive(Debug,PartialEq,PartialOrd,Clone,Copy)]
pub enum DK { None=0, Binary=1, String=2, Int=3, Float=4, Bool=5, Decimal=6 }

/// Low 3 bits are DK, next 5 bits are size in bytes, or p ( for DECIMAL ).
pub type DataType = usize;

const KBITS : usize = 3;

pub(crate) const NONE : DataType = DK::None as usize;
pub(crate) const BINARY : DataType = DK::Binary as usize + ( 8 << KBITS );
pub(crate) const STRING : DataType = DK::String as usize + ( 8 << KBITS );
pub(crate) const BIGINT : DataType = DK::Int as usize + ( 8 << KBITS );
pub(crate) const INT    : DataType = DK::Int as usize + ( 4 << KBITS );
pub(crate) const SMALLINT : DataType = DK::Int as usize + ( 2 << KBITS );
pub(crate) const TINYINT : DataType = DK::Int as usize + ( 1 << KBITS );
pub(crate) const FLOAT : DataType = DK::Float as usize + ( 4 << KBITS );
pub(crate) const DOUBLE : DataType = DK::Float as usize + ( 8 << KBITS );
pub(crate) const BOOL : DataType = DK::Bool as usize + ( 1 << KBITS );
pub(crate) const DECIMAL : DataType = DK::Decimal as usize;


const DKLOOK : [DK;7] = [ DK::None, DK::Binary, DK::String, DK::Int, DK::Float, DK::Bool, DK::Decimal ];

/// Compute the DataKind(DK) of a DataType.
pub fn data_kind( x: DataType ) -> DK
{
  DKLOOK[ x % ( 1 << KBITS ) ]
}

/// Number of bytes needed to store a Decimal of index digits.
const DECSIZE : [u8;19] = [ 0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 7, 8, 8 ];

/// Compute the number of bytes required to store a value of the specified DataType.
pub fn data_size( x:DataType ) -> usize
{
  let p = ( x >> KBITS ) & 31;
  if data_kind( x ) == DK::Decimal
  {
    DECSIZE[p] as usize
  }
  else
  {
    p
  }    
}

/// Compilation block ( body of routine or batch section ).
pub(crate) struct Block <'a>
{
  pub param_count: usize,
  pub return_type: DataType,
  pub local_types: Vec<DataType>,
  pub ilist: Vec<Inst>,

  pub jumps: Vec<usize>,
  pub labels: HashMap<&'a [u8],usize>,
  pub local_map: HashMap<&'a [u8],usize>,
  pub locals: Vec<&'a[u8]>,
  pub break_id: usize,
}

impl <'a> Block <'a>
{ 
  /// Construct a new block.
  pub fn new() -> Self
  {
    Block
    {
      ilist: Vec::new(),
      jumps: Vec::new(),
      labels: HashMap::new(),
      local_map: HashMap::new(),
      locals: Vec::new(),
      local_types: Vec::new(),
      break_id: 0,
      param_count: 0,
      return_type: NONE,
    }
  }

  /// Patch check labels are all defined and patch jump instructions. 
  pub fn resolve_jumps( &mut self )
  {
    for (k,v) in &self.labels
    {
      if self.jumps[*v] == usize::MAX 
      {
        panic!( "Undefined label: {}", sqlparse::tos(k) );
      }
    }

    for i in &mut self.ilist
    {
      match i
      {
        Inst::JumpIfFalse( x, _e ) => *x = self.jumps[*x],
        Inst::Jump( x ) => *x = self.jumps[*x],
        Inst::ForNext( x, _y ) => *x = self.jumps[*x],
        Inst::ForSortNext( x, _y ) => *x = self.jumps[*x],
        _ => {}
      }
    }
  }
}
