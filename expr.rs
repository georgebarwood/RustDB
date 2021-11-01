use crate::*;

/// Holds function name, line, column and message.
pub struct SqlError
{
  pub rname: String,
  pub line: usize,
  pub column: usize,
  pub msg: String,
}

/// Table Expression ( not yet type-checked or compiled against database ).
// #[derive(Debug)]
pub enum TableExpression
{
  // Select( SelectExpression ),
  Base(ObjRef),
  Values(Vec<Vec<Expr>>),
}

/// Assign or Append.
#[derive(Clone, Copy, Debug)]
pub enum AssignOp
{
  Assign,
  Append,
}

/// Vector of local variable numbers and AssignOp( assign or append ).
pub type Assigns = Vec<(usize, AssignOp)>;

/// Select Expression ( not yet compiled ).
// #[derive(Debug)]
pub struct SelectExpression
{
  pub colnames: Vec<String>,
  pub assigns: Assigns,
  pub exps: Vec<Expr>,
  pub from: Option<Box<TableExpression>>,
  pub wher: Option<Expr>,
  pub orderby: Vec<(Expr, bool)>,
}

/// Parsing token.
#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub enum Token
{
  /* Note: order is significant */
  Less,
  LessEqual,
  GreaterEqual,
  Greater,
  Equal,
  NotEqual,
  In,
  Plus,
  Minus,
  Times,
  Divide,
  Percent,
  VBar,
  And,
  Or,
  VBarEqual,
  Id,
  Number,
  Decimal,
  Hex,
  String,
  LBra,
  RBra,
  Comma,
  Colon,
  Dot,
  Exclamation,
  Unknown,
  EndOfFile,
}

impl Token
{
  pub fn precedence(self) -> i8
  {
    const PA: [i8; 15] = [10, 10, 10, 10, 10, 10, 10, 20, 20, 30, 30, 30, 15, 8, 5];
    PA[self as usize]
  }
}

/// Scalar Expression (uncompiled).
// #[derive(Debug)]
pub struct Expr
{
  pub exp: ExprIs,
  pub data_type: DataType,
  pub is_constant: bool, // Doesn't depend on FROM clause
  pub checked: bool,
  pub col: usize,
}

impl Expr
{
  pub fn new(exp: ExprIs) -> Self
  {
    Expr { exp, data_type: NONE, is_constant: false, checked: false, col: 0 }
  }
}

/// Scalar Expression variants.
// #[derive(Debug)]
pub enum ExprIs
{
  Const(Value),
  Local(usize),
  ColName(String),
  Binary(Token, Box<Expr>, Box<Expr>),
  Not(Box<Expr>),
  Minus(Box<Expr>),
  Case(Vec<(Expr, Expr)>, Box<Expr>),
  FuncCall(ObjRef, Vec<Expr>),
  BuiltinCall(String, Vec<Expr>),
  ScalarSelect(Box<SelectExpression>),
  List(Vec<Expr>),
}

/// Object reference ( Schema.Name ).
// #[derive(Debug)
#[derive(PartialEq, PartialOrd, Eq, Hash, Clone)]
pub struct ObjRef
{
  pub schema: String,
  pub name: String,
}

impl ObjRef
{
  pub fn new(s: &str, n: &str) -> Self
  {
    Self { schema: s.to_string(), name: n.to_string() }
  }
  /// Used for error messages.
  pub fn to_str(&self) -> String
  {
    format!("[{}].[{}]", &self.schema, &self.name)
  }
}

/// Binary=1, String=2, Int=3, Float=4, Bool=5, Decimal=6.
#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub enum DataKind
{
  None = 0,
  Binary = 1,
  String = 2,
  Int = 3,
  Float = 4,
  Bool = 5,
  Decimal = 6,
}

/// Low 3 (=KBITS) bits are DataKind, next 5 bits are size in bytes, or p ( for DECIMAL ).
pub type DataType = usize;

const KBITS: usize = 3;

pub(crate) const NONE: DataType = DataKind::None as usize;
pub(crate) const BINARY: DataType = DataKind::Binary as usize + (16 << KBITS);
pub(crate) const STRING: DataType = DataKind::String as usize + (16 << KBITS);
pub(crate) const BIGINT: DataType = DataKind::Int as usize + (8 << KBITS);
pub(crate) const INT: DataType = DataKind::Int as usize + (4 << KBITS);
pub(crate) const SMALLINT: DataType = DataKind::Int as usize + (2 << KBITS);
pub(crate) const TINYINT: DataType = DataKind::Int as usize + (1 << KBITS);
pub(crate) const FLOAT: DataType = DataKind::Float as usize + (4 << KBITS);
pub(crate) const DOUBLE: DataType = DataKind::Float as usize + (8 << KBITS);
pub(crate) const BOOL: DataType = DataKind::Bool as usize + (1 << KBITS);
pub(crate) const DECIMAL: DataType = DataKind::Decimal as usize;

/// Compute the DataKind of a DataType.
pub fn data_kind(x: DataType) -> DataKind
{
  const DKLOOK: [DataKind; 7] = [
    DataKind::None,
    DataKind::Binary,
    DataKind::String,
    DataKind::Int,
    DataKind::Float,
    DataKind::Bool,
    DataKind::Decimal,
  ];
  DKLOOK[x % (1 << KBITS)]
}

/// Compute the number of bytes required to store a value of the specified DataType.
#[must_use]
pub fn data_size(x: DataType) -> usize
{
  let p = (x >> KBITS) & 31;
  if data_kind(x) == DataKind::Decimal
  {
    /// Number of bytes needed to store a Decimal of index digits.
    const DECSIZE: [u8; 19] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 7, 8, 8];
    DECSIZE[p] as usize
  }
  else
  {
    p
  }
}

/// Compilation block ( body of function or batch section ).
pub(crate) struct Block<'a>
{
  pub param_count: usize,
  pub return_type: DataType,
  pub local_typ: Vec<DataType>,
  pub ilist: Vec<Inst>,

  pub jumps: Vec<usize>,
  pub labels: HashMap<&'a [u8], usize>,
  pub local_map: HashMap<&'a [u8], usize>,
  pub locals: Vec<&'a [u8]>,
  pub break_id: usize,
}

impl<'a> Block<'a>
{
  /// Construct a new block.
  pub fn new() -> Self
  {
    Block {
      ilist: Vec::new(),
      jumps: Vec::new(),
      labels: HashMap::new(),
      local_map: HashMap::new(),
      locals: Vec::new(),
      local_typ: Vec::new(),
      break_id: 0,
      param_count: 0,
      return_type: NONE,
    }
  }

  /// Check labels are all defined and patch jump instructions.
  pub fn resolve_jumps(&mut self)
  {
    for (k, v) in &self.labels
    {
      if self.jumps[*v] == usize::MAX
      {
        panic!("Undefined label: {}", parse::tos(k));
      }
    }

    for i in &mut self.ilist
    {
      match i
      {
        Inst::JumpIfFalse(x, _e) => *x = self.jumps[*x],
        Inst::Jump(x) => *x = self.jumps[*x],
        Inst::ForNext(x, _y) => *x = self.jumps[*x],
        Inst::ForSortNext(x, _y) => *x = self.jumps[*x],
        _ =>
        {}
      }
    }
  }
}
