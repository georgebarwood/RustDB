use crate::{
    panic, Assigns, Block, Cell, ColInfo, DataType, EvalEnv, Expr, IndexInfo, ObjRef, PagePtr, Rc,
    RefCell, Table, Value,
};

/// Instruction.
#[non_exhaustive]
pub enum Instruction {
    ///
    PushConst(Value),
    ///
    PushValue(CExpPtr<Value>),
    ///
    PushLocal(usize),
    ///
    PopToLocal(usize),
    ///
    Jump(usize),
    ///
    JumpIfFalse(usize, CExpPtr<bool>),
    ///
    Call(Rc<Function>),
    ///
    Return,
    ///
    Throw,
    ///
    Execute,
    ///
    ForInit(usize, Box<CTableExpression>),
    ///
    ForNext(usize, Box<ForNextInfo>),
    ///
    ForSortInit(usize, Box<CFromExpression>),
    ///
    ForSortNext(usize, Box<(usize, usize, Assigns)>),
    ///
    DataOp(Box<DO>),
    ///
    Select(Box<CFromExpression>),
    ///
    Set(Box<CFromExpression>),
    // Special push instructions ( optimisations )
    ///
    PushInt(CExpPtr<i64>),
    ///
    PushFloat(CExpPtr<f64>),
    ///
    PushBool(CExpPtr<bool>),
    // More optimisations.
    /// Assign a local variable.
    AssignLocal(usize, CExpPtr<Value>),
    /// Append to a local variable.
    AppendLocal(usize, CExpPtr<Value>),
    /// Increment (+=) a local variable.
    IncLocal(usize, CExpPtr<Value>),
    /// Decrement (-=) a local variable.
    DecLocal(usize, CExpPtr<Value>),
}

/// Compiled Function.
#[non_exhaustive]
pub struct Function {
    /// Number of parameters.
    pub param_count: usize,
    /// Function return type.
    pub return_type: DataType,
    /// Types of local parameters/variables.
    pub local_typ: Vec<DataType>,
    /// Source SQL.
    pub source: Rc<String>,
    /// List of instructions.
    pub ilist: RefCell<Vec<Instruction>>, // Valid when compiled is true.
    /// Has function been compiled.
    pub compiled: Cell<bool>,
}

/// Compiled expression which yields type T when evaluated.
pub trait CExp<T> {
    /// Evaluate the compiled expression.
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> T;
}

/// Pointer to [CExp].
pub type CExpPtr<T> = Box<dyn CExp<T>>;

/// Function that compiles a builtin function call.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum CompileFunc {
    ///
    Value(fn(&Block, &mut [Expr]) -> CExpPtr<Value>),
    ///
    Int(fn(&Block, &mut [Expr]) -> CExpPtr<i64>),
    ///
    Float(fn(&Block, &mut [Expr]) -> CExpPtr<f64>),
}

/// Iterator that yields references to page data.
pub type DataSource = Box<dyn Iterator<Item = (PagePtr, usize)>>;

/// State for FOR loop (non-sorted case).
#[non_exhaustive]
pub struct ForState {
    ///
    pub data_source: DataSource,
}
impl std::fmt::Debug for ForState {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Ok(())
    }
}

/// State for FOR loop (sorted case).
#[non_exhaustive]
pub struct ForSortState {
    ///
    pub ix: usize,
    ///
    pub rows: Vec<Vec<Value>>,
}
impl std::fmt::Debug for ForSortState {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Ok(())
    }
}

/// Info for ForNext Inst.
#[non_exhaustive]
pub struct ForNextInfo {
    ///
    pub for_id: usize,
    ///
    pub assigns: Assigns,
    ///
    pub exps: Vec<CExpPtr<Value>>,
    ///
    pub wher: Option<CExpPtr<bool>>,
}

/// Compiled Table Expression.
#[non_exhaustive]
pub enum CTableExpression {
    // Select( FromExpression ),
    ///
    Base(Rc<Table>),
    ///
    IdGet(Rc<Table>, CExpPtr<i64>),
    ///
    IxGet(Rc<Table>, Vec<CExpPtr<Value>>, usize),
    ///
    Values(Vec<Vec<CExpPtr<Value>>>),
}

impl CTableExpression {
    ///
    pub fn table(&self) -> Rc<Table> {
        match self {
            CTableExpression::Base(t) => t.clone(),
            CTableExpression::IdGet(t, _) => t.clone(),
            CTableExpression::IxGet(t, _, _) => t.clone(),
            _ => panic!(),
        }
    }
}

/// Compiled From Expression.
#[non_exhaustive]
pub struct CFromExpression {
    ///
    pub colnames: Vec<String>,
    ///
    pub assigns: Assigns,
    ///
    pub exps: Vec<CExpPtr<Value>>,
    ///
    pub from: Option<CTableExpression>,
    ///
    pub wher: Option<CExpPtr<bool>>,
    ///
    pub orderby: Vec<CExpPtr<Value>>,
    ///
    pub desc: Vec<bool>,
}

/// Database Operation
#[non_exhaustive]
pub enum DO {
    ///
    CreateTable(ColInfo),
    ///
    CreateIndex(IndexInfo),
    ///
    CreateSchema(String),
    ///
    CreateFunction(ObjRef, Rc<String>, bool),
    ///
    AlterTable(ObjRef, Vec<AlterCol>),
    ///
    DropSchema(String),
    ///
    DropTable(ObjRef),
    ///
    DropIndex(ObjRef, String),
    ///
    DropFunction(ObjRef),
    ///
    Insert(Rc<Table>, Vec<usize>, CTableExpression),
    ///
    Update(
        Vec<(usize, CExpPtr<Value>)>,
        CTableExpression,
        Option<CExpPtr<bool>>,
    ),
    ///
    Delete(CTableExpression, Option<CExpPtr<bool>>),
}

/// Actions for altering columns of a table.
#[non_exhaustive]
pub enum AlterCol {
    ///
    Add(String, DataType),
    ///
    Drop(String),
    ///
    Modify(String, DataType),
}
