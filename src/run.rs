use crate::alloc::{DBox, LBox, LRc, LString, LVec};
use crate::{
    Assigns, Block, Cell, ColInfo, DataType, EvalEnv, Expr, IndexInfo, ObjRef, PagePtr, RefCell,
    Table, Value, panic,
};

/// Instruction.
#[non_exhaustive]
pub enum Instruction {
    /// Push constant.
    PushConst(Value),
    /// Push expression.
    PushValue(CExpPtr<Value>),
    /// Push local variable.
    PushLocal(usize),
    /// Assign local variable.
    PopToLocal(usize),
    /// Jump.
    Jump(usize),
    /// Jump if false.
    JumpIfFalse(usize, CExpPtr<bool>),
    /// Call
    Call(LRc<Function>),
    /// Return from function.
    Return,
    /// Throw error.
    Throw,
    /// Execute string.
    Execute,
    /// Initialise FOR statement.
    ForInit(usize, LBox<CTableExpression>),
    /// Next iteration of FOR statement.
    ForNext(usize, LBox<ForNextInfo>),
    /// Initialise FOR statement ( sorted case ).
    ForSortInit(usize, LBox<CFromExpression>),
    /// Next iteration of FOR statement ( sorted case ).
    ForSortNext(usize, LBox<(usize, usize, Assigns)>),
    /// Data operation.
    DataOp(LBox<DO>),
    /// SELECT expression.
    Select(LBox<CFromExpression>),
    /// Set local variables from table.
    Set(LBox<CFromExpression>),
    // Special push instructions ( optimisations )
    /// Push Integer expression.
    PushInt(CExpPtr<i64>),
    /// Push Float expression.
    PushFloat(CExpPtr<f64>),
    /// Push bool expression.
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
    pub local_typ: LVec<DataType>,
    /// Source SQL.
    pub source: LRc<LString>,
    /// List of instructions.
    pub ilist: RefCell<LVec<Instruction>>, // Valid when compiled is true.
    /// Has function been compiled.
    pub compiled: Cell<bool>,
}

/// Compiled expression which yields type T when evaluated.
pub trait CExp<T> {
    /// Evaluate the compiled expression.
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> T;
}

/// Pointer to [CExp].
pub type CExpPtr<T> = DBox<dyn CExp<T>>;

/// Function that compiles a builtin function call.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum CompileFunc {
    /// Value result.
    Value(fn(&Block, &mut [Expr]) -> CExpPtr<Value>),
    /// Int result.
    Int(fn(&Block, &mut [Expr]) -> CExpPtr<i64>),
    /// Float result.
    Float(fn(&Block, &mut [Expr]) -> CExpPtr<f64>),
}

/// Iterator that yields references to page data.
pub type DataSource = DBox<dyn Iterator<Item = (PagePtr, usize)>>;

/// State for FOR loop (non-sorted case).
#[non_exhaustive]
pub struct ForState {
    /// Data source.
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
    /// Currrent index into rows.
    pub ix: usize,
    /// Rows.
    pub rows: LVec<LVec<Value>>,
}
impl std::fmt::Debug for ForSortState {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Ok(())
    }
}

/// Info for ForNext Inst.
#[non_exhaustive]
pub struct ForNextInfo {
    /// FOR id.
    pub for_id: usize,
    /// Assigns.
    pub assigns: Assigns,
    /// Expressions.
    pub exps: LVec<CExpPtr<Value>>,
    /// WHERE expression.
    pub wher: Option<CExpPtr<bool>>,
}

/// Compiled Table Expression.
#[non_exhaustive]
pub enum CTableExpression {
    /// Base table.
    Base(LRc<Table>),
    /// Row identified by Id.
    IdGet(LRc<Table>, CExpPtr<i64>),
    /// Indexed rows.
    IxGet(LRc<Table>, LVec<CExpPtr<Value>>, usize),
    /// VALUE expressions.
    Values(LVec<LVec<CExpPtr<Value>>>),
}

impl CTableExpression {
    /// Get underlying table.
    pub fn table(&self) -> LRc<Table> {
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
    /// Column names.
    pub colnames: LVec<LBox<str>>,
    /// Assignments ( left hand side ).
    pub assigns: Assigns,
    /// Expressions.
    pub exps: LVec<CExpPtr<Value>>,
    /// FROM expression.
    pub from: Option<CTableExpression>,
    /// WHERE expression.
    pub wher: Option<CExpPtr<bool>>,
    /// ORDER BY expressions.
    pub orderby: LVec<CExpPtr<Value>>,
    /// DESC bits.
    pub desc: LVec<bool>,
}

/// Database Operation
#[non_exhaustive]
pub enum DO {
    /// Create Schema.
    CreateSchema(LBox<str>),
    /// Create Table.
    CreateTable(ColInfo),
    /// Create Index.
    CreateIndex(IndexInfo),
    /// Create Function.
    CreateFunction(ObjRef, LRc<LString>, bool),
    /// Alter Table.
    AlterTable(ObjRef, LVec<AlterCol>),
    /// Drop Schema.
    DropSchema(LBox<str>),
    /// Drop Table.
    DropTable(ObjRef),
    /// Drop Index.
    DropIndex(ObjRef, LBox<str>),
    /// Drop Function.
    DropFunction(ObjRef),
    /// Insert into Table.
    Insert(LRc<Table>, LVec<usize>, CTableExpression),
    /// Update Table rows.
    Update(
        LVec<(usize, CExpPtr<Value>)>,
        CTableExpression,
        Option<CExpPtr<bool>>,
    ),
    /// Delete Table rows.
    Delete(CTableExpression, Option<CExpPtr<bool>>),
}

/// Actions for altering columns of a table.
#[non_exhaustive]
pub enum AlterCol {
    /// Add column.
    Add(LBox<str>, DataType),
    /// Drop column.
    Drop(LBox<str>),
    /// Modify column.
    Modify(LBox<str>, DataType),
}
