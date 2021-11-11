use crate::*;

/// Instruction.
pub enum Instruction {
    PushConst(Value),
    PushValue(CExpPtr<Value>),
    PushLocal(usize),
    PopToLocal(usize),
    Jump(usize),
    JumpIfFalse(usize, CExpPtr<bool>),
    Call(FunctionPtr),
    Return,
    Throw,
    Execute,
    ForInit(usize, Box<CTableExpression>),
    ForNext(usize, Box<ForNextInfo>),
    ForSortInit(usize, Box<CSelectExpression>),
    ForSortNext(usize, Box<(usize, usize, Assigns)>),
    DataOp(Box<DO>),
    Select(Box<CSelectExpression>),
    Set(Box<CSelectExpression>),
    // Special push instructions ( optimisations )
    PushInt(CExpPtr<i64>),
    PushFloat(CExpPtr<f64>),
    PushBool(CExpPtr<bool>),
}

/// Compiled expression which yields type T when evaluated.
pub trait CExp<T> {
    fn eval(&self, ee: &mut EvalEnv, data: &[u8]) -> T;
}
/// Pointer to CExp.
pub type CExpPtr<T> = Box<dyn CExp<T>>;

/// Function that compiles a builtin function call ( see Database::register ).
#[derive(Clone, Copy)]
pub enum CompileFunc {
    Value(fn(&Block, &mut [Expr]) -> CExpPtr<Value>),
    Int(fn(&Block, &mut [Expr]) -> CExpPtr<i64>),
    Float(fn(&Block, &mut [Expr]) -> CExpPtr<f64>),
}

/// Iterator that yields references to page data.
pub type DataSource = Box<dyn Iterator<Item = (PagePtr, usize)>>;

/// State for FOR loop (non-sorted case).
pub struct ForState {
    pub data_source: DataSource,
}
/// State for FOR loop (sorted case).
pub struct ForSortState {
    pub ix: usize,
    pub rows: Vec<Vec<Value>>,
}
/// Info for ForNext Inst.
pub struct ForNextInfo {
    pub for_id: usize,
    pub assigns: Assigns,
    pub exps: Vec<CExpPtr<Value>>,
    pub wher: Option<CExpPtr<bool>>,
}
/// Compiled Table Expression.
pub enum CTableExpression {
    // Select( SelectExpression ),
    Base(TablePtr),
    IdGet(TablePtr, CExpPtr<i64>),
    IxGet(TablePtr, Vec<CExpPtr<Value>>, usize),
    Values(Vec<Vec<CExpPtr<Value>>>),
}
impl CTableExpression {
    pub fn table(&self) -> TablePtr {
        match self {
            CTableExpression::Base(t) => t.clone(),
            CTableExpression::IdGet(t, _) => t.clone(),
            CTableExpression::IxGet(t, _, _) => t.clone(),
            _ => panic!(),
        }
    }
}
/// Compiled Select Expression.
pub struct CSelectExpression {
    pub colnames: Vec<String>,
    pub assigns: Assigns,
    pub exps: Vec<CExpPtr<Value>>,
    pub from: Option<CTableExpression>,
    pub wher: Option<CExpPtr<bool>>,
    pub orderby: Vec<CExpPtr<Value>>,
    pub desc: Vec<bool>,
}
/// Database Operation
pub enum DO {
    CreateTable(ColInfo),
    CreateIndex(IndexInfo),
    CreateSchema(String),
    CreateFunction(ObjRef, Rc<String>, bool),
    CreateView(ObjRef, bool, String),
    AlterTable(ObjRef, Vec<AlterAction>),
    RenameSchema(String, String),
    RenameTsble(ObjRef, ObjRef),
    RenameView(ObjRef, ObjRef),
    RenameFunction(ObjRef, ObjRef),
    DropSchema(String),
    DropTable(ObjRef),
    DropView(ObjRef),
    DropIndex(ObjRef, String),
    DropFunction(ObjRef),
    Insert(TablePtr, Vec<usize>, CTableExpression),
    Update(
        Vec<(usize, CExpPtr<Value>)>,
        CTableExpression,
        Option<CExpPtr<bool>>,
    ),
    Delete(CTableExpression, Option<CExpPtr<bool>>),
}
/// Actions for altering columns of a table.
pub enum AlterAction {
    Add(String, DataType),
    Drop(String),
    Rename(String, String),
    Modify(String, DataType),
}
/// Compiled Function.
pub struct Function {
    pub param_count: usize,
    pub return_type: DataType,
    pub local_typ: Vec<DataType>,
    pub source: Rc<String>,
    pub ilist: RefCell<Vec<Instruction>>, // Valid when compiled is true.
    pub compiled: Cell<bool>,
}
/// ```Rc<Function>```
pub type FunctionPtr = Rc<Function>;
