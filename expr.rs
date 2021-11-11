use crate::*;
use Instruction::*;

/// Holds function name, line, column and message.
pub struct SqlError {
    pub rname: String,
    pub line: usize,
    pub column: usize,
    pub msg: String,
}
/// Table Expression ( not yet type-checked or compiled against database ).
pub enum TableExpression {
    // Select( SelectExpression ),
    Base(ObjRef),
    Values(Vec<Vec<Expr>>),
}
/// Assign or Append.
#[derive(Clone, Copy)]
pub enum AssignOp {
    Assign,
    Append,
}
/// Vector of local variable numbers and AssignOp( assign or append ).
pub type Assigns = Vec<(usize, AssignOp)>;
/// Select Expression ( not yet compiled ).
pub struct SelectExpression {
    pub colnames: Vec<String>,
    pub assigns: Assigns,
    pub exps: Vec<Expr>,
    pub from: Option<Box<TableExpression>>,
    pub wher: Option<Expr>,
    pub orderby: Vec<(Expr, bool)>,
}
/// Parsing token.
#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub enum Token {
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
impl Token {
    pub fn precedence(self) -> i8 {
        const PA: [i8; 15] = [10, 10, 10, 10, 10, 10, 10, 20, 20, 30, 30, 30, 15, 8, 5];
        PA[self as usize]
    }
}
/// Scalar Expression (uncompiled).
pub struct Expr {
    pub exp: ExprIs,
    pub data_type: DataType,
    pub is_constant: bool, // Doesn't depend on FROM clause
    pub checked: bool,
    pub col: usize,
}
impl Expr {
    pub fn new(exp: ExprIs) -> Self {
        Expr {
            exp,
            data_type: NONE,
            is_constant: false,
            checked: false,
            col: 0,
        }
    }
}
/// Scalar Expression variants.
pub enum ExprIs {
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
#[derive(PartialEq, PartialOrd, Eq, Hash, Clone)]
pub struct ObjRef {
    pub schema: String,
    pub name: String,
}
impl ObjRef {
    pub fn new(s: &str, n: &str) -> Self {
        Self {
            schema: s.to_string(),
            name: n.to_string(),
        }
    }
    /// Used for error messages.
    pub fn str(&self) -> String {
        format!("[{}].[{}]", &self.schema, &self.name)
    }
}
/// Binary=1, String=2, Int=3, Float=4, Bool=5.
#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub enum DataKind {
    None = 0,
    Binary = 1,
    String = 2,
    Int = 3,
    Float = 4,
    Bool = 5,
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
/// Compute the DataKind of a DataType.
pub fn data_kind(x: DataType) -> DataKind {
    const DKLOOK: [DataKind; 6] = [
        DataKind::None,
        DataKind::Binary,
        DataKind::String,
        DataKind::Int,
        DataKind::Float,
        DataKind::Bool,
    ];
    DKLOOK[x % (1 << KBITS)]
}
/// Compute the number of bytes required to store a value of the specified DataType.
#[must_use]
pub fn data_size(x: DataType) -> usize {
    (x >> KBITS) & 31
}
/// Compilation block ( body of function or batch section ).
pub struct Block<'a> {
    pub param_count: usize,
    pub return_type: DataType,
    pub local_typ: Vec<DataType>,
    pub ilist: Vec<Instruction>,

    jumps: Vec<usize>,
    labels: HashMap<&'a [u8], usize>,
    local_map: HashMap<&'a [u8], usize>,
    pub locals: Vec<&'a [u8]>,
    pub break_id: usize,
    /// Database.
    pub(crate) db: DB,
    /// Current table in scope by FROM clause( or UPDATE statment ).
    pub(crate) from: Option<CTableExpression>,
    pub(crate) parse_only: bool,
}
impl<'a> Block<'a> {
    /// Construct a new block.
    pub fn new(db: DB) -> Self {
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
            from: None,
            db,
            parse_only: false,
        }
    }
    /// Check labels are all defined and patch jump instructions.
    pub fn resolve_jumps(&mut self) {
        for (k, v) in &self.labels {
            if self.jumps[*v] == usize::MAX {
                panic!("Undefined label: {}", parse::tos(k));
            }
        }
        for i in &mut self.ilist {
            match i {
                JumpIfFalse(x, _) => *x = self.jumps[*x],
                Jump(x) => *x = self.jumps[*x],
                ForNext(x, _) => *x = self.jumps[*x],
                ForSortNext(x, _) => *x = self.jumps[*x],
                _ => {}
            }
        }
    }
    /// Add an instruction to the instruction list.
    pub(crate) fn add(&mut self, s: Instruction) {
        if !self.parse_only {
            self.ilist.push(s);
        }
    }
    /// Add a Data Operation (DO) to the instruction list.
    pub(crate) fn dop(&mut self, dop: DO) {
        if !self.parse_only {
            self.add(DataOp(Box::new(dop)));
        }
    }
    pub(crate) fn check_types(&self, r: &FunctionPtr, ptypes: &[DataType]) {
        if ptypes.len() != r.param_count {
            panic!("param count mismatch");
        }
        for (i, pt) in ptypes.iter().enumerate() {
            let ft = data_kind(r.local_typ[i]);
            let et = data_kind(*pt);
            if ft != et {
                panic!("param type mismatch expected {:?} got {:?}", ft, et);
            }
        }
    }
    // Helper functions for other statements.
    /// Define a local variable ( parameter or declared ).
    pub fn def_local(&mut self, name: &'a [u8], dt: DataType) {
        let local_id = self.local_typ.len();
        self.local_typ.push(dt);
        self.locals.push(name);
        if self.local_map.contains_key(name) {
            panic!("Duplicate variable name");
        }
        self.local_map.insert(name, local_id);
    }
    pub fn get_local(&self, name: &[u8]) -> Option<&usize> {
        self.local_map.get(name)
    }
    pub fn get_jump_id(&mut self) -> usize {
        let result = self.jumps.len();
        self.jumps.push(usize::MAX);
        result
    }
    pub fn set_jump(&mut self, jump_id: usize) {
        self.jumps[jump_id] = self.ilist.len();
    }
    pub fn get_loop_id(&mut self) -> usize {
        let result = self.get_jump_id();
        self.set_jump(result);
        result
    }
    pub fn get_goto_label(&mut self, s: &'a [u8]) -> usize {
        if let Some(jump_id) = self.labels.get(s) {
            *jump_id
        } else {
            let jump_id = self.get_jump_id();
            self.labels.insert(s, jump_id);
            jump_id
        }
    }
    pub fn set_goto_label(&mut self, s: &'a [u8]) {
        if let Some(jump_id) = self.labels.get(s) {
            let j = *jump_id;
            if self.jumps[j] != usize::MAX {
                panic!("Label already set");
            }
            self.set_jump(j);
        } else {
            let jump_id = self.get_loop_id();
            self.labels.insert(s, jump_id);
        }
    }
}
