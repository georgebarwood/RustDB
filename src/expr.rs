use crate::*;
use Instruction::{DataOp, ForNext, ForSortNext, Jump, JumpIfFalse};

/// Holds function name, line, column and message.
#[derive(Clone)]
pub(crate) struct SqlError {
    pub rname: String,
    pub line: usize,
    pub column: usize,
    pub msg: String,
}
/// Table Expression ( not yet type-checked or compiled against database ).
pub enum TableExpression {
    /// Base table.
    Base(ObjRef),
    /// VALUEs.
    Values(Vec<Vec<Expr>>),
}
/// Assign operation.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum AssignOp {
    /// Assign.
    Assign,
    /// append.
    Append,
    /// Increment.
    Inc,
    /// Decrement.
    Dec,
}
/// Vector of local variable numbers and AssignOp.
pub type Assigns = Vec<(usize, AssignOp)>;

/// From Expression ( not yet compiled ).
#[non_exhaustive]
pub struct FromExpression {
    /// Column names.
    pub colnames: Vec<String>,
    /// Assigns.
    pub assigns: Assigns,
    /// Expressions.
    pub exps: Vec<Expr>,
    /// FROM clause.
    pub from: Option<Box<TableExpression>>,
    /// WHERE expression.
    pub wher: Option<Expr>,
    /// ORDER BY clause.
    pub orderby: Vec<(Expr, bool)>,
}

/// Parsing token.
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum Token {
    /* Note: order is significant */
    /// Less.
    Less,
    /// Less or Equal.
    LessEqual,
    /// Greater or Equal.
    GreaterEqual,
    /// Greater.
    Greater,
    /// Equal.
    Equal,
    /// Not Equal.
    NotEqual,
    /// In.
    In,
    /// +
    Plus,
    /// -
    Minus,
    /// *
    Times,
    /// /
    Divide,
    /// %
    Percent,
    /// |
    VBar,
    /// AND
    And,
    /// OR
    Or,
    /// |=
    VBarEqual,
    /// +=
    PlusEqual,
    /// -=
    MinusEqual,
    /// Identifier.
    Id,
    /// Number.
    Number,
    /// Hex number.
    Hex,
    /// String literal.
    String,
    /// (
    LBra,
    /// )
    RBra,
    /// ,
    Comma,
    /// :
    Colon,
    /// .
    Dot,
    /// !
    Exclamation,
    /// Unknown.
    Unknown,
    /// End of file.
    EndOfFile,
}

impl Token {
    /// Get precedence of operator.
    pub fn precedence(self) -> i8 {
        const PA: [i8; 15] = [10, 10, 10, 10, 10, 10, 10, 20, 20, 30, 30, 30, 15, 8, 5];
        PA[self as usize]
    }
}

/// Scalar Expression (uncompiled).
#[non_exhaustive]
pub struct Expr {
    /// Expression kind.
    pub exp: ExprIs,
    /// Data type.
    pub data_type: DataType,
    /// Doesn't depend on FROM clause.
    pub is_constant: bool,
    /// Has been type-checked.
    pub checked: bool,
    /// Column number.
    pub col: usize,
}

impl Expr {
    /// Construct new Expr.
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
#[non_exhaustive]
pub enum ExprIs {
    /// Constant.
    Const(Value),
    /// Local variable.
    Local(usize),
    /// Column.
    ColName(String),
    /// Binary operator expression.
    Binary(Token, Box<Expr>, Box<Expr>),
    /// Not expression.
    Not(Box<Expr>),
    /// Unary minus.
    Minus(Box<Expr>),
    /// Case expression.
    Case(Vec<(Expr, Expr)>, Box<Expr>),
    /// Function call.
    FuncCall(ObjRef, Vec<Expr>),
    /// Builtin function call.
    BuiltinCall(String, Vec<Expr>),
    /// Scalar select.
    ScalarSelect(Box<FromExpression>),
    /// List of expressions.
    List(Vec<Expr>),
}

/// Object reference ( Schema.Name ).
#[derive(PartialEq, PartialOrd, Eq, Hash, Clone)]
#[non_exhaustive]
pub struct ObjRef {
    /// Schema.
    pub schema: String,
    /// Name within Schema.
    pub name: String,
}

impl ObjRef {
    /// Construct from string references.
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
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
#[non_exhaustive]
pub enum DataKind {
    /// None.
    None = 0,
    /// Binary.
    Binary = 1,
    /// String.
    String = 2,
    /// Integer.
    Int = 3,
    /// Float.
    Float = 4,
    /// Bool.
    Bool = 5,
}

/// Low 3 (KBITS) bits are DataKind, rest is size in bytes.
pub type DataType = usize;

pub(crate) const KBITS: usize = 3;
pub(crate) const NONE: DataType = DataKind::None as usize;
pub(crate) const BINARY: DataType = DataKind::Binary as usize + (16 << KBITS);
pub(crate) const STRING: DataType = DataKind::String as usize + (16 << KBITS);
pub(crate) const NAMESTR: DataType = DataKind::String as usize + (32 << KBITS);
pub(crate) const BIGSTR: DataType = DataKind::String as usize + (250 << KBITS);
pub(crate) const INT: DataType = DataKind::Int as usize + (8 << KBITS);
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
    x >> KBITS
}

/// Compilation block ( body of function or batch section ).
pub struct Block<'a> {
    /// Number of function parameters.
    pub param_count: usize,
    /// Function return type.
    pub return_type: DataType,
    /// Datatypes of paramaters and local variables.
    pub local_typ: Vec<DataType>,
    /// List of instructions.
    pub ilist: Vec<Instruction>,
    /// Id of break.
    pub break_id: usize,
    /// Database.
    pub db: DB,
    /// Current table in scope by FROM clause( or UPDATE statment ).
    pub from: Option<CTableExpression>,
    /// Only parse, no type checking or compilation.
    pub parse_only: bool,
    /// List of jumps.
    jumps: Vec<usize>,
    /// Lookup jump label by name.   
    labels: HashMap<&'a [u8], usize>,
    /// Lookup local variable by name.
    local_map: HashMap<&'a [u8], usize>,
    /// Names of local variables.
    locals: Vec<&'a [u8]>,
}

impl<'a> Block<'a> {
    /// Construct a new block.
    pub fn new(db: DB) -> Self {
        Block {
            ilist: Vec::new(),
            jumps: Vec::new(),
            labels: HashMap::default(),
            local_map: HashMap::default(),
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
                panic!("undefined label: {}", parse::tos(k));
            }
        }
        for i in &mut self.ilist {
            match i {
                JumpIfFalse(x, _) | Jump(x) | ForNext(x, _) | ForSortNext(x, _) => {
                    *x = self.jumps[*x]
                }
                _ => {}
            }
        }
    }

    /// Add an instruction to the instruction list.
    pub fn add(&mut self, s: Instruction) {
        if !self.parse_only {
            self.ilist.push(s);
        }
    }

    /// Add a Data Operation (DO) to the instruction list.
    pub fn dop(&mut self, dop: DO) {
        if !self.parse_only {
            self.add(DataOp(Box::new(dop)));
        }
    }

    /// Check the parameter kinds match the function.
    pub fn check_types(&self, r: &Rc<Function>, pkinds: &[DataKind]) {
        if pkinds.len() != r.param_count {
            panic!("param count mismatch");
        }
        for (i, pk) in pkinds.iter().enumerate() {
            let ft = data_kind(r.local_typ[i]);
            let et = *pk;
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
            panic!("duplicate variable name");
        }
        self.local_map.insert(name, local_id);
    }

    /// Get the number of a local variable from a name.
    pub fn get_local(&self, name: &[u8]) -> Option<&usize> {
        self.local_map.get(name)
    }

    /// Get the name of a local variable from a number.
    pub fn local_name(&self, num: usize) -> &[u8] {
        self.locals[num]
    }

    /// Get a local jump id.
    pub fn get_jump_id(&mut self) -> usize {
        let result = self.jumps.len();
        self.jumps.push(usize::MAX);
        result
    }

    /// Set instruction location of jump id.
    pub fn set_jump(&mut self, jump_id: usize) {
        self.jumps[jump_id] = self.ilist.len();
    }

    /// Get a local jump id to current location.
    pub fn get_loop_id(&mut self) -> usize {
        let result = self.get_jump_id();
        self.set_jump(result);
        result
    }

    /// Get a number for a local goto label.
    pub fn get_goto_label(&mut self, s: &'a [u8]) -> usize {
        if let Some(jump_id) = self.labels.get(s) {
            *jump_id
        } else {
            let jump_id = self.get_jump_id();
            self.labels.insert(s, jump_id);
            jump_id
        }
    }

    /// Set the local for a local goto lable.
    pub fn set_goto_label(&mut self, s: &'a [u8]) {
        if let Some(jump_id) = self.labels.get(s) {
            let j = *jump_id;
            if self.jumps[j] != usize::MAX {
                panic!("label already set");
            }
            self.set_jump(j);
        } else {
            let jump_id = self.get_loop_id();
            self.labels.insert(s, jump_id);
        }
    }

    /// Get the DataKind of an expression.
    pub fn kind(&self, e: &mut Expr) -> DataKind {
        compile::c_check(self, e);
        data_kind(e.data_type)
    }
}
