use crate::*;
use Instruction::*;

/// Holds function name, line, column and message.
#[derive(Clone)]
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

/// Low 3 (=[KBITS]) bits are DataKind, rest is size in bytes.
pub type DataType = usize;

pub const KBITS: usize = 3;
pub const NONE: DataType = DataKind::None as usize;
pub const BINARY: DataType = DataKind::Binary as usize + (16 << KBITS);
pub const STRING: DataType = DataKind::String as usize + (16 << KBITS);
pub const BIGSTR: DataType = DataKind::String as usize + (250 << KBITS);
pub const INT: DataType = DataKind::Int as usize + (8 << KBITS);
pub const INT4: DataType = DataKind::Int as usize + (4 << KBITS);
pub const INT2: DataType = DataKind::Int as usize + (2 << KBITS);
pub const INT1: DataType = DataKind::Int as usize + (1 << KBITS);
pub const FLOAT: DataType = DataKind::Float as usize + (4 << KBITS);
pub const DOUBLE: DataType = DataKind::Float as usize + (8 << KBITS);
pub const BOOL: DataType = DataKind::Bool as usize + (1 << KBITS);

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
    pub break_id: usize,
    /// Database.
    pub db: DB,
    /// Current table in scope by FROM clause( or UPDATE statment ).
    pub from: Option<CTableExpression>,
    pub parse_only: bool,
    jumps: Vec<usize>,
    labels: HashMap<&'a [u8], usize>,
    local_map: HashMap<&'a [u8], usize>,
    locals: Vec<&'a [u8]>,
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

    pub fn check_types(&self, r: &FunctionPtr, pkinds: &[DataKind]) {
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
            panic!("Duplicate variable name");
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
                panic!("Label already set");
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
