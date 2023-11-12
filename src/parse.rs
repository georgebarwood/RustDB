use crate::*;
use compile::*;
use std::{mem, str};
use Instruction::*;

/// SQL parser.
///
/// Name convention for methods:
///
/// s_ parses a statement.
///
/// exp_ parses an expression.
pub struct Parser<'a> {
    /// Block information - local labels, jumps, instructions etc.
    pub b: Block<'a>,
    /// Name of function being compiled ( None if batch ).
    pub function_name: Option<&'a ObjRef>,
    /// Source SQL.
    source: &'a [u8],
    /// Index into source.
    source_ix: usize,
    /// Current input byte (char).
    cc: u8,
    /// Current token.
    token: Token,
    /// Source index of start of current token.
    token_start: usize,
    /// Source index of start of current token (including spacce).
    token_space_start: usize,
    /// source slice for current token ( but string literals are in ts )
    cs: &'a [u8],
    /// String literal.
    ts: String,
    source_column: usize,
    source_line: usize,
    decimal_int: i64,
    /// May be able to get rid of this.
    prev_source_column: usize,
    prev_source_line: usize,
}

impl<'a> Parser<'a> {
    /// Construct a new parser.
    pub fn new(src: &'a str, db: &DB) -> Self {
        let source = src.as_bytes();
        let mut result = Self {
            source,
            function_name: None,
            source_ix: 0,
            cc: 0,
            token_start: 0,
            token_space_start: 0,
            token: Token::EndOfFile,
            cs: source,
            ts: String::new(),
            source_column: 1,
            source_line: 1,
            prev_source_column: 1,
            prev_source_line: 1,
            decimal_int: 0,
            b: Block::new(db.clone()),
        };
        result.read_char();
        result.read_token();
        result
    }

    /// Parse a single statement.
    fn statement(&mut self) {
        if self.token == Token::Id {
            let id = self.cs;
            self.read_token();
            if self.test(Token::Colon) {
                self.b.set_goto_label(id);
            } else {
                match id {
                    b"ALTER" => self.s_alter(),
                    b"BEGIN" => self.s_begin(),
                    b"BREAK" => self.s_break(),
                    b"CREATE" => self.s_create(),
                    b"DROP" => self.s_drop(),
                    b"DECLARE" => self.s_declare(),
                    b"DELETE" => self.s_delete(),
                    b"EXEC" => self.s_exec(),
                    b"CHECK" => self.s_check(),
                    b"EXECUTE" => self.s_execute(),
                    b"FOR" => self.s_for(),
                    b"GOTO" => self.s_goto(),
                    b"IF" => self.s_if(),
                    b"INSERT" => self.s_insert(),
                    b"RETURN" => self.s_return(),
                    b"SELECT" => self.s_select(),
                    b"SET" => self.s_set(),
                    b"THROW" => self.s_throw(),
                    b"UPDATE" => self.s_update(),
                    b"WHILE" => self.s_while(),
                    _ => panic!("statement keyword expected, got '{}'", tos(id)),
                }
            }
        } else {
            panic!("statement keyword expected, got '{:?}'", self.token)
        }
    } // end fn statement

    /// Parse and execute a batch of statements.
    pub fn batch(&mut self, rs: &mut dyn Transaction) {
        loop {
            while self.token != Token::EndOfFile && !self.test_id(b"GO") {
                self.statement();
            }
            self.b.resolve_jumps();
            let mut ee = EvalEnv::new(self.b.db.clone(), rs);
            // let start = std::time::Instant::now();
            ee.alloc_locals(&self.b.local_typ, 0);
            ee.go(&self.b.ilist);
            if self.token == Token::EndOfFile {
                break;
            }
            self.b = Block::new(self.b.db.clone());
        }
    }

    /// Parse the definition of a function.
    pub fn parse_function(&mut self) {
        self.read(Token::LBra);
        while self.token == Token::Id {
            let name = self.id_ref();
            let typ = self.read_data_type();
            self.b.def_local(name, typ);
            self.b.param_count += 1;
            if self.token == Token::RBra {
                break;
            }
            if self.token != Token::Comma {
                panic!("comma or closing bracket expected");
            }
            self.read_token();
        }
        self.read(Token::RBra);
        self.b.return_type = if
        /*is_func == 1 || is_func == 0 &&*/
        self.cs == b"RETURNS" {
            self.read_id(b"RETURNS");
            self.read_data_type()
        } else {
            NONE
        };
        if self.b.return_type != NONE {
            self.b.def_local(b"result", self.b.return_type);
        }
        self.read_id(b"AS");
        self.read_id(b"BEGIN");
        self.s_begin();
        self.b.resolve_jumps();
    }

    /// Read a byte, adjusting source line/column.
    fn read_char(&mut self) -> u8 {
        let cc;
        if self.source_ix >= self.source.len() {
            cc = 0;
            self.source_ix = self.source.len() + 1;
        } else {
            cc = self.source[self.source_ix];
            if cc == b'\n' {
                self.source_column = 1;
                self.source_line += 1;
            } else if (cc & 192) != 128
            // Test allows for UTF8 continuation chars.
            {
                self.source_column += 1;
            }
            self.source_ix += 1;
        }
        self.cc = cc;
        cc
    }

    /// Read the next token.
    fn read_token(&mut self) {
        self.token_space_start = self.source_ix - 1;
        self.prev_source_line = self.source_line;
        self.prev_source_column = self.source_column;
        let mut cc = self.cc;
        let mut token;
        'skip_space: loop {
            while cc == b' ' || cc == b'\n' || cc == b'\r' {
                cc = self.read_char();
            }
            self.token_start = self.source_ix - 1;
            let sc: u8 = cc;
            cc = self.read_char();
            match sc {
                b'A'..=b'Z' | b'a'..=b'z' | b'@' => {
                    token = Token::Id;
                    while cc.is_ascii_alphabetic() {
                        cc = self.read_char();
                    }
                    self.cs = &self.source[self.token_start..self.source_ix - 1];
                }
                b'0'..=b'9' => {
                    token = Token::Number;
                    let fc = self.source[self.token_start];
                    if fc == b'0' && cc == b'x' {
                        cc = self.read_char();
                        token = Token::Hex;
                        while cc.is_ascii_hexdigit()
                        {
                            cc = self.read_char();
                        }
                    } else {
                        while cc.is_ascii_digit() {
                            cc = self.read_char();
                        }
                        let part1 = self.source_ix - 1;
                        let s = str::from_utf8(&self.source[self.token_start..part1]).unwrap();
                        self.decimal_int = s.parse().unwrap();
                    }
                    self.cs = &self.source[self.token_start..self.source_ix - 1];
                }

                b'[' => {
                    token = Token::Id;
                    let start = self.source_ix - 1;
                    while cc != 0 {
                        if cc == b']' {
                            self.read_char();
                            break;
                        }
                        cc = self.read_char();
                    }
                    self.cs = &self.source[start..self.source_ix - 2];
                }

                b'\'' => {
                    token = Token::String;
                    let mut start = self.source_ix - 1;
                    self.ts = String::new();
                    loop {
                        if cc == 0 {
                            panic!("missing closing quote for string literal");
                        }
                        if cc == b'\'' {
                            cc = self.read_char();
                            if cc != b'\'' {
                                break;
                            }
                            self.ts.push_str(
                                str::from_utf8(&self.source[start..self.source_ix - 1]).unwrap(),
                            );
                            start = self.source_ix;
                        }
                        cc = self.read_char();
                    }
                    self.ts
                        .push_str(str::from_utf8(&self.source[start..self.source_ix - 2]).unwrap());
                    break;
                }

                b'-' => {
                    token = Token::Minus;
                    if cc == b'-'
                    // Skip single line comment.
                    {
                        while cc != b'\n' && cc != 0 {
                            cc = self.read_char();
                        }
                        continue 'skip_space;
                    }
                }

                b'/' => {
                    token = Token::Divide;
                    if cc == b'*'
                    // Skip comment.
                    {
                        cc = self.read_char();
                        let mut prevchar = b'X';
                        while (cc != b'/' || prevchar != b'*') && cc != 0 {
                            prevchar = cc;
                            cc = self.read_char();
                        }
                        cc = self.read_char();
                        continue 'skip_space;
                    }
                }
                b'>' => {
                    token = Token::Greater;
                    if cc == b'=' {
                        token = Token::GreaterEqual;
                        self.read_char();
                    }
                }
                b'<' => {
                    token = Token::Less;
                    if cc == b'=' {
                        token = Token::LessEqual;
                        self.read_char();
                    } else if cc == b'>' {
                        token = Token::NotEqual;
                        self.read_char();
                    }
                }
                b'!' => {
                    token = Token::Exclamation;
                    if cc == b'=' {
                        token = Token::NotEqual;
                        self.read_char();
                    }
                }
                b'(' => token = Token::LBra,
                b')' => token = Token::RBra,
                b'|' => {
                    token = Token::VBar;
                    if cc == b'=' {
                        token = Token::VBarEqual;
                        self.read_char();
                    }
                }
                b',' => token = Token::Comma,
                b'.' => token = Token::Dot,
                b'=' => token = Token::Equal,
                b'+' => token = Token::Plus,
                b':' => token = Token::Colon,
                b'*' => token = Token::Times,
                b'%' => token = Token::Percent,
                0 => token = Token::EndOfFile,
                _ => token = Token::Unknown,
            }
            break;
        } // skip_space loop
        self.token = token;
    }

    // ****************** Helper functions for parsing.

    fn source_from(&self, start: usize, end: usize) -> String {
        to_s(&self.source[start..end])
    }

    fn read_data_type(&mut self) -> DataType {
        if self.token != Token::Id {
            panic!("datatype expected");
        }
        let mut t = match self.id_ref() {
            b"int" => INT,
            b"string" => STRING,
            b"binary" => BINARY,
            b"float" => FLOAT,
            b"double" => DOUBLE,
            b"bool" => BOOL,
            _ => panic!("datatype expected"),
        };
        if self.test(Token::LBra) {
            let mut n = self.decimal_int as usize;
            self.read(Token::Number);
            self.read(Token::RBra);
            match t {
                BINARY | STRING => {
                    n += 1;
                    if n > 250 {
                        n = 250;
                    }
                    if n < 9 {
                        n = 9;
                    }
                }
                INT => {
                    if n < 1 {
                        panic!("minimum int precision is 1");
                    }
                    if n > 8 {
                        panic!("maximum int precision is 8");
                    }
                }
                _ => panic!("invalid data type specification"),
            }
            t = (t % 8) + (8 * n);
        }
        t
    }

    /// Examine current token, determine if it is an operator.
    /// Result is operator token and precedence, or -1 if current token is not an operator.
    fn operator(&mut self) -> (Token, i8) {
        let mut t = self.token;
        if t >= Token::Id {
            if t == Token::Id {
                t = match self.cs {
                    b"AND" => Token::And,
                    b"OR" => Token::Or,
                    b"IN" => Token::In,
                    _ => return (t, -1),
                }
            } else {
                return (t, -1);
            }
        }
        (t, t.precedence())
    }

    fn id(&mut self) -> String {
        to_s(self.id_ref())
    }

    fn id_ref(&mut self) -> &'a [u8] {
        if self.token != Token::Id {
            panic!("name expected");
        }
        let result = self.cs;
        self.read_token();
        result
    }

    fn local(&mut self) -> usize {
        let result: usize;
        if self.token != Token::Id {
            panic!("name expected");
        }
        if let Some(lnum) = self.b.get_local(self.cs) {
            result = *lnum;
        } else {
            panic!("undeclared local: {}", tos(self.cs))
        }
        self.read_token();
        result
    }

    /// Checks the token is as expected, and consumes it.
    fn read(&mut self, t: Token) {
        if self.token != t {
            panic!("expected '{:?}' got '{:?}'", t, self.token)
        } else {
            self.read_token();
        }
    }

    /// Checks the token is the specified Id and consumes it.
    fn read_id(&mut self, s: &[u8]) {
        if self.token != Token::Id || self.cs != s {
            panic!("expected '{}' got '{}'", tos(s), tos(self.cs));
        } else {
            self.read_token();
        }
    }

    /// Tests whether the token is as specified. If so, it is consumed.
    fn test(&mut self, t: Token) -> bool {
        let result = self.token == t;
        if result {
            self.read_token();
        }
        result
    }

    /// Tests whether the token is the specified id. If so, it is consumed.
    fn test_id(&mut self, s: &[u8]) -> bool {
        if self.token != Token::Id || self.cs != s {
            false
        } else {
            self.read_token();
            true
        }
    }

    /// Reads an ObjRef ( schema.name pair ).
    fn obj_ref(&mut self) -> ObjRef {
        let schema = self.id();
        self.read(Token::Dot);
        let name = self.id();
        ObjRef { schema, name }
    }

    // Error handling.
    /// Get the function name or "batch" if no function.
    fn rname(&self) -> String {
        if let Some(name) = self.function_name {
            name.str()
        } else {
            "batch".to_string()
        }
    }

    /// Construct SqlError based on current line/column/rname.
    pub(crate) fn make_error(&self, msg: String) -> SqlError {
        SqlError {
            line: self.prev_source_line,
            column: self.prev_source_column,
            msg,
            rname: self.rname(),
        }
    }

    // End Helper functions for parsing.

    // ****************** Expression parsing

    /// Parses an expression that starts with an id.
    fn exp_id(&mut self, _agg_allowed: bool) -> Expr {
        let name = self.id_ref();
        if self.test(Token::Dot) {
            let fname = self.id_ref();
            let mut parms = Vec::new();
            self.read(Token::LBra);
            if self.token != Token::RBra {
                loop {
                    parms.push(self.exp());
                    if !self.test(Token::Comma) {
                        break;
                    }
                }
            }
            self.read(Token::RBra);
            let name = ObjRef {
                schema: to_s(name),
                name: to_s(fname),
            };
            Expr::new(ExprIs::FuncCall(name, parms))
        } else if self.test(Token::LBra) {
            let mut parms = Vec::new();
            if self.token != Token::RBra {
                loop {
                    parms.push(self.exp());
                    if !self.test(Token::Comma) {
                        break;
                    }
                }
            }
            self.read(Token::RBra);
            Expr::new(ExprIs::BuiltinCall(to_s(name), parms))
        } else if name == b"true" {
            Expr::new(ExprIs::Const(Value::Bool(true)))
        } else if name == b"false" {
            Expr::new(ExprIs::Const(Value::Bool(false)))
        } else if let Some(lnum) = self.b.get_local(name) {
            Expr::new(ExprIs::Local(*lnum))
        } else {
            Expr::new(ExprIs::ColName(to_s(name)))
        }
    }

    /// Parses a primary expression ( basic expression with no operators ).
    fn exp_primary(&mut self, agg_allowed: bool) -> Expr {
        let result;
        if self.token == Token::Id {
            result = if self.test_id(b"CASE") {
                self.exp_case()
            } else if self.test_id(b"NOT") {
                let e = self.exp_p(10); // Not sure about precedence here.
                Expr::new(ExprIs::Not(Box::new(e)))
            } else {
                self.exp_id(agg_allowed)
            };
        } else if self.test(Token::LBra) {
            if self.test_id(b"SELECT") {
                result = self.exp_scalar_select();
            } else {
                let exp = self.exp();
                if self.test(Token::Comma)
                // Operand of IN e.g. X IN ( 1,2,3 )
                {
                    let mut list = vec![exp];
                    loop {
                        list.push(self.exp());
                        if !self.test(Token::Comma) {
                            break;
                        }
                    }
                    result = Expr::new(ExprIs::List(list));
                } else {
                    result = exp;
                }
            }
            self.read(Token::RBra);
        } else if self.token == Token::String {
            result = Expr::new(ExprIs::Const(Value::String(Rc::new(self.ts.clone()))));
            self.read_token();
        } else if self.token == Token::Number {
            let value = self.decimal_int;
            result = Expr::new(ExprIs::Const(Value::Int(value)));
            self.read_token();
        } else if self.token == Token::Hex {
            if self.cs.len() % 2 == 1 {
                panic!("hex literal must have even number of characters");
            }
            let hb = &self.source[self.token_start + 2..self.source_ix - 1];
            result = Expr::new(ExprIs::Const(Value::RcBinary(Rc::new(util::parse_hex(hb)))));
            self.read_token();
        } else if self.test(Token::Minus) {
            result = Expr::new(ExprIs::Minus(Box::new(self.exp_p(30))));
        } else {
            panic!("expression expected")
        }
        result
    }

    fn exp_or_agg(&mut self) -> Expr {
        let pri = self.exp_primary(true);
        self.exp_lp(pri, 0)
    }

    /// Parse an expression.
    fn exp(&mut self) -> Expr {
        self.exp_p(0)
    }

    /// Parse an expression, with specified operator precedence.
    fn exp_p(&mut self, precedence: i8) -> Expr {
        let pr = self.exp_primary(false);
        self.exp_lp(pr, precedence)
    }

    /// Apply binary operator to lhs based on precedence.
    fn exp_lp(&mut self, mut lhs: Expr, precedence: i8) -> Expr {
        let mut t = self.operator();
        while t.1 >= precedence {
            let op = t;
            self.read_token();
            let mut rhs = self.exp_primary(false);
            t = self.operator();
            while t.1 > op.1
            /* or t is right-associative and t.1 == op.1 */
            {
                rhs = self.exp_lp(rhs, t.1);
                t = self.operator();
            }
            lhs = Expr::new(ExprIs::Binary(op.0, Box::new(lhs), Box::new(rhs)));
        }
        lhs
    }

    /// Parse a CASE expression.
    fn exp_case(&mut self) -> Expr {
        let mut list = Vec::new();
        while self.test_id(b"WHEN") {
            let test = self.exp();
            self.read_id(b"THEN");
            let e = self.exp();
            list.push((test, e));
        }
        if list.is_empty() {
            panic!("empty CASE expression");
        }
        self.read_id(b"ELSE");
        let els = Box::new(self.exp());
        self.read_id(b"END");
        Expr::new(ExprIs::Case(list, els))
    }

    fn exp_scalar_select(&mut self) -> Expr {
        let te = self.select_expression(false);
        // if ( te.ColumnCount != 1 ) Error ( "Scalar select must have one column" );
        Expr::new(ExprIs::ScalarSelect(Box::new(te)))
    }

    // End Expression parsing

    // ****************** Table expression parsing

    fn insert_expression(&mut self, expect: usize) -> TableExpression {
        if !self.test_id(b"VALUES") {
            panic!("VALUES or SELECT expected");
        }
        // else if self.test_id( b"SELECT" ) { self.expressions() } ...
        self.values(expect)
    }

    fn values(&mut self, expect: usize) -> TableExpression {
        let mut values = Vec::new();
        while self.test(Token::LBra) {
            let mut v = Vec::new();
            loop {
                v.push(self.exp());
                if self.test(Token::RBra) {
                    break;
                }
                if self.token != Token::Comma {
                    panic!("comma or closing bracket expected");
                }
                self.read_token();
            }
            if v.len() != expect {
                panic!("wrong number of values");
            }
            values.push(v);
            if !self.test(Token::Comma) && self.token != Token::LBra {
                break;
            } // The comma between multiple VALUES is optional.
        }
        TableExpression::Values(values)
    }

    fn te_named_table(&mut self) -> TableExpression {
        let schema = self.id();
        self.read(Token::Dot);
        let name = self.id();
        let name = ObjRef { schema, name };
        TableExpression::Base(name)
    }

    fn primary_table_exp(&mut self) -> TableExpression {
        /*
            if ( test( Token::LBra ) )
            {
              read( "SELECT" );
              TableExpression te = Expressions( null );
              read( Token::RBra );
              if ( test("AS") ) te.Alias = Name();
              return te;
            } else
        */
        if self.token != Token::Id {
            panic!("table expected");
        }
        self.te_named_table()
    }

    fn exp_name(&self, exp: &Expr) -> String {
        match &exp.exp {
            ExprIs::Local(num) => to_s(self.b.local_name(*num)),
            ExprIs::ColName(name) => name.to_string(),
            _ => "".to_string(),
        }
    }

    /// Parse a SELECT / SET / FOR expression.
    fn select_expression(&mut self, set_or_for: bool) -> SelectExpression {
        let mut exps = Vec::new();
        let mut colnames = Vec::new();
        let mut assigns = Vec::new();
        loop {
            if set_or_for {
                let local = self.local();
                let op = match self.token {
                    Token::Equal => AssignOp::Assign,
                    Token::VBarEqual => AssignOp::Append,
                    _ => panic!("= or |= expected"),
                };
                self.read_token();
                assigns.push((local, op));
            }
            let exp = self.exp_or_agg();
            if self.test_id(b"AS") {
                colnames.push(self.id());
            } else {
                colnames.push(self.exp_name(&exp));
            }
            exps.push(exp);
            if !self.test(Token::Comma) {
                break;
            }
        }
        let from = if self.test_id(b"FROM") {
            Some(Box::new(self.primary_table_exp()))
        } else {
            None
        };
        let wher = if self.test_id(b"WHERE") {
            Some(self.exp())
        } else {
            None
        };
        let mut orderby = Vec::new();
        if self.test_id(b"ORDER") {
            self.read_id(b"BY");
            loop {
                let exp = self.exp();
                let desc = if self.test_id(b"DESC") {
                    true
                } else {
                    self.test_id(b"ASC");
                    false
                };
                orderby.push((exp, desc));
                if !self.test(Token::Comma) {
                    break;
                }
            }
        }
        SelectExpression {
            colnames,
            assigns,
            exps,
            from,
            wher,
            orderby,
        }
    }

    // ****************** Statement parsing

    fn s_select(&mut self) {
        let se = self.select_expression(false);
        if !self.b.parse_only {
            let cte = c_select(&mut self.b, se);
            self.b.add(Select(Box::new(cte)));
        }
    }

    fn s_set(&mut self) {
        let se = self.select_expression(true);
        if !self.b.parse_only {
            let cte = c_select(&mut self.b, se);
            self.b.add(Set(Box::new(cte)));
        }
    }

    fn s_insert(&mut self) {
        self.read_id(b"INTO");
        let tr = self.obj_ref();
        self.read(Token::LBra);
        let mut cnames = Vec::new();
        loop {
            let cname = self.id_ref();
            if cnames.contains(&cname) {
                panic!("duplicate column name");
            }
            cnames.push(cname);
            if self.test(Token::RBra) {
                break;
            }
            if !self.test(Token::Comma) {
                panic!("comma or closing bracket expected");
            }
        }
        let mut src = self.insert_expression(cnames.len());
        if !self.b.parse_only {
            let t = c_table(&self.b, &tr);
            let mut cnums: Vec<usize> = Vec::new();
            {
                for cname in &cnames {
                    if let Some(cnum) = t.info.get(tos(cname)) {
                        cnums.push(*cnum);
                    } else {
                        panic!("column name '{}' not found", tos(cname))
                    }
                }
            }
            let csrc = c_te(&self.b, &mut src);
            self.b.dop(DO::Insert(t, cnums, csrc));
        }
    }

    fn s_update(&mut self) {
        let tname = self.obj_ref();
        self.read_id(b"SET");
        let mut assigns = Vec::new();
        loop {
            let name = self.id();
            self.read(Token::Equal);
            let exp = self.exp();
            assigns.push((name, exp));
            if !self.test(Token::Comma) {
                break;
            }
        }
        if !self.test_id(b"WHERE") {
            panic!("UPDATE must have a WHERE");
        }
        let mut wher = Some(self.exp());
        if !self.b.parse_only {
            c_update(&mut self.b, &tname, &mut assigns, &mut wher);
        }
    }

    fn s_delete(&mut self) {
        self.read_id(b"FROM");
        let tname = self.obj_ref();
        if !self.test_id(b"WHERE") {
            panic!("DELETE must have a WHERE");
        }
        let mut wher = Some(self.exp());
        if !self.b.parse_only {
            c_delete(&mut self.b, &tname, &mut wher);
        }
    }

    fn s_execute(&mut self) {
        self.read(Token::LBra);
        let mut exp = self.exp();
        self.read(Token::RBra);
        if !self.b.parse_only {
            push(&mut self.b, &mut exp);
            self.b.add(Execute);
        }
    }

    fn s_check(&mut self) {
        let name = self.obj_ref();
        if !self.b.parse_only {
            c_function(&self.b.db, &name);
        }
    }

    fn s_exec(&mut self) {
        let mut pname = self.id();
        let mut sname = "".to_string();
        if self.test(Token::Dot) {
            sname = pname;
            pname = self.id();
        }
        let name = ObjRef {
            schema: sname,
            name: pname,
        };
        self.read(Token::LBra);
        let mut pkinds = Vec::new();
        if !self.test(Token::RBra) {
            let mut e = self.exp();
            pkinds.push(push(&mut self.b, &mut e));
            while self.test(Token::Comma) {
                let mut e = self.exp();
                pkinds.push(push(&mut self.b, &mut e));
            }
            self.read(Token::RBra);
        }
        if !self.b.parse_only {
            let func = c_function(&self.b.db, &name);
            self.b.check_types(&func, &pkinds);
            self.b.add(Call(func));
        }
    }

    fn s_for(&mut self) {
        let se: SelectExpression = self.select_expression(true);
        let for_id = self.b.local_typ.len();
        self.b.local_typ.push(NONE);
        let start_id = self.b.get_jump_id();
        let break_id = self.b.get_jump_id();
        if !self.b.parse_only {
            c_for(&mut self.b, se, start_id, break_id, for_id);
        }
        let save = self.b.break_id;
        self.b.break_id = break_id;
        self.statement();
        self.b.break_id = save;
        self.b.add(Jump(start_id));
        self.b.set_jump(break_id);
    }

    // ****************** Parse Create statements.

    fn create_table(&mut self) {
        let name = self.obj_ref();
        let source_start = self.source_ix - 2;
        self.read(Token::LBra);
        let mut ti = ColInfo::empty(name);
        loop {
            let cname = self.id();
            let typ = self.read_data_type();
            if ti.add(cname, typ) {
                panic!("duplicate column name");
            }
            if self.test(Token::RBra) {
                break;
            }
            if self.token != Token::Comma {
                panic!("comma or closing bracket expected");
            }
            self.read_token();
        }
        if !self.b.parse_only {
            let _source = self.source_from(source_start, self.token_start);
            self.b.dop(DO::CreateTable(ti));
        }
    }

    fn create_index(&mut self) {
        let iname = self.id();
        self.read_id(b"ON");
        let tname = self.obj_ref();
        self.read(Token::LBra);
        let mut cnames = Vec::new();
        loop {
            cnames.push(self.id());
            if self.test(Token::RBra) {
                break;
            }
            if self.token != Token::Comma {
                panic!("comma or closing bracket expected")
            };
            self.read_token();
        }
        if !self.b.parse_only {
            let mut cols = Vec::new();
            let table = c_table(&self.b, &tname);
            for cname in &cnames {
                if let Some(cnum) = table.info.colmap.get(cname) {
                    cols.push(*cnum);
                } else {
                    panic!("index column name not found {}", cname);
                }
            }
            self.b
                .dop(DO::CreateIndex(IndexInfo { tname, iname, cols }));
        }
    }

    fn create_function(&mut self, alter: bool) {
        let rref: ObjRef = self.obj_ref();
        let source_start: usize = self.source_ix - 2;
        let db = self.b.db.clone();
        let save: Block = mem::replace(&mut self.b, Block::new(db));
        let save2: bool = self.b.parse_only;
        self.b.parse_only = true;
        self.parse_function();
        let _cb: Block = mem::replace(&mut self.b, save);
        self.b.parse_only = save2;
        if !self.b.parse_only {
            let source: String = self.source_from(source_start, self.token_space_start);
            self.b.dop(DO::CreateFunction(rref, Rc::new(source), alter));
        }
    }

    fn s_create(&mut self) {
        match self.id_ref() {
            b"FN" => self.create_function(false),
            b"TABLE" => self.create_table(),
            b"SCHEMA" => {
                let name = self.id();
                self.b.dop(DO::CreateSchema(name));
            }
            b"INDEX" => self.create_index(),
            _ => panic!("unknown keyword"),
        }
    }

    fn s_alter(&mut self) {
        match self.id_ref() {
            b"FN" => self.create_function(true),
            b"TABLE" => self.s_alter_table(),
            _ => panic!("ALTER : TABLE,FN.. expected"),
        }
    }

    fn s_drop(&mut self) {
        match self.id_ref() {
            b"TABLE" => {
                let tr = self.obj_ref();
                self.b.dop(DO::DropTable(tr));
            }
            b"INDEX" => {
                let ix = self.id();
                self.read_id(b"ON");
                let tr = self.obj_ref();
                self.b.dop(DO::DropIndex(tr, ix));
            }
            b"FN" => {
                let fr = self.obj_ref();
                self.b.dop(DO::DropFunction(fr));
            }
            b"SCHEMA" => {
                let s = self.id();
                self.b.dop(DO::DropSchema(s));
            }
            _ => {
                panic!("DROP : TABLE,FN .. expected");
            }
        }
    }

    fn s_alter_table(&mut self) {
        let tr = self.obj_ref();
        let mut list = Vec::new();
        loop {
            if self.test_id(b"ADD") {
                let col = self.id();
                let datatype = self.read_data_type();
                list.push(AlterCol::Add(col, datatype));
            } else if self.test_id(b"DROP") {
                let col = self.id();
                list.push(AlterCol::Drop(col));
            } else if self.test_id(b"MODIFY") {
                let col = self.id();
                let datatype = self.read_data_type();
                list.push(AlterCol::Modify(col, datatype));
            } else {
                break;
            }
            if !self.test(Token::Comma) {
                break;
            }
        }
        self.b.dop(DO::AlterTable(tr, list));
    }

    // Other statements.
    fn s_declare(&mut self) {
        loop {
            let name = self.id_ref();
            let dt = self.read_data_type();
            self.b.def_local(name, dt);
            if !self.test(Token::Comma) {
                break;
            }
        }
    }

    fn s_while(&mut self) {
        let mut exp = self.exp();
        let start_id = self.b.get_loop_id();
        let break_id = self.b.get_jump_id();
        if !self.b.parse_only {
            let exp = c_bool(&self.b, &mut exp);
            self.b.add(JumpIfFalse(break_id, exp));
            let save = self.b.break_id;
            self.b.break_id = break_id;
            self.statement();
            self.b.break_id = save;
            self.b.add(Jump(start_id));
            self.b.set_jump(break_id);
        }
    }

    fn s_if(&mut self) {
        let mut exp = self.exp();
        let false_id = self.b.get_jump_id();
        if !self.b.parse_only {
            let exp = c_bool(&self.b, &mut exp);
            self.b.add(JumpIfFalse(false_id, exp));
        }
        self.statement();
        if self.test_id(b"ELSE") {
            let end_id = self.b.get_jump_id();
            self.b.add(Jump(end_id)); // Skip over the else clause
            self.b.set_jump(false_id);
            self.statement();
            self.b.set_jump(end_id);
        } else {
            self.b.set_jump(false_id);
        }
    }

    fn s_goto(&mut self) {
        let label = self.id_ref();
        let to = self.b.get_goto_label(label);
        self.b.add(Jump(to));
    }

    fn s_break(&mut self) {
        let break_id = self.b.break_id;
        if break_id == usize::MAX {
            panic!("no enclosing loop for break");
        }
        self.b.add(Jump(break_id));
    }

    fn s_return(&mut self) {
        if self.b.return_type != NONE {
            let mut e = self.exp();
            if !self.b.parse_only {
                let k = push(&mut self.b, &mut e);
                let rk = data_kind(self.b.return_type);
                if k != rk {
                    panic!("return type mismatch expected {:?} got {:?}", rk, k)
                }
                self.b.add(PopToLocal(self.b.param_count));
            }
        }
        self.b.add(Return);
    }

    fn s_throw(&mut self) {
        let mut msg = self.exp();
        if !self.b.parse_only {
            push(&mut self.b, &mut msg);
            self.b.add(Throw);
        }
    }

    fn s_begin(&mut self) {
        while !self.test_id(b"END") {
            self.statement();
        }
    }
} // end impl Parser

/// Convert byte ref to &str.
pub fn tos(s: &[u8]) -> &str {
    str::from_utf8(s).unwrap()
}

/// Convert byte ref to String.
pub fn to_s(s: &[u8]) -> String {
    str::from_utf8(s).unwrap().to_string()
}
