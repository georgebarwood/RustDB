use crate::*;
use std::{mem, str};

/// SQL parser.
///
/// Name convention for methods:
///
/// s_ parses a statement.
///
/// exp_ parses an expression.
pub struct Parser<'a>
{
  pub(crate) function_name: Option<&'a ObjRef>,
  source: &'a [u8], // Source SQL.
  source_ix: usize, // Index into source.
  cc: u8,           // Current input char.
  token: Token,     // Current token.
  token_start: usize,
  cs: &'a [u8], // source slice for current token ( but string literals are in ts )
  ts: String,
  source_column: usize,
  source_line: usize,
  decimal_int: i64,
  prev_source_column: usize,
  prev_source_line: usize,

  pub(crate) parse_only: bool,
  pub(crate) b: Block<'a>,
  pub(crate) db: DB,
  pub(crate) from: Option<CTableExpression>,
}

impl<'a> Parser<'a>
{
  /// Construct a new parser.
  pub(crate) fn new(src: &'a str, db: &DB) -> Self
  {
    let source = src.as_bytes();
    let mut result = Self {
      source,
      db: db.clone(),
      function_name: None,
      source_ix: 0,
      cc: 0,
      token_start: 0,
      token: Token::EndOfFile,
      cs: source,
      ts: String::new(),
      source_column: 1,
      source_line: 1,
      prev_source_column: 1,
      prev_source_line: 1,
      decimal_int: 0,
      b: Block::new(),
      parse_only: false,
      from: None,
    };
    result.read_char();
    result.read_token();
    // println!( "Parsing {}", src );
    result
  }

  /// Parse a single statement.
  fn statement(&mut self)
  {
    if self.token == Token::Id
    {
      let id = self.cs;
      self.read_token();
      if self.test(Token::Colon)
      {
        self.s_set_label(id);
      }
      else
      {
        match id
        {
          b"ALTER" => self.s_alter(),
          b"BEGIN" => self.s_begin(),
          b"BREAK" => self.s_break(),
          b"CREATE" => self.s_create(),
          b"DROP" => self.s_drop(),
          b"DECLARE" => self.s_declare(),
          b"DELETE" => self.s_delete(),
          b"EXEC" => self.s_exec(),
          b"EXECUTE" => self.s_execute(),
          b"FOR" => self.s_for(),
          b"GOTO" => self.s_goto(),
          b"IF" => self.s_if(),
          b"INSERT" => self.s_insert(),
          b"RENAME" => self.s_rename(),
          b"RETURN" => self.s_return(),
          b"SELECT" => self.s_select(),
          b"SET" => self.s_set(),
          b"THROW" => self.s_throw(),
          b"UPDATE" => self.s_update(),
          b"WHILE" => self.s_while(),
          _ => panic!("statement keyword expected, got '{}'", tos(id)),
        }
      }
    }
    else
    {
      panic!("statement keyword expected, got '{:?}'", self.token)
    }
  } // end fn statement

  /// Parse and execute a batch of statements.
  pub(crate) fn batch(&mut self, rs: &mut dyn Query)
  {
    loop
    {
      while self.token != Token::EndOfFile && !self.test_id(b"GO")
      {
        self.statement();
      }
      self.b.resolve_jumps();
      let mut ee = EvalEnv::new(self.db.clone(), rs);

      // let start = std::time::Instant::now();

      ee.alloc_locals(&self.b.local_typ, 0);
      ee.go(&self.b.ilist);

      // println!( "EvalEnv::exec Time elapsed={} micro sec.", start.elapsed().as_micros() );

      if self.token == Token::EndOfFile
      {
        break;
      }
      self.b = Block::new();
    }
  }

  /// Parse the definition of a function.
  pub(crate) fn parse_function(&mut self)
  {
    self.read(Token::LBra);
    while self.token == Token::Id
    {
      let name = self.id_ref();
      let typ = self.read_data_type();
      self.def_local(name, typ);
      self.b.param_count += 1;
      if self.token == Token::RBra
      {
        break;
      }
      if self.token != Token::Comma
      {
        self.err("Comma or closing bracket expected");
      }
      self.read_token();
    }
    self.read(Token::RBra);
    self.b.return_type = if
    /*is_func == 1 || is_func == 0 &&*/
    self.cs == b"RETURNS"
    {
      self.read_id(b"RETURNS");
      self.read_data_type()
    }
    else
    {
      NONE
    };

    if self.b.return_type != NONE
    {
      self.def_local(b"result", self.b.return_type);
    }

    self.read_id(b"AS");
    self.read_id(b"BEGIN");
    self.s_begin();
    self.b.resolve_jumps();
  }

  /// Read a byte, adjusting source line/column.
  fn read_char(&mut self) -> u8
  {
    let cc;
    if self.source_ix >= self.source.len()
    {
      cc = 0;
      self.source_ix = self.source.len() + 1;
    }
    else
    {
      cc = self.source[self.source_ix];
      if cc == b'\n'
      {
        self.source_column = 1;
        self.source_line += 1;
      }
      else if (cc & 192) != 128
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
  fn read_token(&mut self)
  {
    self.prev_source_line = self.source_line;
    self.prev_source_column = self.source_column;
    let mut cc = self.cc;
    let mut token;
    'skip_space: loop
    {
      while cc == b' ' || cc == b'\n' || cc == b'\r'
      {
        cc = self.read_char();
      }
      self.token_start = self.source_ix - 1;

      let sc: u8 = cc;
      cc = self.read_char();
      match sc
      {
        b'A'..=b'Z' | b'a'..=b'z' | b'@' =>
        {
          token = Token::Id;
          while (b'A'..=b'Z').contains(&cc) || (b'a'..=b'z').contains(&cc)
          {
            cc = self.read_char();
          }
          self.cs = &self.source[self.token_start..self.source_ix - 1];
        }
        b'0'..=b'9' =>
        {
          token = Token::Number;
          let fc = self.source[self.token_start];
          if fc == b'0' && cc == b'x'
          {
            cc = self.read_char();
            token = Token::Hex;
            while (b'0'..=b'9').contains(&cc) || (b'A'..b'F').contains(&cc) || (b'a'..=b'f').contains(&cc)
            {
              cc = self.read_char();
            }
          }
          else
          {
            while (b'0'..=b'9').contains(&cc)
            {
              cc = self.read_char();
            }
            let part1 = self.source_ix - 1;
            let s = str::from_utf8(&self.source[self.token_start..part1]).unwrap();
            self.decimal_int = s.parse().unwrap();
            if cc == b'.' && token == Token::Number
            {
              token = Token::Decimal;
              cc = self.read_char();
              while (b'0'..=b'9').contains(&cc)
              {
                cc = self.read_char();
              }
              // DecimalScale = source_ix - ( part1 + 1 );
              // DecimalFrac = long.Parse( Source.Substring( part1 + 1, DecimalScale ) );
            }
            else
            {
              // DecimalScale = 0;
              // DecimalFrac = 0;
            }
          }
          self.cs = &self.source[self.token_start..self.source_ix - 1];
        }

        b'[' =>
        {
          token = Token::Id;
          let start = self.source_ix - 1;
          while cc != 0
          {
            if cc == b']'
            {
              self.read_char();
              break;
            }
            cc = self.read_char();
          }
          self.cs = &self.source[start..self.source_ix - 2];
        }

        b'\'' =>
        {
          token = Token::String;
          let mut start = self.source_ix - 1;
          self.ts = String::new();
          loop
          {
            if cc == 0
            {
              self.err("missing closing quote for string literal");
            }
            if cc == b'\''
            {
              cc = self.read_char();
              if cc != b'\''
              {
                break;
              }
              self
                .ts
                .push_str(str::from_utf8(&self.source[start..self.source_ix - 1]).unwrap());
              start = self.source_ix;
            }
            cc = self.read_char();
          }
          self
            .ts
            .push_str(str::from_utf8(&self.source[start..self.source_ix - 2]).unwrap());
          break;
        }

        b'-' =>
        {
          token = Token::Minus;
          if cc == b'-'
          // Skip single line comment.
          {
            while cc != b'\n' && cc != 0
            {
              cc = self.read_char();
            }
            continue 'skip_space;
          }
        }

        b'/' =>
        {
          token = Token::Divide;
          if cc == b'*'
          // Skip comment.
          {
            cc = self.read_char();
            let mut prevchar = b'X';
            while (cc != b'/' || prevchar != b'*') && cc != 0
            {
              prevchar = cc;
              cc = self.read_char();
            }
            cc = self.read_char();
            continue 'skip_space;
          }
        }
        b'>' =>
        {
          token = Token::Greater;
          if cc == b'='
          {
            token = Token::GreaterEqual;
            self.read_char();
          }
        }
        b'<' =>
        {
          token = Token::Less;
          if cc == b'='
          {
            token = Token::LessEqual;
            self.read_char();
          }
          else if cc == b'>'
          {
            token = Token::NotEqual;
            self.read_char();
          }
        }
        b'!' =>
        {
          token = Token::Exclamation;
          if cc == b'='
          {
            token = Token::NotEqual;
            self.read_char();
          }
        }
        b'(' => token = Token::LBra,
        b')' => token = Token::RBra,
        b'|' =>
        {
          token = Token::VBar;
          if cc == b'='
          {
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
    // println!("Got token {:}", token );
  }

  // ****************** Helper functions for parsing.

  fn source_from(&self, start: usize, end: usize) -> String
  {
    to_s(&self.source[start..end])
  }

  fn read_data_type(&mut self) -> DataType
  {
    if self.token != Token::Id
    {
      self.err("datatype expected");
    }
    match self.id_ref()
    {
      b"int" => INT,
      b"string" => STRING,
      b"binary" => BINARY,
      b"tinyint" => TINYINT,
      b"smallint" => SMALLINT,
      b"bigint" => BIGINT,
      b"float" => FLOAT,
      b"double" => DOUBLE,
      b"bool" => BOOL,
      b"decimal" =>
      {
        let mut p = 0;
        let mut q = 0;
        if self.test(Token::LBra)
        {
          p = self.read_int();
          if p < 1
          {
            self.err("Minimum precision is 1")
          }
          if p > 18
          {
            self.err("Maxiumum decimal precision is 18")
          }
          if self.test(Token::Comma)
          {
            q = self.read_int();
          }
          if q < 0
          {
            self.err("Scale cannot be negative")
          }
          if q > p
          {
            self.err("Scale cannot be greater than precision")
          }
          self.read(Token::RBra);
        }
        DECIMAL + ((p as usize) << 3) + ((q as usize) << 8)
      }
      _ => self.err("Datatype expected"),
    }
  }

  pub(crate) fn check_types(&self, r: &FunctionPtr, ptypes: &[DataType])
  {
    if ptypes.len() != r.param_count
    {
      self.err("param count mismatch");
    }
    for (i, pt) in ptypes.iter().enumerate()
    {
      let ft = data_kind(r.local_typ[i]);
      let et = data_kind(*pt);
      if ft != et
      {
        panic!("param type mismatch expected {:?} got {:?}", ft, et);
      }
    }
  }

  fn get_operator(&mut self) -> (Token, i8)
  {
    let mut t = self.token;
    if t >= Token::Id
    {
      if t == Token::Id
      {
        t = match self.cs
        {
          b"AND" => Token::And,
          b"OR" => Token::Or,
          b"IN" => Token::In,
          _ => return (t, -1),
        }
      }
      else
      {
        return (t, -1);
      }
    }
    (t, t.precedence())
  }

  fn id(&mut self) -> String
  {
    to_s(self.id_ref())
  }

  fn id_ref(&mut self) -> &'a [u8]
  {
    if self.token != Token::Id
    {
      self.err("Name expected");
    }
    let result = self.cs;
    self.read_token();
    result
  }

  fn local(&mut self) -> usize
  {
    let result: usize;
    if self.token != Token::Id
    {
      self.err("Name expected");
    }
    if let Some(local) = self.b.local_map.get(self.cs)
    {
      result = *local;
    }
    else
    {
      panic!("Undeclared local: {}", tos(self.cs))
    }
    self.read_token();
    result
  }

  fn read_int(&mut self) -> i64
  {
    if self.token != Token::Number
    {
      self.err("Number expected");
    }
    let result = tos(self.cs).parse::<i64>().unwrap();
    self.read_token();
    result
  }

  /// Checks the token is as expected, and consumes it.
  fn read(&mut self, t: Token)
  {
    if self.token != t
    {
      panic!("Expected ttoken '{:?}' got '{:?}'", t, self.token)
    }
    else
    {
      self.read_token();
    }
  }

  /// Checks the token is the specified Id and consumes it.
  fn read_id(&mut self, s: &[u8])
  {
    if self.token != Token::Id || self.cs != s
    {
      panic!("Expected '{}' got '{}'", tos(s), tos(self.cs));
    }
    else
    {
      self.read_token();
    }
  }

  /// Tests whether the token is the speificed id. If so, it is consumed.
  fn test_id(&mut self, s: &[u8]) -> bool
  {
    if self.token != Token::Id || self.cs != s
    {
      false
    }
    else
    {
      self.read_token();
      true
    }
  }

  /// Tests whether the token is as specified. If so, it is consumed.
  fn test(&mut self, t: Token) -> bool
  {
    let result = self.token == t;
    if result
    {
      self.read_token();
    }
    result
  }

  /// Reads an ObjRef ( schema.name pair ).
  fn obj_ref(&mut self) -> ObjRef
  {
    let schema = self.id();
    self.read(Token::Dot);
    let name = self.id();
    ObjRef { schema, name }
  }

  /// Add an instruction to the instruction list.
  pub(crate) fn add(&mut self, s: Inst)
  {
    if !self.parse_only
    {
      self.b.ilist.push(s);
    }
  }

  /// Add a Data Operation (DO) to the instruction list.
  fn dop(&mut self, dop: DO)
  {
    if !self.parse_only
    {
      self.add(Inst::DataOp(Box::new(dop)));
    }
  }

  // Error handling.

  /// Get the function name or "batch" if no function.
  fn rname(&self) -> String
  {
    if let Some(r) = self.function_name
    {
      r.schema.to_string() + "." + &r.name
    }
    else
    {
      "batch".to_string()
    }
  }

  /// Construct SqlError based on current line/column/rname.
  pub(crate) fn make_error(&self, msg: String) -> SqlError
  {
    SqlError { line: self.prev_source_line, column: self.prev_source_column, msg, rname: self.rname() }
  }

  /// Panic based on current line/column with specified message.
  pub(crate) fn err(&self, msg: &str) -> !
  {
    panic!("{}", msg.to_string());
  }

  // End Helper functions for parsing.

  // ****************** Expression parsing

  /// Parses an expression that starts with an id.
  fn exp_id(&mut self, _agg_allowed: bool) -> Expr
  {
    let name = self.id_ref();
    if self.test(Token::Dot)
    {
      let fname = self.id_ref();
      let mut parms = Vec::new();
      self.read(Token::LBra);
      if self.token != Token::RBra
      {
        loop
        {
          parms.push(self.exp());
          if !self.test(Token::Comma)
          {
            break;
          }
        }
      }
      self.read(Token::RBra);
      let name = ObjRef { schema: to_s(name), name: to_s(fname) };
      Expr::new(ExprIs::FuncCall(name, parms))
    }
    else if self.test(Token::LBra)
    {
      let mut parms = Vec::new();
      if self.token != Token::RBra
      {
        loop
        {
          parms.push(self.exp());
          if !self.test(Token::Comma)
          {
            break;
          }
        }
      }
      self.read(Token::RBra);
      Expr::new(ExprIs::BuiltinCall(to_s(name), parms))
    /*
          if agg_allowed && name == "COUNT"
          {
            if ( parms.Count > 0 { return self.error( "COUNT does have any parameters" ); }
            result = new COUNT();
          }
          else if ( agg_allowed && name == "SUM" ) result = new ExpAgg( AggOp.Sum, parms, this );
          else if ( agg_allowed && name == "MIN" ) result = new ExpAgg( AggOp.Min, parms, this );
          else if ( agg_allowed && name == "MAX" ) result = new ExpAgg( AggOp.Max, parms, this );
          else if ( name == "PARSEINT" ) result = new PARSEINT( parms, this );
          else if ( name == "PARSEDOUBLE" ) result = new PARSEDOUBLE( parms, this );
          else if ( name == "PARSEDECIMAL" ) result = new PARSEDECIMAL( parms, this );
          else if ( name == "LEN" ) result = new LEN( parms, this );
          else if ( name == "REPLACE" ) result = new REPLACE( parms, this );
          else if ( name == "SUBSTRING" ) result = new SUBSTRING( parms, this );
          else if ( name == "EXCEPTION" ) result = new EXCEPTION( parms, this );
          else if ( name == "LASTID" ) result = new LASTID( parms, this );
          else if ( name == "GLOBAL" ) result = new GLOBAL( parms, this );
          else if ( name == "ARG" ) result = new ARG( parms, this );
          else if ( name == "ARGNAME" ) result = new ARGNAME( parms, this );
          else if ( name == "FILEATTR" ) result = new FILEATTR( parms, this );
          else if ( name == "FILECONTENT" ) result = new FILECONTENT( parms, this );
          else Error( "Unknown function : " + name );
        }
    */
    }
    else if name == b"true"
    {
      Expr::new(ExprIs::Const(Value::Bool(true)))
    }
    else if name == b"false"
    {
      Expr::new(ExprIs::Const(Value::Bool(false)))
    }
    else
    {
      let look = self.b.local_map.get(&name);
      if let Some(lnum) = look
      {
        Expr::new(ExprIs::Local(*lnum))
      }
      else
      {
        Expr::new(ExprIs::ColName(to_s(name)))
      }
    }
  }

  /// Parses a primary expression ( basic expression with no operators ).
  fn exp_primary(&mut self, agg_allowed: bool) -> Expr
  {
    let result;
    if self.token == Token::Id
    {
      result = if self.test_id(b"CASE")
      {
        self.exp_case()
      }
      else if self.test_id(b"NOT")
      {
        let e = self.exp_p(10); // Not sure about precedence here.
        Expr::new(ExprIs::Not(Box::new(e)))
      }
      else
      {
        self.exp_id(agg_allowed)
      };
    }
    else if self.test(Token::LBra)
    {
      if self.test_id(b"SELECT")
      {
        result = self.exp_scalar_select();
      }
      else
      {
        let exp = self.exp();
        if self.test(Token::Comma)
        // Operand of IN e.g. X IN ( 1,2,3 )
        {
          let mut list = vec![exp];
          loop
          {
            list.push(self.exp());
            if !self.test(Token::Comma)
            {
              break;
            }
          }
          result = Expr::new(ExprIs::List(list));
        }
        else
        {
          result = exp;
        }
      }
      self.read(Token::RBra);
    }
    else if self.token == Token::String
    {
      result = Expr::new(ExprIs::Const(Value::String(Rc::new(self.ts.clone()))));
      self.read_token();
    }
    else if self.token == Token::Number || self.token == Token::Decimal
    {
      let value = self.decimal_int;
      // if ( DecimalScale > 0 ) value = value * (long)Util.PowerTen( DecimalScale ) + DecimalFrac;
      // result = new Ok( Constant( value, DecimalScale > 0  DTI.Decimal( 18, DecimalScale ) : DataType.Bigint );
      result = Expr::new(ExprIs::Const(Value::Int(value)));
      self.read_token();
    }
    else if self.token == Token::Hex
    {
      if self.cs.len() % 2 == 1
      {
        self.err("Hex literal must have even number of characters");
      }
      let hb = &self.source[self.token_start + 2..self.source_ix - 1];
      result = Expr::new(ExprIs::Const(Value::Binary(Rc::new(util::parse_hex(hb)))));
      self.read_token();
    }
    else if self.test(Token::Minus)
    {
      result = Expr::new(ExprIs::Minus(Box::new(self.exp_p(30))));
    }
    else
    {
      self.err("Expression expected")
    }
    result
  }

  fn exp_or_agg(&mut self) -> Expr
  {
    let pri = self.exp_primary(true);
    self.exp_lp(pri, 0)
  }

  fn exp(&mut self) -> Expr
  {
    self.exp_p(0)
  }

  fn exp_p(&mut self, precedence: i8) -> Expr
  {
    let pr = self.exp_primary(false);
    self.exp_lp(pr, precedence)
  }

  fn exp_lp(&mut self, mut lhs: Expr, precedence: i8) -> Expr
  {
    let (mut t, mut prec_t) = self.get_operator();
    while prec_t >= precedence
    {
      let prec_op = prec_t;
      let op = t;
      self.read_token();
      let mut rhs = self.exp_primary(false);
      let z = self.get_operator();
      t = z.0;
      prec_t = z.1;
      while prec_t > prec_op
      /* or t is right-associative and prec_t == prec_op */
      {
        rhs = self.exp_lp(rhs, prec_t);
        let z = self.get_operator();
        t = z.0;
        prec_t = z.1;
      }
      lhs = Expr::new(ExprIs::Binary(op, Box::new(lhs), Box::new(rhs)));
    }
    lhs
  }

  /// Parse a CASE expression.
  fn exp_case(&mut self) -> Expr
  {
    let mut list = Vec::new();
    while self.test_id(b"WHEN")
    {
      let test = self.exp();
      self.read_id(b"THEN");
      let e = self.exp();
      list.push((test, e));
    }
    if list.is_empty()
    {
      self.err("Empty Case Expression");
    }
    self.read_id(b"ELSE");
    let els = Box::new(self.exp());
    self.read_id(b"END");
    Expr::new(ExprIs::Case(list, els))
  }

  fn exp_scalar_select(&mut self) -> Expr
  {
    let te = self.select_expression(false);
    // if ( te.ColumnCount != 1 ) Error ( "Scalar select must have one column" );
    Expr::new(ExprIs::ScalarSelect(Box::new(te)))
  }

  // End Expression parsing

  // ****************** Table expression parsing

  fn insert_expression(&mut self, expect: usize) -> TableExpression
  {
    if !self.test_id(b"VALUES")
    {
      self.err("VALUES or SELECT expected");
    }
    // else if self.test_id( b"SELECT" ) { self.expressions() } ...
    self.values(expect)
  }

  fn values(&mut self, expect: usize) -> TableExpression
  {
    let mut values = Vec::new();
    loop
    {
      self.read(Token::LBra);
      let mut v = Vec::new();
      loop
      {
        v.push(self.exp());
        if self.test(Token::RBra)
        {
          break;
        }
        if self.token != Token::Comma
        {
          self.err("Comma or closing bracket expected");
        }
        self.read_token();
      }
      if v.len() != expect
      {
        self.err("Wrong number of values");
      }
      values.push(v);
      if !self.test(Token::Comma) && self.token != Token::LBra
      {
        break;
      } // The comma between multiple VALUES is optional.
    }
    TableExpression::Values(values)
  }

  fn te_named_table(&mut self) -> TableExpression
  {
    let schema = self.id();
    self.read(Token::Dot);
    let name = self.id();
    let name = ObjRef { schema, name };
    TableExpression::Base(name)
  }

  fn primary_table_exp(&mut self) -> TableExpression
  {
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
    if self.token != Token::Id
    {
      self.err("Table expected");
    }
    self.te_named_table()
  }

  fn exp_name(&self, exp: &Expr) -> String
  {
    match &exp.exp
    {
      ExprIs::Local(num) => to_s(self.b.locals[*num]),
      ExprIs::ColName(name) => name.to_string(),
      _ => "".to_string(),
    }
  }

  /// Parse a SELECT / SET / FOR expression.
  fn select_expression(&mut self, set_or_for: bool) -> SelectExpression
  {
    let mut exps = Vec::new();
    let mut colnames = Vec::new();
    let mut assigns = Vec::new();
    loop
    {
      if set_or_for
      {
        let local = self.local();
        let op = match self.token
        {
          Token::Equal => AssignOp::Assign,
          Token::VBarEqual => AssignOp::Append,
          _ => panic!("= or |= expected"),
        };
        self.read_token();
        assigns.push((local, op));
      }
      let exp = self.exp_or_agg();
      if self.test_id(b"AS")
      {
        colnames.push(self.id());
      }
      else
      {
        colnames.push(self.exp_name(&exp));
      }
      exps.push(exp);
      if !self.test(Token::Comma)
      {
        break;
      }
    }

    let from = if self.test_id(b"FROM")
    {
      Some(Box::new(self.primary_table_exp()))
    }
    else
    {
      None
    };
    let wher = if self.test_id(b"WHERE") { Some(self.exp()) } else { None };

    let mut orderby = Vec::new();
    if self.test_id(b"ORDER")
    {
      self.read_id(b"BY");
      loop
      {
        let exp = self.exp();
        let desc = if self.test_id(b"DESC")
        {
          true
        }
        else
        {
          self.test_id(b"ASC");
          false
        };
        orderby.push((exp, desc));
        if !self.test(Token::Comma)
        {
          break;
        }
      }
    }
    SelectExpression { colnames, assigns, exps, from, wher, orderby }
  }

  // ****************** Statement parsing

  fn s_select(&mut self)
  {
    let se = self.select_expression(false);
    if !self.parse_only
    {
      let cte = compile_select(self, se);
      self.add(Inst::Select(Box::new(cte)));
    }
  }

  fn s_set(&mut self)
  {
    let se = self.select_expression(true);
    if !self.parse_only
    {
      let cte = compile_select(self, se);
      self.add(Inst::Set(Box::new(cte)));
    }
  }

  fn s_insert(&mut self)
  {
    self.read_id(b"INTO");
    let tr = self.obj_ref();
    self.read(Token::LBra);
    let mut cnames = Vec::new();
    loop
    {
      let cname = self.id_ref();
      if cnames.contains(&cname)
      {
        self.err("Duplicate column name");
      }
      cnames.push(cname);
      if self.test(Token::RBra)
      {
        break;
      }
      if !self.test(Token::Comma)
      {
        self.err("Comma or closing bracket expected");
      }
    }
    let mut src = self.insert_expression(cnames.len());
    if !self.parse_only
    {
      let t = tlook(self, &tr);
      let mut cnums: Vec<usize> = Vec::new();
      {
        for cname in &cnames
        {
          if let Some(cnum) = t.info.get(tos(cname))
          {
            cnums.push(*cnum);
          }
          else
          {
            panic!("Column name '{}' not found", tos(cname))
          }
        }
      }
      let csrc = compile_te(self, &mut src);
      self.dop(DO::Insert(t, cnums, csrc));
    }
  }

  fn s_update(&mut self)
  {
    let t = self.obj_ref();
    self.read_id(b"SET");
    let mut s = Vec::new();
    loop
    {
      let name = self.id();
      self.read(Token::Equal);
      let exp = self.exp();
      s.push((name, exp));
      if !self.test(Token::Comma)
      {
        break;
      }
    }
    if !self.test_id(b"WHERE")
    {
      self.err("UPDATE must have a WHERE");
    }
    let mut w = self.exp();
    if !self.parse_only
    {
      let t = tlook(self, &t);
      let from = CTableExpression::Base(t.clone());
      let save = mem::replace(&mut self.from, Some(from));

      let w = cexp_bool(self, &mut w);
      let mut se = Vec::new();
      for (name, mut exp) in s
      {
        if let Some(cnum) = t.info.colmap.get(&name)
        {
          let exp = cexp_value(self, &mut exp);
          se.push((*cnum, exp));
        }
        else
        {
          panic!("update column name not found");
        }
      }
      self.from = save;
      self.dop(DO::Update(t, se, w));
    }
  }

  fn s_delete(&mut self)
  {
    self.read_id(b"FROM");
    let tname = self.obj_ref();
    if !self.test_id(b"WHERE")
    {
      self.err("DELETE must have a WHERE");
    }
    let mut w = self.exp();

    if !self.parse_only
    {
      let t = tlook(self, &tname);
      let from = CTableExpression::Base(t.clone());

      let save = mem::replace(&mut self.from, Some(from));
      let w = cexp_bool(self, &mut w);
      self.from = save;
      self.dop(DO::Delete(t, w));
    }
  }

  fn s_execute(&mut self)
  {
    self.read(Token::LBra);
    let mut exp = self.exp();
    self.read(Token::RBra);
    if !self.parse_only
    {
      push(self, &mut exp);
      self.add(Inst::Execute);
    }
  }

  fn s_exec(&mut self)
  {
    let mut pname = self.id();
    let mut sname = "".to_string();
    if self.test(Token::Dot)
    {
      sname = pname;
      pname = self.id();
    }
    let name = ObjRef { schema: sname, name: pname };
    self.read(Token::LBra);

    let mut ptypes = Vec::new();
    if !self.test(Token::RBra)
    {
      let mut e = self.exp();
      ptypes.push(push(self, &mut e));
      while self.test(Token::Comma)
      {
        let mut e = self.exp();
        ptypes.push(push(self, &mut e));
      }
      self.read(Token::RBra);
    }
    if !self.parse_only
    {
      let rp = rlook(self, &name);
      self.check_types(&rp, &ptypes);
      self.add(Inst::Call(rp));
    }
  }

  /// Parse FOR statement.
  fn s_for(&mut self)
  {
    let se: SelectExpression = self.select_expression(true);

    let for_id = self.b.local_typ.len();
    self.b.local_typ.push(NONE);

    if !self.parse_only
    {
      let start_id;
      let break_id = self.get_jump_id();
      let mut cse = compile_select(self, se);
      let orderbylen = cse.orderby.len();
      if orderbylen == 0
      {
        self.add(Inst::ForInit(for_id, Box::new(cse.from.unwrap())));
        start_id = self.get_loop_id();
        let info = Box::new(ForNextInfo { for_id, assigns: cse.assigns, exps: cse.exps, wher: cse.wher });
        self.add(Inst::ForNext(break_id, info));
      }
      else
      {
        let assigns = mem::take(&mut cse.assigns);
        self.add(Inst::ForSortInit(for_id, Box::new(cse)));
        start_id = self.get_loop_id();
        let info = Box::new((for_id, orderbylen, assigns));
        self.add(Inst::ForSortNext(break_id, info));
      }

      let save = self.b.break_id;
      self.b.break_id = break_id;
      self.statement();
      self.b.break_id = save;
      self.add(Inst::Jump(start_id));
      self.set_jump(break_id);
    }
  }

  // ****************** Parse Create statements.

  fn create_table(&mut self)
  {
    let name = self.obj_ref();
    let source_start = self.source_ix - 2;
    self.read(Token::LBra);
    let mut ti = ColInfo::empty(name);
    loop
    {
      let cname = self.id();
      let typ = self.read_data_type();
      if ti.add(cname, typ)
      {
        self.err("Duplicate column name");
      }
      if self.test(Token::RBra)
      {
        break;
      }
      if self.token != Token::Comma
      {
        self.err("Comma or closing bracket expected");
      }
      self.read_token();
    }
    if !self.parse_only
    {
      let _source = self.source_from(source_start, self.token_start);
      self.dop(DO::CreateTable(ti));
    }
  }

  fn create_index(&mut self)
  {
    let iname = self.id();
    self.read_id(b"ON");
    let tname = self.obj_ref();
    self.read(Token::LBra);
    let mut cnames = Vec::new();
    loop
    {
      cnames.push(self.id());
      if self.test(Token::RBra)
      {
        break;
      }
      if self.token != Token::Comma
      {
        self.err("Comma or closing bracket expected")
      };
      self.read_token();
    }
    if !self.parse_only
    {
      let mut cols = Vec::new();
      let table = tlook(self, &tname);
      for cname in &cnames
      {
        if let Some(cnum) = table.info.colmap.get(cname)
        {
          cols.push(*cnum);
        }
        else
        {
          panic!("index column name not found {}", cname);
        }
      }
      self.dop(DO::CreateIndex(IndexInfo { tname, iname, cols }));
    }
  }

  fn create_view(&mut self, alter: bool)
  {
    let r = self.obj_ref();
    self.read_id(b"AS");
    let source_start = self.token_start;
    self.read_id(b"SELECT");
    let _se = self.select_expression(false);
    let source = self.source_from(source_start, self.token_start);
    if !self.parse_only
    {
      self.dop(DO::CreateView(r, alter, source));
    }
  }

  fn create_function(&mut self, alter: bool)
  {
    let rref: ObjRef = self.obj_ref();
    let source_start: usize = self.source_ix - 2;
    let save: Block = mem::replace(&mut self.b, Block::new());
    let save2: bool = self.parse_only;
    self.parse_only = true;
    self.parse_function();
    let _cb: Block = mem::replace(&mut self.b, save);
    self.parse_only = save2;

    if !self.parse_only
    {
      let source: String = self.source_from(source_start, self.token_start);
      self.dop(DO::CreateFunction(rref, Rc::new(source), alter));
    }
  }

  fn s_create(&mut self)
  {
    match self.id_ref()
    {
      b"FUNCTION" => self.create_function(false),
      b"TABLE" => self.create_table(),
      b"VIEW" => self.create_view(false),
      b"SCHEMA" =>
      {
        let name = self.id();
        self.dop(DO::CreateSchema(name));
      }
      b"INDEX" => self.create_index(),
      _ => self.err("Unknown keyword"),
    }
  }

  fn s_alter(&mut self)
  {
    match self.id_ref()
    {
      b"FUNCTION" => self.create_function(true),
      b"TABLE" => self.s_alter_table(),
      b"VIEW" => self.create_view(true),
      _ => self.err("ALTER : TABLE,VIEW.. expected"),
    }
  }

  fn s_drop(&mut self)
  {
    match self.id_ref()
    {
      b"TABLE" =>
      {
        let tr = self.obj_ref();
        self.dop(DO::DropTable(tr));
      }
      b"VIEW" =>
      {
        let vr = self.obj_ref();
        self.dop(DO::DropView(vr));
      }
      b"INDEX" =>
      {
        let ix = self.id();
        self.read_id(b"ON");
        let tr = self.obj_ref();
        self.dop(DO::DropIndex(tr, ix));
      }
      b"PROCEDURE" =>
      {
        let pr = self.obj_ref();
        self.dop(DO::DropProcedure(pr));
      }
      b"FUNCTION" =>
      {
        let fr = self.obj_ref();
        self.dop(DO::DropFunction(fr));
      }
      b"SCHEMA" =>
      {
        let s = self.id();
        self.dop(DO::DropSchema(s));
      }
      _ =>
      {
        self.err("DROP : TABLE,VIEW.. expected");
      }
    }
  }

  fn s_rename(&mut self)
  {
    match self.id_ref()
    {
      b"SCHEMA" =>
      {
        let s = self.id();
        self.read_id(b"TO");
        let t = self.id();
        self.dop(DO::RenameSchema(s, t));
      }
      b"TABLE" =>
      {
        let o = self.obj_ref();
        self.read_id(b"TO");
        let n = self.obj_ref();
        self.dop(DO::Renasysble(o, n));
      }
      b"VIEW" =>
      {
        let o = self.obj_ref();
        self.read_id(b"TO");
        let n = self.obj_ref();
        self.dop(DO::RenameView(o, n));
      }
      b"PROCEDURE" =>
      {
        let o = self.obj_ref();
        self.read_id(b"TO");
        let n = self.obj_ref();
        self.dop(DO::RenameProcedure(o, n));
      }
      b"FUNCTION" =>
      {
        let o = self.obj_ref();
        self.read_id(b"TO");
        let n = self.obj_ref();
        self.dop(DO::RenameFunction(o, n));
      }
      _ =>
      {
        self.err("RENAME : TABLE,VIEW.. expected");
      }
    }
  }

  fn s_alter_table(&mut self)
  {
    let tr = self.obj_ref();
    let mut list = Vec::new();
    loop
    {
      if self.test_id(b"ADD")
      {
        let col = self.id();
        let datatype = self.read_data_type();
        list.push(AlterAction::Add(col, datatype));
      }
      else if self.test_id(b"DROP")
      {
        let col = self.id();
        list.push(AlterAction::Drop(col));
      }
      else if self.test_id(b"RENAME")
      {
        let col = self.id();
        self.read_id(b"TO");
        let to = self.id();
        list.push(AlterAction::Rename(col, to));
      }
      else if self.test_id(b"MODIFY")
      {
        let col = self.id();
        let datatype = self.read_data_type();
        list.push(AlterAction::Modify(col, datatype));
      }
      else
      {
        break;
      }
      if !self.test(Token::Comma)
      {
        break;
      }
    }
    self.dop(DO::AlterTable(tr, list));
  }

  // Helper functions for other statements.

  /// Define a local variable ( parameter or declared ).
  fn def_local(&mut self, name: &'a [u8], dt: DataType)
  {
    let local_id = self.b.local_typ.len();
    self.b.local_typ.push(dt);
    self.b.locals.push(name);
    if self.b.local_map.contains_key(name)
    {
      self.err("Duplicate variable name");
    }
    self.b.local_map.insert(name, local_id);
  }

  fn get_jump_id(&mut self) -> usize
  {
    let result = self.b.jumps.len();
    self.b.jumps.push(usize::MAX);
    result
  }

  fn set_jump(&mut self, jump_id: usize)
  {
    self.b.jumps[jump_id] = self.b.ilist.len();
  }

  fn get_loop_id(&mut self) -> usize
  {
    let result = self.get_jump_id();
    self.set_jump(result);
    result
  }

  fn get_goto(&mut self, s: &'a [u8]) -> usize
  {
    let v = self.b.labels.get(s);
    match v
    {
      Some(jump_id) => *jump_id,
      None =>
      {
        let jump_id = self.get_jump_id();
        self.b.labels.insert(s, jump_id);
        jump_id
      }
    }
  }

  // Other statements.

  fn s_declare(&mut self)
  {
    loop
    {
      let name = self.id_ref();
      let dt = self.read_data_type();
      self.def_local(name, dt);
      if !self.test(Token::Comma)
      {
        break;
      }
    }
  }

  fn s_set_label(&mut self, s: &'a [u8])
  {
    let v = self.b.labels.get(s);
    match v
    {
      Some(jump_id) =>
      {
        let j = *jump_id;
        if self.b.jumps[j] != usize::MAX
        {
          self.err("Label already set");
        }
        else
        {
          self.set_jump(j);
        }
      }
      None =>
      {
        let jump_id = self.get_loop_id();
        self.b.labels.insert(s, jump_id);
      }
    }
  }

  fn s_while(&mut self)
  {
    let mut exp = self.exp();
    let start_id = self.get_loop_id();
    let break_id = self.get_jump_id();
    if !self.parse_only
    {
      let exp = cexp_bool(self, &mut exp);
      self.add(Inst::JumpIfFalse(break_id, exp));
      let save = self.b.break_id;
      self.b.break_id = break_id;
      self.statement();
      self.b.break_id = save;
      self.add(Inst::Jump(start_id));
      self.set_jump(break_id);
    }
  }

  fn s_if(&mut self)
  {
    let mut exp = self.exp();
    let false_id = self.get_jump_id();
    if !self.parse_only
    {
      let exp = cexp_bool(self, &mut exp);
      self.add(Inst::JumpIfFalse(false_id, exp));
    }
    self.statement();
    if self.test_id(b"ELSE")
    {
      let end_id = self.get_jump_id();
      self.add(Inst::Jump(end_id)); // Skip over the else clause
      self.set_jump(false_id);
      self.statement();
      self.set_jump(end_id);
    }
    else
    {
      self.set_jump(false_id);
    }
  }

  fn s_goto(&mut self)
  {
    let label = self.id_ref();
    let to = self.get_goto(label);
    self.add(Inst::Jump(to));
  }

  fn s_break(&mut self)
  {
    let break_id = self.b.break_id;
    if break_id == usize::MAX
    {
      self.err("No enclosing loop for break");
    }
    self.add(Inst::Jump(break_id));
  }

  fn s_return(&mut self)
  {
    if self.b.return_type != NONE
    {
      let mut e = self.exp();
      if !self.parse_only
      {
        let t = data_kind(push(self, &mut e));
        let rt = data_kind(self.b.return_type);
        if t != rt
        {
          panic!("Return type mismatch expected {:?} got {:?}", rt, t)
        }
        self.add(Inst::PopToLocal(self.b.param_count));
      }
    }
    self.add(Inst::Return);
  }

  fn s_throw(&mut self)
  {
    let mut msg = self.exp();
    if !self.parse_only
    {
      push(self, &mut msg);
      self.add(Inst::Throw);
    }
  }

  fn s_begin(&mut self)
  {
    while !self.test_id(b"END")
    {
      self.statement();
    }
  }
} // end impl Parser

/// Convert byte ref to &str.
pub(crate) fn tos(s: &[u8]) -> &str
{
  str::from_utf8(s).unwrap()
}

/// Convert byte ref to String.
pub(crate) fn to_s(s: &[u8]) -> String
{
  str::from_utf8(s).unwrap().to_string()
}
