use crate::{ Value, util, sys, Query, DB, 
  sql::{DataKind, DataType, NONE, data_kind, AssignOp, Assigns }, 
  compile::CExpPtr, table::{TablePtr,Row}, run::* };

/// Evaluation environment - stack of Values, references to DB and Query.
pub struct EvalEnv <'r>
{
  pub stack: Vec<Value>,
  pub bp: usize, // "Base Pointer" - used to access local variables.
  pub db: DB,
  pub qy: &'r mut dyn Query,
  pub call_depth: usize,
}

impl <'r> EvalEnv <'r>
{
  /// Construct a new EvalEnv.
  pub(crate) fn new( db: DB, qy: &'r mut dyn Query ) -> Self
  {
    EvalEnv{ stack: Vec::new(), bp:0, db, qy, call_depth:0 }
  }

  /// Allocate and initialise local variables.
  pub(crate) fn alloc_locals( &mut self, dt: &[DataType], param_count: usize )
  {
    for d in dt.iter().skip(param_count)
    {
      let v = default( *d );
      self.stack.push( v );
    }
  }

  /// Execute list of instructions.
  pub(crate) fn go( &mut self, ilist: &[Inst] )
  {
    let n = ilist.len();
    let mut ip = 0;
    while ip < n
    {
      let i = &ilist[ ip ];
      ip += 1;
      match i
      {
        Inst::DataOp( x ) =>
          self.exec_do( x ),
        Inst::Call( x ) => 
          self.call( &*( *x ) ),
        Inst::Jump( x ) => 
          ip = *x,
        Inst::JumpIfFalse( x, e ) => 
          if !e.eval( self, &[] ) 
            { ip = *x; }
        Inst::Return => 
          break,
        Inst::PopToLocal( x ) => 
          self.pop_to_local( *x ),
        Inst::PushValue( e ) => 
          { let v = e.eval( self, &[] ); self.stack.push( v ); }
        Inst::ForInit( for_id, cte ) =>
          { self.for_init( *for_id, cte ); }
        Inst::ForNext( break_id, info ) =>
          { if !self.for_next( info ) { ip = *break_id; } }
        Inst::ForSortInit( for_id, cte ) =>
          { self.for_sort_init( *for_id, cte ); }
        Inst::ForSortNext( break_id, info ) =>
          { if !self.for_sort_next( info ) { ip = *break_id; } }     
        Inst::Select( cse ) =>
          { self.select( cse ); }
        Inst::Set( cse ) =>
          { self.set( cse ); }
        Inst::Execute => 
          { self.execute(); }
        // Special push instructions ( optimisations )
        Inst::PushInt( e ) => 
          { let v = e.eval( self, &[] ); self.stack.push( Value::Int(v) ); }
        Inst::_PushFloat( e ) => 
          { let v = e.eval( self, &[] ); self.stack.push( Value::Float(v) ); }
        Inst::PushBool( e ) => 
          { let v = e.eval( self, &[] ); self.stack.push( Value::Bool(v) ); }
        Inst::PushLocal( x ) => 
          self.push_local( *x ),
        Inst::PushIntConst( x ) => 
          self.push_int( *x ),
        Inst::PushConst( x ) => 
          self.stack.push( (*x).clone() ),
        Inst::Throw => 
          { let s = self.pop_string(); panic!( "{}", s ); }
      }
    }
  } // end fn go

  /// Discard n items from stack.
  fn discard( &mut self, mut n: usize )
  {   
    while n > 0 { self.stack.pop(); n-=1; }
  }

  /// Call a function.
  pub(crate) fn call( &mut self, r: &Function )
  {
    self.call_depth += 1;
/*
    if let Some(n) = stacker::remaining_stack()
    {
      if n < 64 * 1024 { panic!("Stack less than 64k call depth={}", self.call_depth) }
    }
    else 
*/ 
    if self.call_depth > 500
    {
      panic!("Call depth limit of 500 reached");
    }

    let save_bp = self.bp;
    self.bp = self.stack.len() - r.param_count;          
    self.alloc_locals( &r.local_types, r.param_count );          
    self.go( &r.ilist.borrow() );
    let pop_count = r.local_types.len();
    if pop_count > 0
    {
      if r.return_type != NONE
      {
        if r.param_count == 0 // function result already in correct position.
        {
          self.discard( pop_count - 1 );
        }
        else
        {
          let result = self.stack[  self.bp + r.param_count ].clone();
          self.discard( pop_count );
          self.stack.push( result );
        }
      }
      else { self.discard( pop_count ); }
    }
    self.bp = save_bp;
    self.call_depth -= 1;
  }

  /// Push an integer literal onto the stack.
  fn push_int( &mut self, x: i64 )
  {
    self.stack.push( Value::Int( x ) );
  }

  /// Pop a value from the stack and assign it to a local varaiable.
  fn pop_to_local( &mut self, local: usize )
  {
    self.stack[ self.bp + local ] = self.stack.pop().unwrap();
  }

  /// Pop string from the stack.
  fn pop_string( &mut self ) -> String
  {
    if let Value::String(s) = self.stack.pop().unwrap()
    {
      s.to_string()
    }
    else { panic!() }    
  }

  /// Push clone of local variable onto the stack.
  fn push_local( &mut self, local: usize )
  {
    self.stack.push( self.stack[ self.bp + local ].clone() );
  }

  /// Execute a ForInit instruction. Constructs For state and assigns it to local variable.
  fn for_init( &mut self, for_id: usize, cte: &CTableExpression )
  {
    let data_source = self.data_source( cte );
    let fs = util:: new( ForState{ data_source } );
    self.stack[ self.bp + for_id ] = Value::For(fs);
  }

  /// Execute a ForNext instruction. Fetches a record from underlying file that satisfies the where condition, 
  /// evaluates the expressions and assigns the results to local variables. 
  fn for_next( &mut self, info: &ForNextInfo ) -> bool
  {
    loop
    {
      let next = if let Value::For(fs) = &self.stack[ self.bp + info.for_id ]
      {
        fs.borrow_mut().data_source.next()
      } else { panic!( "Jump into FOR loop"); };

      if let Some( ( p, off ) ) = next
      {
        let p = p.borrow();
        let data = &p.data[off..];

        // Eval and check WHERE condition, eval expressions and assign to locals.
        if if let Some(w) = &info.wher { w.eval( self, data ) } else { true }
        {
          for (i,a) in info.assigns.iter().enumerate()
          {
            let val = info.exps[i].eval( self, data );
            self.assign_local( a, val );
          }
          return true;
        }
      } else { return false; }
    }
  }

  /// Execute ForSortInit instruction. Constructs sorted vector of rows.
  fn for_sort_init( &mut self, for_id: usize, cse: &CSelectExpression )
  {
    let rows = self.get_temp( cse );
    self.stack[ self.bp + for_id ] = Value::ForSort( util::new( ForSortState{ ix:0, rows } ) );
  }

  /// Execute ForSortNext instruction. Assigns locals from current row, moves to next row.
  fn for_sort_next( &mut self, info: &(usize,usize,Assigns) ) -> bool
  {
    let ( for_id, orderbylen, assigns ) = info;
    if let Value::ForSort(fs) = &self.stack[ self.bp + for_id ]
    {
      let fs = fs.clone();
      let mut fs = fs.borrow_mut();
      if fs.ix == fs.rows.len() 
      { 
        false
      }
      else
      {
        fs.ix += 1;
        let row = &fs.rows[ fs.ix - 1 ];
        for (cn,a) in assigns.iter().enumerate()
        {
          let val = row[ orderbylen + cn ].clone();
          self.assign_local( a, val );
        }
        true
      }
    }
    else { panic!( "Jump into FOR loop"); }
  }

  /// Execute SQL string.
  fn execute( &mut self )
  {
    let s = self.pop_string();
    self.db.run( &s, self.qy );
  }

  /// Execute a data operation (DO).
  fn exec_do( &mut self, dop: &DO )
  {
    match dop
    {
      DO::CreateFunction( name, source, alter ) =>
      {
        sys::create_function( &self.db, name, source.clone(), *alter );
      }
      DO::CreateSchema( name ) =>
      {
        sys::create_schema( &self.db, name );
      }
      DO::CreateTable( ti ) => 
      {
        sys::create_table( &self.db, ti );
      }
      DO::CreateIndex( x ) => 
      {
        sys::create_index( &self.db, x );
      }
      DO::Insert( tp, cols, values ) => 
      {
        self.insert( tp.clone(), cols, values );
      }
      DO::Delete( tp, wher ) =>
      {
        self.delete( tp, wher )
      }
      DO::Update( tp, assigns, wher ) =>
      {
        self.update( tp, assigns, wher )
      }
      _ => 
      {
        panic!()
      }
    }
  }

  /// Get list of record ids for DELETE/UPDATE.
  fn get_id_list( &mut self, t: &TablePtr, w: &CExpPtr<bool> ) -> Vec<u64>
  {
    let mut idlist = Vec::new();
    for ( p, off ) in t.scan( &self.db )
    {
      let p = p.borrow();
      let data = &p.data[off..];
      if w.eval( self, data )
      {
        idlist.push( util::getu64( data, 0 ) );
      }
    }
    idlist
  }

  /// Execute INSERT operation.
  fn insert( &mut self, t: TablePtr, cols: &[usize], src: &CTableExpression )
  {
    if let CTableExpression::Values(x) = src
    {
      self.insert_values( t, cols, x );
    }
    else
    {
      panic!();
    }
  }

  /// Execute a DELETE operation.
  fn delete( &mut self, t: &TablePtr, w: &CExpPtr<bool> )
  {
    let idlist = self.get_id_list( t, w );
    let mut row = t.row();
    let mut codes = Vec::new();
    for id in idlist
    {
      row.id = id as i64; 
      if let Some( ( p, off ) ) = t.id_get( &self.db, id )
      {
        let p = p.borrow_mut();
        // Delete any codes no longer in use.
        for (i,typ) in t.info.types.iter().enumerate()
        {
          match data_kind( *typ )
          {
            DataKind::String | DataKind::Binary =>
            {
              let u = util::getu64( &p.data, off + t.info.off[i] );
              codes.push( u );
            }
            _ => {}
          }
        }
      }
      t.remove( &self.db, &row );
    }
    for u in codes
    {
      self.db.delcode( u );
    }
  }

  /// Execute an UPDATE operation.
  fn update( &mut self, t: &TablePtr, assigns:&[(usize,CExpPtr<Value>)], w: &CExpPtr<bool> )
  {
    let idlist = self.get_id_list( t, w );
    let mut row = t.row();
    for id in idlist
    {
      row.id = id as i64;
      if let Some( ( p, off ) ) = t.id_get( &self.db, id )
      {
        let mut p = p.borrow_mut();
        p.dirty = true;
        let data = &mut p.data[off..];
        // ToDo: update indexes. Also this code belongs in table module.
        for ( col, exp ) in assigns // Maybe should calculate all the values before doing any updates.
        {
          let col = *col;
          let off = t.info.off[ col ];
          let val = exp.eval( self, data );
          match val
          {
            Value::Int(val) => { util::set( data, off, val as u64, t.info.sizes[ col ] ); }
            Value::String(val) => 
            { 
              let old_id = util::getu64( data, off );
              let old_str = String::from_utf8( self.db.decode( old_id ) ).unwrap();
              if old_str != *val
              {
                self.db.delcode( old_id );
                let new_id = self.db.encode( val.as_bytes() );
                util::set( data, off, new_id, 8 );
              } 
            }
            Value::Binary(val) => 
            { 
              let old_id = util::getu64( data, off );
              let old_bin = self.db.decode( old_id );
              if old_bin != *val 
              {
                self.db.delcode( id );
                let id = self.db.encode( &val );
                util::set( data, off, id, 8 );
              } 
            }
            _ => panic!()
          }
        }
      }
    }
  }

  /// Get DataSource from CTableExpression.
  fn data_source( &mut self, te: & CTableExpression ) -> DataSource
  {
    match te
    {
      CTableExpression::Base( t ) => Box::new( t.scan( &self.db ) ),
      CTableExpression::IdGet( t, idexp ) => 
      {
        let id = idexp.eval( self, &[] );
        Box::new( t.scan_id( &self.db, id ) )
      }
      CTableExpression::IxGet( t, val, col ) =>
      {
        let key = val.eval( self, &[] );
        Box::new( t.scan_key( &self.db, *col, key ) )
      }
      _ => panic!()
    }
  }

  /// Execute a SELECT operation.
  fn select( &mut self, cse: &CSelectExpression )
  {   
    if let Some( te ) = &cse.from
    {
      let obl = cse.orderby.len();
      let mut temp = Vec::new(); // For sorting.
      for ( p, off ) in self.data_source( te )
      {
        let p = p.borrow();
        let data = &p.data[off..];
        if if let Some(w) = &cse.wher { w.eval( self, data ) } else { true }
        {
          let mut values = Vec::new();
          if obl > 0
          {
            // Push the sort keys.
            for ce in &cse.orderby
            {
              let val = ce.eval( self, data );
              values.push( val );
            }
          }
          for ce in &cse.exps
          {
            let val = ce.eval( self, data );
            values.push( val );
          }
          if obl > 0 
          {
            temp.push( values ); // Save row for later sorting.
          }
          else // Output directly.
          {
            self.qy.push( &values ); 
          }
        }
      }
      if obl > 0 
      {
        // Sort then output the rows.
        temp.sort_by( |a, b| compare( a, b, &cse.desc ) );
        for r in &temp
        {
          self.qy.push( &r[obl..] );
        }
      }
    }
    else
    {
      let mut values = Vec::new();
      for ce in &cse.exps
      {
        let val = ce.eval( self, &[] );
        values.push( val );
      }
      self.qy.push( &values );
    }
  }

  /// Execute a SET operation.
  fn set( &mut self, cse: &CSelectExpression )
  {    
    if let Some( te ) = &cse.from
    {
      for ( p, off ) in self.data_source( te )
      {
        let p = p.borrow();
        let data = &p.data[off..];

        let ok = if let Some(w) = &cse.wher
        {
          w.eval( self, data )
        }
        else { true };
        if ok
        {
          for (i,ce) in cse.exps.iter().enumerate()
          {
            let val = ce.eval( self, data );
            self.assign_local( &cse.assigns[i], val );
          }
          break; // Only one row is used for SET.
        }
      }
    }
    else
    {
      for (i,ce) in cse.exps.iter().enumerate()
      {
        let val = ce.eval( self, &[] );
        self.assign_local( &cse.assigns[i], val );
      }
    }
  }

  /// Assign or append to a local variable.
  fn assign_local( &mut self, a: &(usize,AssignOp), val: Value )
  {
    let var = &mut self.stack[ self.bp + a.0 ];
    match a.1
    { 
      AssignOp::Assign => { *var = val; }
      AssignOp::Append => { var.append( val ); }
    }
  }

  /// Insert evaluated values into a table.
  fn insert_values( &mut self, table:TablePtr, ci: &[usize], vals: &[Vec<CExpPtr<Value>>] )
  {
    let mut row = Row::new( table.info.clone() );
    for r in vals
    {
      row.id = 0;
      for (i,ce) in r.iter().enumerate()
      {
        let val = ce.eval( self, &[] ); 
        let cn = ci [ i ];
        if cn == usize::MAX
        {
          if let Value::Int( v ) = val
          {
            row.id = v;
          }
        }
        else
        {
          row.values[ cn ] = val;
        }
      }
      if row.id == 0
      { 
        row.id = table.alloc_id();
      }
      else
      {
        table.id_allocated( row.id );
      }
      self.db.lastid.set( row.id ); 
      table.insert( &self.db, &mut row );
    }
  }
  
  /// Get sorted temporary table.
  fn get_temp( &mut self, cse: &CSelectExpression ) -> Vec<Vec<Value>>
  {   
    if let Some( te ) = &cse.from
    {
      let mut temp = Vec::new(); // For sorting.
      for ( p, off ) in self.data_source( te )
      {
        let p = p.borrow();
        let data = &p.data[off..];
        if if let Some(w) = &cse.wher { w.eval( self, data ) } else { true }
        {
          let mut values = Vec::new();
          for ce in &cse.orderby
          {
            let val = ce.eval( self, data );
            values.push( val );
          }
          for ce in &cse.exps
          {
            let val = ce.eval( self, data );
            values.push( val );
          }
          temp.push( values ); // Save row for later sorting.
        }
      }
      // Sort the rows.
      temp.sort_by( |a, b| compare( a, b, &cse.desc ) );
      temp        
    }
    else
    {
      panic!()
    }
  }

} // impl EvalEnv

