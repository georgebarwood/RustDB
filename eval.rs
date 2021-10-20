use crate::{ Value, util, sys, table, page::PAGE_SIZE, Query, DB, 
  sql::{DK, DataType,NONE,data_kind}, 
  compile::CExpPtr, table::{Zero,TablePtr,Row}, run::* };


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
  /// Construction a new EvalEnv.
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
          if !e.eval( self, &[0] ) 
            { ip = *x; }
        Inst::Return => 
          break,
        Inst::PopToLocal( x ) => 
          self.pop_to_local( *x ),
        Inst::PushValue( e ) => 
          { let v = e.eval( self, &[0] ); self.stack.push( v ); }
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
          { let v = e.eval( self, &[0] ); self.stack.push( Value::Int(v) ); }
        Inst::_PushFloat( e ) => 
          { let v = e.eval( self, &[0] ); self.stack.push( Value::Float(v) ); }
        Inst::PushBool( e ) => 
          { let v = e.eval( self, &[0] ); self.stack.push( Value::Bool(v) ); }
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

  /// Call a routine.
  pub(crate) fn call( &mut self, r: &Routine )
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
          // println!( "EvalEnv::call result={:?}", result );
          self.discard( pop_count );
          self.stack.push( result );
        }
      }
      else { self.discard( pop_count ); }
    }
    self.bp = save_bp;
    self.call_depth -= 1;
    // println!( "stack={:?}", self.stack );
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
    match cte
    {
      CTableExpression::Base( t ) => 
      { 
        let start = Zero{};
        let c = util::new( ForState{ asc: t.file.asc( &self.db, Box::new(start) ) } );
        self.stack[ self.bp + for_id ] = Value::For(c);
      }
      _ => panic!()
    }
  }

  /// Execute a ForNext instruction. Fetches a record from underlying file that satisfies the where condition, 
  /// evaluates the expressions and assigns the results to local variables. 
  fn for_next( &mut self, info: &ForNextInfo ) -> bool
  {
    loop
    {
      let next = if let Value::For(f) = &self.stack[ self.bp + info.for_id ]
      {
        f.borrow_mut().asc.next()
      } else { panic!( "Jump into FOR loop"); };

      if let Some( ( p, off ) ) = next
      {
        let p = p.borrow();
        let data = &p.data[off..PAGE_SIZE];

        // Check WHERE condition, eval expressions and assign to locals.
        if if let Some(w) = &info.wher { w.eval( self, data ) } else { true }
        {
          for i in 0..info.assigns.len()
          {
            let val = info.exps[i].eval( self, data );
            self.stack[ self.bp + info.assigns[i] ] = val;
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
  fn for_sort_next( &mut self, info: &(usize,usize,Vec<usize>) ) -> bool
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
          self.stack[ self.bp + a ] = row[ orderbylen + cn ].clone();
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
    self.db.runtimed( &s, self.qy );
  }

  /// Execute a data operation (DO).
  fn exec_do( &mut self, dop: &DO )
  {
    match dop
    {
      DO::CreateRoutine( name, source, alter ) =>
      {
        sys::create_routine( &self.db, name, source.clone(), *alter );
      }
      DO::CreateSchema( name ) =>
      {
        sys::create_schema( &self.db, name );
      }
      DO::CreateTable( ti ) => 
      {
        sys::create_table( &self.db, ti.clone() );
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
      DO::CreateIndex( _x ) => {}
      _ => 
      {
        panic!();
      }
    }
  }

  /// Get list of record ids for DELETE/UPDATE.
  fn get_id_list( &mut self, t: &TablePtr, w: &CExpPtr<bool> ) -> Vec<u64>
  {
    let mut idlist = Vec::new();
    for ( p, off ) in t.file.asc( &self.db, Box::new(table::Zero{}) )
    {
      let p = p.borrow();
      let data = &p.data[off..PAGE_SIZE];
      if w.eval( self, data )
      {
        idlist.push( util::get64( data, 0 ) );
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
    // println!( "delete idlist={:?}", idlist );
    let mut row = t.row();
    for id in idlist
    {
      row.id = id as i64;    
      if let Some( ( p, off ) ) = t.file.get( &self.db, &row )
      {
        let p = p.borrow_mut();
        // Need to delete any codes no longer in use.
        for i in 0..t.info.types.len()
        {
          match data_kind( t.info.types[i] )
          {
            DK::String | DK::Binary =>
            {
              let u = util::get64( &p.data, off + t.info.off[i] );
              self.db.delcode( u );
            }
            _ => {}
          }
        }
      }
      t.file.remove( &self.db, &row );
    }
  }

  /// Execute an UPDATE operation.
  fn update( &mut self, t: &TablePtr, assigns:&[(usize,CExpPtr<Value>)], w: &CExpPtr<bool> )
  {
    let idlist = self.get_id_list( t, w );
    // println!( "update idlist={:?}", idlist );
    let mut row = t.row();
    for id in idlist
    {
      row.id = id as i64;
      if let Some( ( p, off ) ) = t.file.get( &self.db, &row )
      {
        let mut p = p.borrow_mut();
        p.dirty = true;
        let data = &mut p.data[off..PAGE_SIZE];
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
              let id = util::get64( data, off );
              self.db.delcode( id );
              let id = self.db.encode( val.as_bytes() );
              util::set( data, off, id, 8 ); 
            }
            Value::Binary(val) => 
            { 
              let id = util::get64( data, off );
              self.db.delcode( id );
              let id = self.db.encode( &val );
              util::set( data, off, id, 8 ); 
            }
            _ => panic!()
          }
        }
      }
    }
  }

  /// Execute a SELECT operation.
  fn select( &mut self, cse: &CSelectExpression )
  {   
    if let Some( t ) = &cse.from
    {
      match &*t
      {
        CTableExpression::Base( t ) => 
        {
          let obl = cse.orderby.len();
          let mut temp = Vec::new(); // For sorting.
          let start = Box::new(table::Zero{});
          for ( p, off ) in t.file.asc( &self.db, start )
          {
            let p = p.borrow();
            let data = &p.data[off..PAGE_SIZE];
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
        CTableExpression::Values( _x ) => { panic!() } // Should only occur in INSERT statement.
      }
    }
    else
    {
      let mut values = Vec::new();
      for ce in &cse.exps
      {
        let val = ce.eval( self, &[0] );
        values.push( val );
      }
      self.qy.push( &values );
    }
  }

  /// Execute a SET operation.
  fn set( &mut self, cse: &CSelectExpression )
  {    
    if let Some( t ) = &cse.from
    {
      match &*t
      {
        CTableExpression::Base( t ) => 
        {
          let start = Box::new(table::Zero{});
          for ( p, off ) in t.file.asc( &self.db, start )
          {
            let p = p.borrow();
            let data = &p.data[off..PAGE_SIZE];

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
                self.stack[ self.bp + cse.assigns[i] ] = val;
              }
              break; // Only one row is used for SET.
            }
          }
        }
        CTableExpression::Values( _x ) => { todo!() }
      }
    }
    else
    {
      for (i,ce) in cse.exps.iter().enumerate()
      {
        let val = ce.eval( self, &[0] );
        self.stack[ self.bp + cse.assigns[i] ] = val;
      }
    }
  }

  fn insert_values( &mut self, table:TablePtr, ci: &[usize], vals: &[Vec<CExpPtr<Value>>] )
  {
    let mut row = Row::new( table.info.clone() );
    for r in vals
    {
      row.id = 0;
      for (i,ce) in r.iter().enumerate()
      {
        let val = ce.eval( self, &[0] ); 
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
      if row.id == 0 { row.id = table.alloc_id(); }
      table.file.insert( &self.db, &row );
      // println!( "insert_values row inserted id={} values={:?}", row.id, row.values );
    }
  }
  
  /// Get sorted temporary table.
  fn get_temp( &mut self, cse: &CSelectExpression ) -> Vec<Vec<Value>>
  {   
    if let Some( t ) = &cse.from
    {
      match &*t
      {
        CTableExpression::Base( t ) => 
        {
          let mut temp = Vec::new(); // For sorting.
          let start = Box::new(table::Zero{});
          for ( p, off ) in t.file.asc( &self.db, start )
          {
            let p = p.borrow();
            let data = &p.data[off..PAGE_SIZE];
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
        CTableExpression::Values( _x ) => { panic!() } // Should only occur in INSERT statement.
      }
    }
    else
    {
      panic!()
    }
  }

} // impl EvalEnv
