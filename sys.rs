use std::{ rc::Rc, cell::{Cell,RefCell} };
use crate::{*,sql::*,run::*,table::{Table,Id,Zero,TableInfo,TablePtr}};

/// Creates a schema in the database by writing to the Schema system table.
pub fn create_schema( db: &DB, name: &str )
{
  if let Some(_id) = get_schema( db, name )
  {
    panic!( "Schema '{}' already exists", name );
  }
  let t = &db.sys_schema;
  let mut row = t.row();
  row.id = t.alloc_id() as i64;
  row.values[0] = Value::String( Rc::new( name.to_string() ) );
  t.file.insert( db, &row );
}

/// Creates a routine in the database by inserting the source into the Routine system table.
pub fn create_routine( db: &DB, name: &ObjRef, source: Rc<String>, alter: bool )
{
  if alter
  {
    alter_routine( db, name, source );
  }
  else if let Some(schema_id) = get_schema( db, &name.schema )
  {
    let t = db.load_table( &ObjRef::new( "sys", "Routine" ) ).unwrap();
    let mut row = t.row();
    // Columns are Schema, Name, Definition
    row.id = t.alloc_id() as i64;
    row.values[0] = Value::Int( schema_id );
    row.values[1] = Value::String( Rc::new( name.name.clone() ) );
    row.values[2] = Value::String( source );
    t.file.insert( db, &row );
  }
  else
  {
    panic!( "schema [{}] not found", &name.schema );
  }
}

/// Create a new table in the database by writing to the system Table and Col tables.
pub fn create_table( db: &DB, info: Rc<TableInfo> )
{
  if let Some(_t) = get_table( db, &info.name )
  {
    panic!( "Table [{}].[{}] already exists", info.name.schema, info.name.name );
  }
  let tid =
  {
    if let Some(schema_id) = get_schema( db, &info.name.schema )
    {
      let t = &db.sys_table;
      let mut row = t.row();
      // Columns are root, schema, name, is_view, definition, id_alloc
      row.id = t.alloc_id() as i64;
      row.values[0] = Value::Int( db.file.alloc_page() as i64 );
      row.values[1] = Value::Int( schema_id );
      row.values[2] = Value::String( Rc::new( info.name.name.clone() ) );
      row.values[3] = Value::Bool( false );
      row.values[4] = Value::String( Rc::new( String::new() ) );
      row.values[5] = Value::Int( 0 );
      t.file.insert( db, &row );
      row.id
    }
    else
    {
      panic!( "Schema not found" );
    }
  };
  {
    let cnames = &info.colnames;
    let t = &db.sys_col;
    let mut row = t.row();
    row.values[0] = Value::Int( tid );
    for (num,typ) in info.types.iter().enumerate()
    {
      // Columns are Table, Name, Type
      row.id = t.alloc_id();
      row.values[1] = Value::String( Rc::new( cnames[ num ].to_string() ) );
      row.values[2] = Value::Int( *typ as i64 );
      t.file.insert( db, &row );
    }
  }
}

/// Gets the id of a schema from a name by searching the schema table.
fn get_schema( db: &DB, sname:&str ) -> Option<i64>
{
  let t = &db.sys_schema;
  for ( p, off ) in t.file.asc( db, Box::new(Zero{}) )
  {
    let p = p.borrow();
    let a = t.access( &p, off );
    if a.str( db, 0 ) == sname
    {
      return Some( a.id() ); 
    }
  }
  None
}

fn get_table0( db: &DB, name: &ObjRef ) -> Option< (i64,i64,i64) >
{
  if let Some(schema_id) = get_schema( db, &name.schema )
  {
    let t = &db.sys_table;
    for ( p, off ) in t.file.asc( db, Box::new(Zero{}) )
    {
      let p = p.borrow();
      let a = t.access( &p, off );
      // Columns are root, schema, name, is_view, definition, id_alloc
      if a.int( 1 ) == schema_id && a.str( db, 2 ) == name.name
      {   
        return Some( ( a.id(), a.int(0), a.int( 5 ) ) );
      }
    }
  }
  None
}

/// Gets a table from the database.
pub(crate) fn get_table( db: &DB, name: &ObjRef ) -> Option< TablePtr >
{ 
  if let Some( ( tid, root, id_alloc ) ) = get_table0( db, name )
  {
    let mut info = TableInfo::empty( name.clone() );
    let t = &db.sys_col;
    for ( p, off ) in t.file.asc( db, Box::new(Zero{}) )
    {
      let p = p.borrow();
      let a = t.access( &p, off );
      // Columns are Table, Name, Type
      if a.int( 0 ) == tid
      {   
        let cname = a.str( db, 1 );
        let ctype = a.int( 2 ) as DataType; 
        info.add( cname, ctype );        
      }
    }
    let tptr = Table::new( tid, root as u64, id_alloc, Rc::new(info) );
    db.publish_table( tptr.clone() );
    // println!( "got table {:?}", name );
    Some( tptr )
  }
  else { None }
}

/// Gets a routine from the database, and partially parse it ( parameters and return type ).
pub(crate) fn get_routine( db: &DB, name: &ObjRef ) -> Option< RoutinePtr >
{ 
  if let Some(schema_id) = get_schema( db, &name.schema )
  {
    let t = db.load_table( &ObjRef::new( "sys", "Routine" ) ).unwrap();
    for  ( p, off ) in t.file.asc( db, Box::new(Zero{}) )
    {
      let p = p.borrow();
      let a = t.access( &p, off );
      // Columns are Schema, Name, Definition
      if a.int( 0 ) == schema_id && a.str( db, 1 ) == name.name
      {   
        let source = Rc::new( a.str( db, 2 ) );
        let rptr = parse_routine( db, source ); 
        db.publish_routine( name, rptr.clone() );
        // println!( "got routine {:?}", name );
        return Some( rptr );
      }
    }
  }
  None
}

/// Alter routine definition.
pub(crate) fn alter_routine( db: &DB, name: &ObjRef, source:Rc<String> )
{
  if let Some(schema_id) = get_schema( db, &name.schema )
  {
    let t = db.load_table( &ObjRef::new( "sys", "Routine" ) ).unwrap();
    for  ( p, off ) in t.file.asc( db, Box::new(Zero{}) )
    {
      let mut p = p.borrow_mut();
      let a = t.access( &p, off );
      // Columns are Schema, Name, Definition
      if a.int( 0 ) == schema_id && a.str( db, 1 ) == name.name
      {   
        let oldid = a.int( 2 );
        db.delcode( oldid as u64 );
        let newid = db.encode( source.as_bytes() );
        drop( a );
        let mut w = t.write_access( &mut p, off );
        w.set_int( 2, newid as i64 );
        p.dirty = true;
        db.routines_dirty.set( true );
        return;
      }
    }
  }
  panic!( "routine {:?} not found", name );
}  

/// Parse a routine definition.
fn parse_routine( db: &DB, source: Rc<String> ) -> RoutinePtr
{
  let mut p = sqlparse::Parser::new( &source, db );
  p.parse_only = true;
  p.parse_routine( 0 );
  Rc::new( Routine
  {
    compiled: Cell::new(false),
    ilist: RefCell::new( Vec::new() ),
    local_types: p.b.local_types,
    return_type: p.b.return_type,
    param_count: p.b.param_count,
    source,
  } )
}

/// Update the alloc_id field for a table.
pub(crate) fn save_alloc( db: &DB, id: i64, val: i64 )
{
  let t = &db.sys_table;
  let ( p, off ) = t.file.get( db, &Id{id} ).unwrap();
  let mut p = p.borrow_mut();
  let mut wa = t.write_access( &mut p, off );
  wa.set_int( 5, val );
  p.dirty = true;
}

/// This is only needed to initialise system tables.
pub(crate) fn get_alloc( db: &DB, id: i64 ) -> i64
{
  let t = &db.sys_table;
  let ( p, off ) = t.file.get( db, &Id{id} ).unwrap();
  let p = p.borrow();
  let a = t.access( &p, off );
  a.int(5)
}
