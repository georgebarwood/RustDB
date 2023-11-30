#[test]

pub fn rtest() {
    use crate::*;

    const INITSQL : &str = "

CREATE FN sys.QuoteName( s string ) RETURNS string AS
BEGIN
  RETURN '[' | REPLACE( s, ']', ']]' ) | ']'
END

CREATE FN sys.Dot( schema string, name string ) RETURNS string AS
BEGIN
  RETURN sys.QuoteName( schema ) | '.' | sys.QuoteName( name )
END

CREATE FN sys.TableName( table int ) RETURNS string AS
BEGIN
  DECLARE schema int, name string
  SET schema = Schema, name = Name FROM sys.Table WHERE Id = table
  IF name = '' RETURN ''
  SET result = sys.Dot( Name, name ) FROM sys.Schema WHERE Id = schema
END

CREATE FN sys.DropTable
( t int ) AS 
/* Note: this should not be called directly, instead use DROP TABLE statement */
BEGIN
  /* Delete the rows */
  EXECUTE( 'DELETE FROM ' | sys.TableName(t) | ' WHERE true' )

  DECLARE id int
  /* Delete the Index data */
  FOR id = Id FROM sys.Index WHERE Table = t
  BEGIN
    DELETE FROM sys.IndexColumn WHERE Index = id
  END
  DELETE FROM sys.Index WHERE Table = t
   /* Delete the column data */
  FOR id = Id FROM sys.Column WHERE Table = t
  BEGIN
    -- DELETE FROM browse.Column WHERE Id = id
  END
  /* Delete other data */
  -- DELETE FROM browse.Table WHERE Id = t
  DELETE FROM sys.Column WHERE Table = t
  DELETE FROM sys.Table WHERE Id = t
END

CREATE SCHEMA rtest
GO
CREATE TABLE rtest.Gen(x int)
GO
INSERT INTO rtest.Gen(x) VALUES(1)
GO
CREATE SCHEMA rtestdata
GO

CREATE FN rtest.repeat( s string, n int ) RETURNS string AS
BEGIN
  WHILE n > 0
  BEGIN
    SET result |= s
    SET n = n - 1
  END
END

CREATE FN rtest.OneTest() AS
BEGIN 
  DECLARE rtestdata int
  SET rtestdata = Id FROM sys.Schema WHERE Name = 'rtestdata'

  DECLARE r int
  SET r = x FROM rtest.Gen
  SET r = r * 48271 % 2147483647
  UPDATE rtest.Gen SET x = r WHERE true

  DECLARE sql string, a int
  SET a = r % 2

  DECLARE tname string
  SET tname = 't' | ( r / 100 ) % 7

  DECLARE exists string
  SET exists = ''
  SET exists = Name FROM sys.Table WHERE Schema = rtestdata AND Name = tname

  SET sql = CASE 
    WHEN r % 20 = 0 THEN 'SELECT VERIFYDB()'
    WHEN r % 20 = 19 THEN 'SELECT REPACKFILE(-4,'''','''')'
    WHEN r % 20 = 18 THEN 'SELECT REPACKFILE(-3,'''','''')'
    WHEN exists = '' THEN 
      CASE WHEN r % 2 =1 THEN 'CREATE TABLE rtestdata.[' | tname | '](x string, y int(5))'
      ELSE 'CREATE TABLE rtestdata.[' | tname | '](x string, y int(3), z string )'
      END
    WHEN r % 10 = 0 THEN 'DROP TABLE rtestdata.[' | tname | ']'
    WHEN r % 2 = 1 THEN 'INSERT INTO rtestdata.[' | tname | '](x,y) VALUES ( rtest.repeat(''George Gordon Fairbrother Barwood'','|(r % 1000)|'),' | (r % 10) | ')'
    ELSE 'DELETE FROM rtestdata.[' | tname | '] WHERE y = ' | ( r%15)
  END
  
  SELECT ' sql=' | sql

  EXECUTE( sql )
 
END
GO
";

    let file = Box::new(MemFile::default());
    let upd = Box::new(MemFile::default());
    let stg = Box::new(AtomicFile::new(file, upd));

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = Arc::new(SharedPagedData::new(stg));
    {
        //let mut stash = spd.stash.lock().unwrap();
        //stash.mem_limit = 10 * 1024;
        spd.file.write().unwrap().trace = true;
    }

    let wapd = AccessPagedData::new_writer(spd.clone());
    let db = Database::new(wapd, INITSQL, bmap.clone());

    for _i in 0..10000 {
        let mut tr = GenTransaction::default();
        let sql = "EXEC rtest.OneTest()";
        db.run(&sql, &mut tr);
        db.save();
        let s = std::str::from_utf8(&tr.rp.output).unwrap();
        if s.len() > 0 {
            println!("output={}", s);
        }
    }
    // assert!(false);
}

#[test]

pub fn test_rollback() {
    use crate::*;

    let file = Box::new(MemFile::default());
    let upd = Box::new(MemFile::default());
    let stg = Box::new(AtomicFile::new(file, upd));

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = Arc::new(SharedPagedData::new(stg));
    // let mut stash = spd.stash.lock().unwrap();
    // stash.mem_limit = 1 << 20;

    let wapd = AccessPagedData::new_writer(spd.clone());
    let db = Database::new(wapd, "", bmap.clone());

    let mut tr = GenTransaction::default();
    let sql = "
      CREATE TABLE sys.test(x int) 
      DECLARE sql string SET sql = 'SELECT PARSEINT(''x'')'
      EXECUTE(sql)
    ";
    db.run(&sql, &mut tr);
}
