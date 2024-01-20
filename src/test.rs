#[cfg(test)]
pub fn test_amount() -> usize {
    str::parse(&std::env::var("TA").unwrap_or("1".to_string())).unwrap()
}

#[test]
pub fn concurrency() {
    use crate::*;

    let stg = AtomicFile::new(MemFile::new(), MemFile::new());

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = SharedPagedData::new(stg);
    let wapd = AccessPagedData::new_writer(spd.clone());
    let db = Database::new(wapd, "CREATE SCHEMA test", bmap.clone());

    let nt = 100;

    // Create nt tables.
    for i in 0..nt {
        let mut tr = GenTransaction::default();
        let sql = format!(
            "CREATE TABLE test.[T{}](N int) GO INSERT INTO test.[T{}](N) VALUES (0)",
            i, i
        );
        db.run(&sql, &mut tr);
        assert!(db.save() > 0);
    }

    // Create readers at different update times.
    let mut rapd = Vec::new();
    for i in 0..1000 * test_amount() {
        rapd.push((i, AccessPagedData::new_reader(spd.clone())));
        let mut tr = GenTransaction::default();
        let table = i % nt;
        let sql = format!("UPDATE test.[T{}] SET N = N + 1 WHERE 1=1", table);
        db.run(&sql, &mut tr);
        assert!(db.save() > 0);
    }

    // Run the readers in random order, checking content of random table.
    use rand::Rng;
    let mut rng = rand::thread_rng();
    while !rapd.is_empty() {
        let r = rng.gen::<usize>() % rapd.len();
        let (i, rapd) = rapd.remove(r);
        let db = Database::new(rapd, "", bmap.clone());
        let mut tr = GenTransaction::default();
        let table = rng.gen::<usize>() % nt;
        let sql = format!("SELECT N FROM test.[T{}]", table);
        db.run(&sql, &mut tr);
        let expect = i / nt + if i % nt > table { 1 } else { 0 };
        assert!(tr.rp.output == format!("{}", expect).as_bytes());
    }
}

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

CREATE FN sys.ClearTable
(t int) AS 
BEGIN 
  EXECUTE( 'DELETE FROM ' | sys.TableName(t) | ' WHERE true' )
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
    SET n -= 1
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
    WHEN r % 20 = 17 THEN 'SELECT RENUMBER()'
    WHEN exists = '' THEN 
      CASE WHEN r % 2 =1 THEN 'CREATE TABLE rtestdata.[' | tname | '](x string, y int(5))'
      ELSE 'CREATE TABLE rtestdata.[' | tname | '](x string, y int(3), z string )'
      END
    WHEN r % 5 = 0 THEN 'ALTER TABLE rtestdata.[' | tname | '] ADD [z' | r | '] binary' 
    WHEN r % 21 = 1 THEN 'DROP TABLE rtestdata.[' | tname | ']'
    WHEN r % 2 = 1 THEN 'INSERT INTO rtestdata.[' | tname | '](x,y) VALUES ( rtest.repeat(''George Gordon Fairbrother Barwood'','|(r % 1000)|'),' | (r % 10) | ')'
    ELSE 'DELETE FROM rtestdata.[' | tname | '] WHERE y = ' | ( r%15)
  END
  
  SELECT ' sql=' | sql

  EXECUTE( sql )
 
END
GO
";

    let stg = AtomicFile::new(MemFile::new(), MemFile::new());

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = SharedPagedData::new(stg);
    let wapd = AccessPagedData::new_writer(spd.clone());
    let db = Database::new(wapd, INITSQL, bmap.clone());

    for _i in 0..1000 * test_amount() {
        let mut tr = GenTransaction::default();
        let sql = "EXEC rtest.OneTest()";
        db.run(&sql, &mut tr);
        db.save();
        let s = std::str::from_utf8(&tr.rp.output).unwrap();
        if s.len() > 0 {
            // println!("output={}", s);
        }
    }
    // assert!(false);
}

#[test]
pub fn rollback() {
    use crate::*;

    let stg = AtomicFile::new(MemFile::new(), MemFile::new());

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = SharedPagedData::new(stg);

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

#[test]
pub fn insert_delete() {
    use crate::*;

    let stg = AtomicFile::new(MemFile::new(), MemFile::new());

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = SharedPagedData::new(stg);
    let wapd = AccessPagedData::new_writer(spd.clone());
    let db = Database::new(wapd, "", bmap.clone());

    let mut tr = GenTransaction::default();

    let sql = format!(
        "
      CREATE TABLE sys.test(x int,name string) 
      GO
      DECLARE @i int
      WHILE @i < {}
      BEGIN
        INSERT INTO sys.test(x,name) VALUES(@i,'Hello World')    
        SET @i += 1
      END      
      DELETE FROM sys.test WHERE Id % 3 = 1
      DELETE FROM sys.test WHERE Id % 3 = 2
      DELETE FROM sys.test WHERE true
    ",
        test_amount() * 100000
    );
    db.run(&sql, &mut tr);
    db.save();
    assert_eq!(tr.get_error(), "");
}
