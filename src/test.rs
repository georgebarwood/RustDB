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

    // To check things work with low mem_limit.
    {
        // let mut s = spd.stash.lock().unwrap();
        // s.mem_limit = 1;
    }

    for _i in 0..1000 * test_amount() {
        let mut tr = GenTransaction::default();
        let sql = "EXEC rtest.OneTest()";
        db.run(&sql, &mut tr);
        db.save();
        let s = std::str::from_utf8(&tr.rp.output).unwrap();
        if s.len() > 0 {
            // println!("output={}", s);
        }
        assert_eq!(tr.get_error(), "");
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

//$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

#[test]
fn test_date_calc() {
    use crate::*;
    const INITSQL: &str = "
CREATE SCHEMA [date]
CREATE FN [date].[YearDayToYearMonthDay]( yd int ) RETURNS int AS
BEGIN
  DECLARE y int, d int, leap bool, fdm int, m int, dim int
  SET y = yd / 512
  SET d = yd % 512 - 1
  SET leap = date.IsLeapYear( y )
  -- Jan = 0..30, Feb = 0..27 or 0..28  
  IF NOT leap AND d >= 59 SET d = d + 1
  SET fdm = CASE 
    WHEN d < 31 THEN 0 -- Jan
    WHEN d < 60 THEN 31 -- Feb
    WHEN d < 91 THEN 60 -- Mar
    WHEN d < 121 THEN 91 -- Apr
    WHEN d < 152 THEN 121 -- May
    WHEN d < 182 THEN 152 -- Jun
    WHEN d < 213 THEN 182 -- Jul
    WHEN d < 244 THEN 213 -- Aug
    WHEN d < 274 THEN 244 -- Sep
    WHEN d < 305 THEN 274 -- Oct
    WHEN d < 335 THEN 305 -- Nov
    ELSE 335 -- Dec
    END
  SET dim = d - fdm
  SET m = ( d - dim + 28 ) / 31
  RETURN date.YearMonthDay( y, m+1, dim+1 )
END

CREATE FN [date].[DaysToYearDay]( days int ) RETURNS int AS
BEGIN
  -- Given a date represented by the number of days since 1 Jan 0000
  -- calculate a date in Year/Day representation stored as
  -- year * 512 + day where day is 1..366, the day in the year.
  
  DECLARE year int, day int, cycle int
  -- 146097 is the number of the days in a 400 year cycle ( 400 * 365 + 97 leap years )
  SET cycle = days / 146097
  SET days = days - 146097 * cycle -- Same as days % 146097
  SET year = days / 365
  SET day = days - year * 365 -- Same as days % 365
  -- Need to adjust day to allow for leap years.
  -- Leap years are 0, 4, 8, 12 ... 96, not 100, 104 ... not 200... not 300, 400, 404 ... not 500.
  -- Adjustment as function of y is 0 => 0, 1 => 1, 2 =>1, 3 => 1, 4 => 1, 5 => 2 ..
  SET day = day - ( year + 3 ) / 4 + ( year + 99 ) / 100 - ( year + 399 ) / 400
  
  IF day < 0
  BEGIN
    SET year -= 1
    SET day += CASE WHEN date.IsLeapYear( year ) THEN 366 ELSE 365 END
  END
  RETURN 512 * ( cycle * 400 + year ) + day + 1
END

CREATE FN [date].[YearMonthDay]( year int, month int, day int ) RETURNS int AS
BEGIN
  RETURN year * 512 + month * 32 + day
END

CREATE FN [date].[IsLeapYear]( y int ) RETURNS bool AS
BEGIN
  RETURN y % 4 = 0 AND ( y % 100 != 0 OR y % 400 = 0 )
END

CREATE FN [date].[DaysToYearMonthDay]( days int ) RETURNS int AS
BEGIN
  RETURN date.YearDayToYearMonthDay( date.DaysToYearDay( days ) )
END

CREATE FN [date].[test]() AS
BEGIN
   DECLARE days int, ymd int
   WHILE days < 1000
   BEGIN
      SET days += 1
      SET ymd = date.DaysToYearMonthDay(days)
   END
END
";

    let stg = AtomicFile::new(MemFile::new(), MemFile::new());

    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let bmap = Arc::new(bmap);

    let spd = SharedPagedData::new(stg);
    let wapd = AccessPagedData::new_writer(spd.clone());
    let db = Database::new(wapd, INITSQL, bmap.clone());

    // To check things work with low mem_limit.
    {
        // let mut s = spd.stash.lock().unwrap();
        // s.mem_limit = 1;
    }

    let mut results = Vec::new();
    for _i in 0..100 {
        let start = std::time::Instant::now();
        let mut tr = GenTransaction::default();
        let sql = "EXEC date.test()";
        db.run(&sql, &mut tr);
        results.push(start.elapsed().as_micros() as u64);
        assert_eq!(tr.get_error(), "");
    }
    crate::bench::print_results("date calc test", results);
}
