#[test]
fn sqlite_test() {
    let connection = sqlite::open(":memory:").unwrap();

    let sql = "
    CREATE TABLE users (Id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
    INSERT INTO users(name,age) VALUES ('Alice', 1000);";
    connection.execute(sql).unwrap();

    let sql = "INSERT INTO users(name,age) SELECT name, age FROM users";

    // Create 8192 records (each iteration should double number of records)
    for _i in 0..13 {
        connection.execute(sql).unwrap();
    }

    let start = std::time::SystemTime::now();
    for _i in 0..10000
    {

      let sql = "SELECT SUM(age) FROM users";
      connection.execute(sql).unwrap();
    }
    println!(
        "sqllite test took {} milli-seconds",
        start.elapsed().unwrap().as_millis()
    );
}

#[test]
fn rustdb_test() {
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
    CREATE SCHEMA test GO
    CREATE TABLE test.users (name string, age int) GO";

    db.run(&sql, &mut tr);

    let sql = "DECLARE @i int SET @i = 8192
      WHILE @i > 0
      BEGIN
        INSERT INTO test.users(name,age) VALUES ('Alice', 1000)
        SET @i -= 1
      END";

    db.run(&sql, &mut tr);

    let start = std::time::SystemTime::now();

    for _i in 0..10000
    {
        let sql = "DECLARE @total int FOR @total += age FROM test.users BEGIN END SELECT ''|@total";
        let mut tr = GenTransaction::default();
        db.run(&sql, &mut tr);
        assert_eq!(tr.rp.output, b"8192000");
    }

    println!(
        "rustdb test took {} milli-seconds",
        start.elapsed().unwrap().as_millis()
    );
}

