/* Each test should first create a table with two columns, insert 8,192 identical rows 'Alice', 1000.
   Then (the timed part) should total the second column ( result 8,192,000 ) and do this 1,000 times.
*/

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

    let mut results = Vec::new();
    for _outer in 0..100 {
        let start = std::time::Instant::now();
        for _i in 0..1000 {
            let sql = "SELECT SUM(age) FROM users";
            connection.execute(sql).unwrap();
        }
        results.push(start.elapsed().as_millis() as u64);
    }
    print_results("sqlite_test", results);
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

    let mut results = Vec::new();
    for _outer in 0..100 {
        let start = std::time::Instant::now();

        for _i in 0..1000 {
            let sql =
                "DECLARE @total int FOR @total += age FROM test.users BEGIN END SELECT ''|@total";
            let mut tr = GenTransaction::default();
            db.run(&sql, &mut tr);
            assert_eq!(tr.rp.output, b"8192000");
        }

        results.push(start.elapsed().as_millis() as u64);
    }
    print_results("rustdb_test", results);
}

#[test]
fn rustdb_direct_test() {
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

    let mut results = Vec::new();
    for _outer in 0..100 {
        let start = std::time::Instant::now();
        for _i in 0..1000 {
            let ut = db.table("test", "users");
            let mut total = 0;
            for (pp, off) in ut.scan(&db) {
                let p = &pp.borrow();
                let a = ut.access(p, off);
                total += a.int(1);
            }
            assert_eq!(total, 8192000);
        }
        results.push(start.elapsed().as_millis() as u64);
    }
    print_results("rustdb_direct_test", results);
}

#[cfg(test)]
fn print_results(name: &str, mut results: Vec<u64>) {
    results.sort();
    let n = results.len() / 10;
    let results = &results[0..n];
    let mut total = 0;
    for result in results {
        total += result;
    }
    println!(
        "{} average time={} sorted results={:?}",
        name,
        total / (n as u64),
        results
    );
}
