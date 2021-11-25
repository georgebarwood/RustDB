use rustdb::{Database, SharedPagedData, SimpleFileStorage, WebQuery, INITSQL};
use std::net::TcpListener;
use std::sync::Arc;

fn main() {
    let sfs = Box::new(SimpleFileStorage::new(
        "c:\\Users\\pc\\rust\\sftest01.rustdb",
    ));
    let spd = Arc::new(SharedPagedData::new(sfs));
    let apd = spd.open_write();
    let db = Database::new(apd, INITSQL);

    let listener = TcpListener::bind("127.0.0.1:3000").unwrap(); // 7878 is another possible port.
    for tcps in listener.incoming() {
        let mut tcps = tcps.unwrap();
        let mut wq = WebQuery::new(&tcps); // Reads the http request from the TCP stream into wq.
        // wq.trace();
        if &*wq.method != "" 
        {
          let sql = "EXEC web.Main()";
          db.run_timed(&sql, &mut wq); // Executes SQL, http response, SQL output, (status,headers,content) is accumulated in wq.
        }
        let _err = wq.write(&mut tcps); // Write the http response to the TCP stream.
        db.save(); // Saves database changes to disk.
    }
}
