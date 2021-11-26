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

    let listener = TcpListener::bind("127.0.0.1:3000").unwrap();
    for tcps in listener.incoming() {
        if let Ok(mut tcps) = tcps {
            if let Ok(mut wq) = WebQuery::new(&tcps) {
                // wq.trace();
                let sql = "EXEC web.Main()";
                // Execute SQL. http response, SQL output, (status,headers,content) is accumulated in wq.
                db.run_timed(&sql, &mut wq);
                // Write the http response to the TCP stream.
                let _err = wq.write(&mut tcps);
                // Save database changes to disk.
                db.save();
            }
        }
    }
}
