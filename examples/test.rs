use rustdb::{AtomicFile, Database, SharedPagedData, SimpleFileStorage, WebTransaction, INITSQL};
use std::net::TcpListener;
use std::sync::Arc;

fn main() {
    let file = Box::new(SimpleFileStorage::new(
        "c:\\Users\\pc\\rust\\sftest01.rustdb",
    ));
    let upd = Box::new(SimpleFileStorage::new(
        "c:\\Users\\pc\\rust\\sftest01.upd",
    ));
    let stg = Box::new(AtomicFile::new(file,upd));
    let spd = Arc::new(SharedPagedData::new(stg));
    let apd = spd.open_write();
    let db = Database::new(apd, INITSQL);

    let listener = TcpListener::bind("127.0.0.1:3000").unwrap();
    for tcps in listener.incoming() {
        match tcps {
            Err(e) => {
                println!("Incoming connection error {:?}", e);
            }
            Ok(mut tcps) => {
                match WebTransaction::new(&tcps) {
                    Err(e) => {
                        println!("Error getting query {:?}", e);
                    }
                    Ok(mut wq) => {
                        wq.trace();
                        let sql = "EXEC web.Main()";
                        // Execute SQL. http response, SQL output, (status,headers,content) is accumulated in wq.
                        db.run_timed(sql, &mut wq);
                        // Write the http response to the TCP stream.
                        let _err = wq.write(&mut tcps);
                        // Save database changes to disk.
                        db.save();
                    }
                }
            }
        }
    }
}
