/*
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
*/

use rustdb::{
    init::INITSQL, pstore::SharedPagedData, stg::SimpleFileStorage, web::WebQuery, Database,
};
use std::net::TcpListener;
use std::sync::Arc;

fn main() {
    let sfs = Box::new(SimpleFileStorage::new(
        "c:\\Users\\pc\\rust\\sftest01.rustdb",
    ));
    let spd = Arc::new(SharedPagedData::new(sfs));
    let wstg = spd.open_write();
    let db = Database::new(wstg, INITSQL);

    let listener = TcpListener::bind("127.0.0.1:3000").unwrap(); // 7878 is another possible port.
    for tcps in listener.incoming() {
        let mut tcps = tcps.unwrap();
        let mut wq = WebQuery::new(&tcps); // Reads the http request from the TCP stream into wq.

        // wq.trace();
        let sql = "EXEC web.Main()";
        db.run_timed(&sql, &mut wq); // Executes SQL, http response, SQL output, (status,headers,content) is accumulated in wq.
        wq.write(&mut tcps); // Write the http response to the TCP stream.
        db.save(); // Saves database changes to disk.
    }
}
