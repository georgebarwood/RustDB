# rustdb

Database with SQL-like language implemented in Rust.

The SQL-like language is relatively minimal, and does not (currently) include features such as joins or views. Instead it has high performance SET .. FROM … and FOR .. FROM statements to access database tables, generally using an INDEX.

Read-only transactions run immediately and concurrently on a virtual read-only copy of the database, and cannot be blocked. 

Write transactions run sequentially (and should typically execute in around 100 micro-seconds). 

The Storage trait allows a variety of underlying storage, including SimpleFileStorage, MemFile and AtomicFile.

Data is accessed either by a Transaction interface or directly ( as an offset into a page of byte data ).

Transactions can be logged, allowing database replication.

See https://github.com/georgebarwood/rustweb2 for example program : a webserver based on rustdb database, with database browsing, password hashing, database replication, email transmission and timed jobs.

crates.io : https://crates.io/crates/rustdb 

documentation: https://docs.rs/rustdb/latest/rustdb/

blog: https://rustdb.wordpress.com/
