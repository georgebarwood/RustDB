# RustDB


Database with SQL-like language implemented in Rus.

The SQL-like language is relatively minimal, and does not (currently) include features such as joins or views. Instead it has high performance SET .. FROM … and FOR .. FROM statements to access database tables, generally using an INDEX.

The complete language manual is available at run-time via the pre-configured (but optional) init::INITSQL database initialisation string, which also includes many functions which illustrate how the language works, including generic table browsing/editing, date and other functions.

Read-only transactions run immediately and concurrently on a virtual read-only copy of the database, and cannot be blocked. Write transactions run sequentially (and should typically execute in around 100 micro-seconds). The Storage trait allows a variety of underlying storage, including SimpleFileStorage, MemFile and AtomicFile.