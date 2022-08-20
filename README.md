Axum-based webserver based on rustdb database, with database browsing, 
timed jobs, password hashing, data compression, email transmission and database replication.

USAGE:\
    rustweb.exe [OPTIONS] <PORT>

ARGS:\
    <PORT>    Port to listen on

OPTIONS:\
    -h, --help             Print help information\
    -i, --ip <IP>          Ip Address to listen on [default: 0.0.0.0]\
    -l, --login <LOGIN>    Login cookies for replication [default: ]\
    -m, --mem <MEM>        Memory limit for page cache (in MB) [default: 10]\
    -r, --rep <REP>        Server to replicate [default: ]\
        --tracemem         Trace memory trimming\
        --tracetime        Trace query time\
    -V, --version          Print version information

crates.io : https://crates.io/crates/rustweb