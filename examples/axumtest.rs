use mimalloc::MiMalloc;

/// Memory allocator ( MiMalloc ).
#[global_allocator]
static MEMALLOC: MiMalloc = MiMalloc;

use axum::{
    extract::{Extension, Form, Multipart, Path, Query},
    routing::get,
    Router,
};
use rustdb::{
    c_int, c_value, check_types, expr::ObjRef, standard_builtins, AccessPagedData, AtomicFile,
    Block, BuiltinMap, CExp, CExpPtr, CompileFunc, DataKind, Database, EvalEnv, Expr,
    GenTransaction, Part, SharedPagedData, SimpleFileStorage, Transaction, Value, INITSQL,
};
use std::{collections::BTreeMap, rc::Rc, sync::Arc, thread};
use std::{fs, fs::OpenOptions, io::Write};

use tokio::sync::{mpsc, oneshot};
use tower::ServiceBuilder;
use tower_cookies::{CookieManagerLayer, Cookies};

/// Transaction to be sent to server thread, implements IntoResponse.
struct ServerTrans {
    x: Box<GenTransaction>,
}

impl ServerTrans {
    fn new() -> Self {
        let mut result = Self {
            x: Box::new(GenTransaction::new()),
        };
        result.x.ext = TransExt::new();
        result
    }
}

/// Message to server thread, includes oneshot Sender for reply.
struct ServerMessage {
    st: ServerTrans,
    reply: oneshot::Sender<ServerTrans>,
}

/// Extra transaction data.
#[derive(Default)]
struct TransExt {
    /// Signals there is new email to be sent.
    tx_email: bool,
    sleep: u64,
}

impl TransExt {
    fn new() -> Box<Self> {
        Box::new(Self::default())
    }
}

/// State shared with handlers.
struct SharedState {
    /// Sender channel for sending queries to server thread.
    tx: mpsc::Sender<ServerMessage>,
    /// Shared storage used for read-only queries.
    spd: Arc<SharedPagedData>,
    /// Map of builtin SQL functions for Database.
    bmap: Arc<BuiltinMap>,
    email_tx: mpsc::Sender<()>,
    sleep_tx: mpsc::Sender<u64>,
}

impl SharedState {
    async fn process(&self, st: ServerTrans) -> ServerTrans {
        let (reply, rx) = oneshot::channel::<ServerTrans>();
        let _err = self.tx.send(ServerMessage { st, reply }).await;
        let mut st = rx.await.unwrap();
        // Check if email needs sending or sleep time has been specified.
        let ext = st.x.get_extension();
        if let Some(ext) = ext.downcast_ref::<TransExt>() 
        {
            if ext.sleep > 0 {
                let _err = self.sleep_tx.send(ext.sleep).await;
            }

            if ext.tx_email {
                let _err = self.email_tx.send(()).await;
            }
        }
        st
    }
}

#[tokio::main]
/// Execution starts here.
async fn main() {
    // console_subscriber::init();

    let args: Vec<String> = std::env::args().collect();
    let listen: String = if args.len() > 1 {
        &args[1]
    } else {
        "0.0.0.0:80"
    }
    .to_string();
    println!("Listening on {}", listen);

    // Construct an AtomicFile. This ensures that updates to the database are "all or nothing".
    let file = Box::new(SimpleFileStorage::new("../test.rustdb"));
    let upd = Box::new(SimpleFileStorage::new("../test.upd"));
    let stg = Box::new(AtomicFile::new(file, upd));

    // File for logging transactions.
    let logfile = OpenOptions::new()
        .append(true)
        .create(true)
        .open("../test.logfile")
        .unwrap();

    // SharedPagedData allows for one writer and multiple readers.
    // Note that readers never have to wait, they get a "virtual" read-only copy of the database.
    let spd = Arc::new(SharedPagedData::new(stg));

    // Construct map of "builtin" functions that can be called in SQL code.
    // Include the Argon hash function as well as the standard functions.
    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let list = [
        ("ARGON", DataKind::Binary, CompileFunc::Value(c_argon)),
        ("EMAILTX", DataKind::Int, CompileFunc::Int(c_email_tx)),
        ("SLEEP", DataKind::Int, CompileFunc::Int(c_sleep)),
    ];
    for (name, typ, cf) in list {
        bmap.insert(name.to_string(), (typ, cf));
    }
    let bmap = Arc::new(bmap);

    // Get write-access to database ( there will only be one of these ).
    let wapd = AccessPagedData::new_writer(spd.clone());

    // Construct thread communication channels.
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(1);
    let (log_tx, log_rx) = std::sync::mpsc::channel::<String>();
    let (email_tx, email_rx) = mpsc::channel::<()>(1);
    let (sleep_tx, sleep_rx) = mpsc::channel::<u64>(1);

    // Construct shared state.
    let ss = Arc::new(SharedState {
        tx,
        spd,
        bmap: bmap.clone(),
        email_tx,
        sleep_tx,
    });

    // Start the logging task (synchronous)
    thread::spawn(move || {
        log_loop(log_rx, logfile);
    });

    // Start the email task (asynchronous)
    let email_ss = ss.clone();
    tokio::spawn(async move { email_loop(email_rx, email_ss).await });

    // Start the sleep task (asynchronous)
    let sleep_ss = ss.clone();
    tokio::spawn(async move { sleep_loop(sleep_rx, sleep_ss).await });

    // Start the server thread that updates the database (synchronous).
    thread::spawn(move || {
        let db = Database::new(wapd, INITSQL, bmap);
        loop {
            let mut sm = rx.blocking_recv().unwrap();
            let sql = sm.st.x.qy.sql.clone();
            db.run_timed(&sql, &mut *sm.st.x);
            let updates = db.save();
            if updates > 0 {
                println!("Pages updated={}", updates);
                let ser = serde_json::to_string(&sm.st.x.qy).unwrap();
                let _err = log_tx.send(ser);
            }
            let _x = sm.reply.send(sm.st);
        }
    });

    // Build the axum app with a single route.
    let app = Router::new().route("/*key", get(h_get).post(h_post)).layer(
        ServiceBuilder::new()
            .layer(CookieManagerLayer::new())
            .layer(Extension(ss)),
    );

    // Run the axum app.
    axum::Server::bind(&listen.parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

/// Handler for http GET requests.
async fn h_get(
    state: Extension<Arc<SharedState>>,
    path: Path<String>,
    params: Query<BTreeMap<String, String>>,
    cookies: Cookies,
) -> ServerTrans {
    // Build the ServerTrans.
    let mut st = ServerTrans::new();
    st.x.qy.path = path.0;
    st.x.qy.params = params.0;
    st.x.qy.cookies = map_cookies(cookies);

    let blocking_task = tokio::task::spawn_blocking(move || {
        // GET requests should be read-only.
        let apd = AccessPagedData::new_reader(state.spd.clone());
        let db = Database::new(apd, "", state.bmap.clone());
        let sql = st.x.qy.sql.clone();
        db.run_timed(&sql, &mut *st.x);
        st
    });
    blocking_task.await.unwrap()
}

/// Handler for http POST requests.
async fn h_post(
    state: Extension<Arc<SharedState>>,
    path: Path<String>,
    params: Query<BTreeMap<String, String>>,
    cookies: Cookies,
    form: Option<Form<BTreeMap<String, String>>>,
    multipart: Option<Multipart>,
) -> ServerTrans {
    // Build the Server Transaction.
    let mut st = ServerTrans::new();
    st.x.qy.path = path.0;
    st.x.qy.params = params.0;
    st.x.qy.cookies = map_cookies(cookies);
    if let Some(Form(form)) = form {
        st.x.qy.form = form;
    } else {
        st.x.qy.parts = map_parts(multipart).await;
    }

    // Process the Server Transaction.
    state.process(st).await
}

use axum::{
    body::{boxed, BoxBody, Full},
    http::{header::HeaderName, status::StatusCode, HeaderValue, Response},
    response::IntoResponse,
};

impl IntoResponse for ServerTrans {
    fn into_response(self) -> Response<BoxBody> {
        let bf = boxed(Full::from(self.x.rp.output));
        let mut res = Response::builder().body(bf).unwrap();

        *res.status_mut() = StatusCode::from_u16(self.x.rp.status_code).unwrap();

        for (name, value) in &self.x.rp.headers {
            res.headers_mut().append(
                HeaderName::from_lowercase(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        res
    }
}

/// thread that logs write transactions to a file.
fn log_loop(rx: std::sync::mpsc::Receiver<String>, mut logfile: fs::File) {
    loop {
        let logstr = rx.recv().unwrap();
        let _err = logfile.write_all(logstr.as_bytes());
        let _err = logfile.write_all(b"\r\n");
    }
}

// task for sleeping - calls timed.Run once sleep time has elapsed.
async fn sleep_loop(mut rx: mpsc::Receiver<u64>, state: Arc<SharedState>) {
    let mut sleep_ms = 5000;
    loop {
        let sleep = tokio::time::sleep(core::time::Duration::from_millis(sleep_ms));
        tokio::pin!(sleep);

        tokio::select! {
            ms = rx.recv() => { sleep_ms = ms.unwrap(); }
            _ = &mut sleep =>
            {
              let mut st = ServerTrans::new();
              st.x.qy.sql = Arc::new("EXEC timed.Run()".to_string());
              state.process(st).await;
            }
        }
    }
}

// task that sends emails
async fn email_loop(mut rx: mpsc::Receiver<()>, state: Arc<SharedState>) {
    loop {
        let mut send_list = Vec::new();
        {
            let _ = rx.recv().await;
            let apd = AccessPagedData::new_reader(state.spd.clone());
            let db = Database::new(apd, "", state.bmap.clone());
            let qt = db.get_table(&ObjRef::new("email", "Queue")).unwrap();
            let mt = db.get_table(&ObjRef::new("email", "Msg")).unwrap();
            let at = db.get_table(&ObjRef::new("email", "SmtpAccount")).unwrap();

            for (pp, off) in qt.scan(&db) {
                let p = &pp.borrow();
                let a = qt.access(p, off);
                let msg = a.int(0) as u64;

                let (pp, off) = mt.id_get(&db, msg).unwrap();
                let p = &pp.borrow();
                let a = mt.access(p, off);
                let from = a.str(&db, 0);
                let to = a.str(&db, 1);
                let title = a.str(&db, 2);
                let body = a.str(&db, 3);
                let format = a.int(4);
                let account = a.int(5) as u64;

                let (pp, off) = at.id_get(&db, account).unwrap();
                let p = &pp.borrow();
                let a = at.access(p, off);
                let server = a.str(&db, 0);
                let username = a.str(&db, 1);
                let password = a.str(&db, 2);

                send_list.push((
                    msg, from, to, title, body, format, server, username, password,
                ));
            }
        }
        for (msg, from, to, title, body, format, server, username, password) in send_list {
            let blocking_task = tokio::task::spawn_blocking(move || {
                send_email(from, to, title, body, format, server, username, password)
            });
            let result = blocking_task.await.unwrap();
            match result {
                Ok(_) => email_sent(&state, msg).await,
                Err(e) => {
                    /* Log the error : ToDo */
                    match e {
                        EmailError::Address(ae) => {
                            email_error(&state, msg, 0, ae.to_string()).await;
                        }
                        EmailError::Lettre(le) => {
                            email_error(&state, msg, 0, le.to_string()).await;
                        }
                        EmailError::Send(se) => {
                            let retry = if se.is_transient() { 1 } else { 0 };
                            email_error(&state, msg, retry, se.to_string()).await;
                        }
                    }
                }
            }
        }
    }
}

/// Error enum for send_email
#[derive(Debug)]
enum EmailError {
    Address(lettre::address::AddressError),
    Lettre(lettre::error::Error),
    Send(lettre::transport::smtp::Error),
}

impl From<lettre::address::AddressError> for EmailError {
    fn from(e: lettre::address::AddressError) -> Self {
        EmailError::Address(e)
    }
}

impl From<lettre::error::Error> for EmailError {
    fn from(e: lettre::error::Error) -> Self {
        EmailError::Lettre(e)
    }
}

impl From<lettre::transport::smtp::Error> for EmailError {
    fn from(e: lettre::transport::smtp::Error) -> Self {
        EmailError::Send(e)
    }
}

fn send_email(
    from: String,
    to: String,
    title: String,
    body: String,
    format: i64,
    server: String,
    username: String,
    password: String,
) -> Result<(), EmailError> {
    use lettre::{
        message::SinglePart,
        transport::smtp::{
            authentication::{Credentials, Mechanism},
            PoolConfig,
        },
        Message, SmtpTransport, Transport,
    };

    let body = match format {
        1 => SinglePart::html(body),
        _ => SinglePart::plain(body),
    };

    let email = Message::builder()
        .to(to.parse()?)
        .from(from.parse()?)
        .subject(title)
        .singlepart(body)?;

    // Create TLS transport on port 587 with STARTTLS
    let sender = SmtpTransport::starttls_relay(&server)?
        // Add credentials for authentication
        .credentials(Credentials::new(username.to_string(), password.to_string()))
        // Configure expected authentication mechanism
        .authentication(vec![Mechanism::Plain])
        // Connection pool settings
        .pool_config(PoolConfig::new().max_size(20))
        .build();

    let _result = sender.send(&email)?;
    Ok(())
}

async fn email_sent(state: &SharedState, msg: u64) {
    let mut st = ServerTrans::new();
    st.x.qy.sql = Arc::new(format!("EXEC email.Sent({})", msg));
    state.process(st).await;
}

async fn email_error(state: &SharedState, msg: u64, retry: i8, err: String) {
    let mut st = ServerTrans::new();
    let src = format!("EXEC email.LogSendError({},{},'{}')", msg, retry, err);
    st.x.qy.sql = Arc::new(src);
    state.process(st).await;
}

/////////////////////////////////////////////
// Helper functions for building ServerTrans.

/// Get BTreeMap of cookies from Cookies.
fn map_cookies(cookies: Cookies) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    for cookie in cookies.list() {
        let (name, value) = cookie.name_value();
        result.insert(name.to_string(), value.to_string());
    }
    result
}

/// Get Vec of Parts from MultiPart.
async fn map_parts(mp: Option<Multipart>) -> Vec<Part> {
    let mut result = Vec::new();
    if let Some(mut mp) = mp {
        while let Some(field) = mp.next_field().await.unwrap() {
            let name = field.name().unwrap().to_string();
            let file_name = match field.file_name() {
                Some(s) => s.to_string(),
                None => "".to_string(),
            };
            let content_type = match field.content_type() {
                Some(s) => s.to_string(),
                None => "".to_string(),
            };
            let mut data = Vec::new();
            let mut text = "".to_string();
            if content_type.is_empty() {
                if let Ok(s) = field.text().await {
                    text = s;
                }
            } else if let Ok(bytes) = field.bytes().await {
                data = bytes.to_vec()
            }
            let mut part = Part::default();
            part.name = name;
            part.file_name = file_name;
            part.content_type = content_type;
            part.data = Arc::new(data);
            part.text = text;
            result.push(part);
        }
    }
    result
}

/////////////////////////////

use argon2rs::argon2i_simple;

/// Compile call to ARGON.
fn c_argon(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::String, DataKind::String]);
    let password = c_value(b, &mut args[0]);
    let salt = c_value(b, &mut args[1]);
    Box::new(Argon { password, salt })
}

/// Compiled call to ARGON.
struct Argon {
    password: CExpPtr<Value>,
    salt: CExpPtr<Value>,
}
impl CExp<Value> for Argon {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let pw = self.password.eval(ee, d).str();
        let salt = self.salt.eval(ee, d).str();

        let result = argon2i_simple(&pw, &salt).to_vec();
        Value::RcBinary(Rc::new(result))
    }
}

/// Compile call to SLEEP.
fn c_sleep(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::Int]);
    let to = c_int(b, &mut args[0]);
    Box::new(Sleep { to })
}

/// Compiled call to SLEEP
struct Sleep {
    to: CExpPtr<i64>,
}
impl CExp<i64> for Sleep {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let to = self.to.eval(ee, d);
        let mut ext = ee.tr.get_extension();
        if let Some(mut ext) = ext.downcast_mut::<TransExt>() {
            ext.sleep = if to < 0 { 0 } else { to as u64 };
        }
        ee.tr.set_extension(ext);
        0
    }
}

/// Compile call to EMAILTX.
fn c_email_tx(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(EmailTx {})
}

/// Compiled call to EMAILTX
struct EmailTx {}
impl CExp<i64> for EmailTx {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        let mut ext = ee.tr.get_extension();
        if let Some(mut ext) = ext.downcast_mut::<TransExt>() {
            ext.tx_email = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}
