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
    c_value, check_types, expr::ObjRef, standard_builtins, AccessPagedData, AtomicFile, Block,
    BuiltinMap, CExp, CExpPtr, CompileFunc, DataKind, Database, EvalEnv, Expr, GenTransaction,
    Part, SharedPagedData, SimpleFileStorage, Transaction, Value, INITSQL,
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
        Self {
            x: Box::new(GenTransaction::new()),
        }
    }
}

/// Message to server thread, includes oneshot Sender for reply.
struct ServerMessage {
    st: ServerTrans,
    tx: oneshot::Sender<ServerTrans>,
}

/// Extra transaction data.
#[derive(Default)]
struct TransExt {
    /// Signals there is new email to be sent.
    email_tx: bool,
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
}

impl SharedState {
    async fn process(&self, st: ServerTrans) -> ServerTrans {
        let (tx, rx) = oneshot::channel::<ServerTrans>();
        let _err = self.tx.send(ServerMessage { st, tx }).await;
        rx.await.unwrap()
    }
}

#[tokio::main]
/// Execution starts here.
async fn main() {
    // console_subscriber::init();

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

    // Construct shared state.
    let ss = Arc::new(SharedState {
        tx,
        spd,
        bmap: bmap.clone(),
        email_tx,
    });

    // Start the logging thread (synchronous)
    thread::spawn(move || {
        log_loop(log_rx, logfile);
    });

    // Start the email thread (asynchronous)
    let email_ss = ss.clone();
    tokio::spawn(async move { email_loop(email_rx, email_ss).await });

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
                // println!("Serialised query={}", ser);
                let _err = log_tx.send(ser);
            }
            let _x = sm.tx.send(sm.st);
        }
    });

    // Build the axum app with a single route.
    let app = Router::new().route("/*key", get(h_get).post(h_post)).layer(
        ServiceBuilder::new()
            .layer(CookieManagerLayer::new())
            .layer(Extension(ss)),
    );

    // Run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
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
    st.x.ext = TransExt::new();
    st.x.qy.path = path.0;
    st.x.qy.params = params.0;
    st.x.qy.cookies = map_cookies(cookies);
    if let Some(Form(form)) = form {
        st.x.qy.form = form;
    } else {
        st.x.qy.parts = map_parts(multipart).await;
    }

    let mut st = state.process(st).await;

    // Check if email needs sending.
    let ext = st.x.get_extension();
    if let Some(ext) = ext.downcast_ref::<TransExt>() {
        if ext.email_tx {
            let _err = state.email_tx.send(()).await;
        }
    }
    st
}

use axum::{
    body::{boxed, BoxBody, Full},
    http::{header::HeaderName, status::StatusCode, HeaderValue, Response},
    response::IntoResponse,
};

impl IntoResponse for ServerTrans {
    fn into_response(self) -> Response<BoxBody> {
        let mybody = boxed(Full::from(self.x.rp.output));
        let mut res = Response::builder().body(mybody).unwrap();

        *res.status_mut() = StatusCode::from_u16(self.x.rp.status_code).unwrap();

        for (name, value) in &self.x.rp.headers {
            res.headers_mut().insert(
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

// thread that sends emails
async fn email_loop(mut rx: mpsc::Receiver<()>, state: Arc<SharedState>) {
    loop {
        let mut sent = Vec::new();
        {
            let _ = rx.recv().await;
            let apd = AccessPagedData::new_reader(state.spd.clone());
            let db = Database::new(apd, "", state.bmap.clone());
            let qt = db.get_table(&ObjRef::new("email", "Queue")).unwrap();

            let keys = Vec::new();
            for (pp, off) in qt.scan_keys(&db, keys, 0) {
                let p = &pp.borrow();
                let a = qt.access(p, off);
                let msg = a.int(0) as u64;
                let st = a.int(1);

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap();
                let now = now.as_micros() as i64 + 62135596800000000; // Per date.Ticks

                println!(
                    "Queued email id ={} seconds till send={}",
                    msg,
                    (st - now) / 1000000
                );
                let mt = db.get_table(&ObjRef::new("email", "Msg")).unwrap();
                let (pp, off) = mt.id_get(&db, msg).unwrap();
                let p = &pp.borrow();
                let a = mt.access(p, off);
                let from = a.str(&db, 0);
                let to = a.str(&db, 1);
                let title = a.str(&db, 2);
                let body = a.str(&db, 3);

                println!(
                    "Email from={} to={} title={} body={}",
                    from, to, title, body
                );

                // Actual sending of email not yet implemented...
                sent.push(msg);
            }
        }
        for msg in sent {
            tokio::task::yield_now().await;
            email_sent(&state, msg).await;
        }
    }
}

async fn email_sent(state: &SharedState, msg: u64) {
    let mut st = ServerTrans::new();
    st.x.qy.sql = Arc::new("EXEC email.Sent(".to_string() + &msg.to_string() + ")");
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
            ext.email_tx = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}
