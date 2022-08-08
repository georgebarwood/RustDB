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
    c_int, c_value, check_types, standard_builtins, AccessPagedData, AtomicFile, Block, BuiltinMap,
    CExp, CExpPtr, CompileFunc, DataKind, Database, EvalEnv, Expr, GenTransaction, Part,
    SharedPagedData, SimpleFileStorage, Transaction, Value, INITSQL,
};
use std::{collections::BTreeMap, rc::Rc, sync::Arc, thread};

use tokio::sync::{broadcast, mpsc, oneshot};
use tower::ServiceBuilder;
use tower_cookies::{CookieManagerLayer, Cookies};

/// Transaction to be sent to server task, implements IntoResponse.
struct ServerTrans {
    x: Box<GenTransaction>,
    log: bool,
}

impl ServerTrans {
    fn new() -> Self {
        let mut result = Self {
            x: Box::new(GenTransaction::new()),
            log: true,
        };
        result.x.ext = TransExt::new();
        result
    }
}

/// Message to server task, includes oneshot Sender for reply.
struct ServerMessage {
    st: ServerTrans,
    reply: oneshot::Sender<ServerTrans>,
}

/// Extra transaction data.
#[derive(Default)]
struct TransExt {
    /// Signals there is new email to be sent.
    tx_email: bool,
    /// Signals time to sleep.
    sleep: u64,
    /// Signals wait for new transaction to be logged
    trans_wait: bool,
}

impl TransExt {
    fn new() -> Box<Self> {
        Box::new(Self::default())
    }
}

/// State shared with handlers.
struct SharedState {
    /// Shared storage used for read-only queries.
    spd: Arc<SharedPagedData>,
    /// Map of builtin SQL functions for Database.
    bmap: Arc<BuiltinMap>,
    /// Sender channel for sending queries to server task.
    tx: mpsc::Sender<ServerMessage>,
    /// For notifying email loop that emails are in Queue ready to be sent.
    email_tx: mpsc::UnboundedSender<()>,
    /// For setting sleep time.
    sleep_tx: mpsc::UnboundedSender<u64>,
    /// For notifying tasks waiting for transaction.
    wait_tx: broadcast::Sender<()>,

    server_type: String,
    replicate_source: String,
    replicate_credentials: String,
}

impl SharedState {
    async fn process(&self, st: ServerTrans) -> ServerTrans {
        let (reply, rx) = oneshot::channel::<ServerTrans>();
        let _err = self.tx.send(ServerMessage { st, reply }).await;
        let mut st = rx.await.unwrap();
        // Check if email needs sending or sleep time has been specified, etc.
        let ext = st.x.get_extension();
        if let Some(ext) = ext.downcast_ref::<TransExt>() {
            if ext.sleep > 0 {
                let _ = self.sleep_tx.send(ext.sleep);
                // To ensure tasks waiting on transaction proceed after some time.
                let _ = self.wait_tx.send(());
            }
            if ext.tx_email {
                let _ = self.email_tx.send(());
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

    let server_type: String = if args.len() > 2 { &args[2] } else { "master" }.to_string();
    let slave = server_type == "slave";

    let replicate_source: String = if args.len() > 3 { &args[3] } else { "" }.to_string();

    let replicate_credentials: String = if args.len() > 4 { &args[4] } else { "" }.to_string();

    // Construct an AtomicFile. This ensures that updates to the database are "all or nothing".
    let file = Box::new(SimpleFileStorage::new("../test.rustdb"));
    let upd = Box::new(SimpleFileStorage::new("../test.upd"));
    let stg = Box::new(AtomicFile::new(file, upd));

    // SharedPagedData allows for one writer and multiple readers.
    // Note that readers never have to wait, they get a "virtual" read-only copy of the database.
    let spd = Arc::new(SharedPagedData::new(stg));

    // Construct map of "builtin" functions that can be called in SQL code.
    // Include extra functions ARGON, EMAILTX and SLEEP as well as the standard functions.
    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let list = [
        ("ARGON", DataKind::Binary, CompileFunc::Value(c_argon)),
        ("EMAILTX", DataKind::Int, CompileFunc::Int(c_email_tx)),
        ("SLEEP", DataKind::Int, CompileFunc::Int(c_sleep)),
        ("TRANSWAIT", DataKind::Int, CompileFunc::Int(c_trans_wait)),
    ];
    for (name, typ, cf) in list {
        bmap.insert(name.to_string(), (typ, cf));
    }
    let bmap = Arc::new(bmap);

    // Get write-access to database ( there will only be one of these ).
    let wapd = AccessPagedData::new_writer(spd.clone());

    // Construct task communication channels.
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(1);
    let (email_tx, email_rx) = mpsc::unbounded_channel::<()>();
    let (sleep_tx, sleep_rx) = mpsc::unbounded_channel::<u64>();
    let (sync_tx, sync_rx) = oneshot::channel::<bool>();
    let (wait_tx, _wait_rx) = broadcast::channel::<()>(16);

    // Construct shared state.
    let ss = Arc::new(SharedState {
        spd,
        bmap: bmap.clone(),
        tx,
        email_tx,
        sleep_tx,
        wait_tx,
        server_type,
        replicate_source,
        replicate_credentials,
    });

    if slave {
        // Start the sync task.
        let ssc = ss.clone();
        tokio::spawn(async move { sync_loop(sync_rx, ssc).await });
    } else {
        // Start the email task.
        let ssc = ss.clone();
        tokio::spawn(async move { email_loop(email_rx, ssc).await });

        // Start the sleep task.
        let ssc = ss.clone();
        tokio::spawn(async move { sleep_loop(sleep_rx, ssc).await });
    }

    let ssc = ss.clone();
    // Start the server task that updates the database.
    thread::spawn(move || {
        let ss = ssc;
        let db = if slave {
            Database::new(wapd, "", bmap)
        } else {
            Database::new(wapd, INITSQL, bmap)
        };
        if slave {
            let _ = sync_tx.send(db.is_new);
        }
        loop {
            let mut sm = rx.blocking_recv().unwrap();
            let sql = sm.st.x.qy.sql.clone();
            db.run_timed(&sql, &mut *sm.st.x);

            let updates = if sm.st.log {
                // let ser = serde_json::to_string(&sm.st.x.qy).unwrap();
                let ser = rmp_serde::to_vec(&sm.st.x.qy).unwrap();
                db.save_and_log(Some(Value::RcBinary(Rc::new(ser))))
            } else {
                db.save()
            };
            if updates > 0 {
                let _ = ss.wait_tx.send(());
                println!("Pages updated={}", updates);
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

    let mut wait_rx = state.wait_tx.subscribe();

    let mut st = tokio::task::spawn_blocking(move || {
        // GET requests should be read-only.
        let apd = AccessPagedData::new_reader(state.spd.clone());
        let db = Database::new(apd, "", state.bmap.clone());
        let sql = st.x.qy.sql.clone();
        db.run_timed(&sql, &mut *st.x);
        st
    })
    .await
    .unwrap();

    let ext = st.x.get_extension();
    if let Some(ext) = ext.downcast_ref::<TransExt>() {
        if ext.trans_wait {
            let _ = wait_rx.recv().await;
        }
    }
    st
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

/// task for syncing with master database
async fn sync_loop(rx: oneshot::Receiver<bool>, state: Arc<SharedState>) {
    let db_is_new = rx.await.unwrap();
    println!(
        "sync_loop got db_is_new={} source={} credentials={}",
        db_is_new, state.replicate_source, state.replicate_credentials
    );

    if db_is_new {
        // Note: using ScriptAll is problematic as table and field ids are not preserved.
        // Also table allocators are not preserved ( problem if last record in table has beeen deleted ).
        /*
        let sql = rget(state.clone(), "/ScriptAll").await;
        let mut st = ServerTrans::new();
        st.log = false;
        st.x.qy.sql = Arc::new(sql);
        state.process(st).await;
        println!("New slave database initialised");
        */
        panic!("Currently initial slave database must be copied from master by e.g. FTP.");
    }
    loop {
        let url = {
            let apd = AccessPagedData::new_reader(state.spd.clone());
            let db = Database::new(apd, "", state.bmap.clone());
            let lt = db.table("log", "Transaction");
            let tid = lt.id_gen.get();
            format!("/GetTransaction?k={}", tid)
        };

        println!("sync_loop calling rget url ={}", url);
        let ser = rget(state.clone(), &url).await;
        println!("sync_loop returnd from rget");

        if !ser.is_empty() {
            let mut st = ServerTrans::new();
            // st.x.qy = serde_json::from_str(&json).unwrap();
            st.x.qy = rmp_serde::from_slice(&ser).unwrap();
            println!("sync_loop qy={:?}", st.x.qy);
            state.process(st).await;
            println!("sync_loop finished query");
        }
    }
}

async fn rget(state: Arc<SharedState>, query: &str) -> Vec<u8> {
    loop {
        use reqwest::header;
        let headers = header::HeaderMap::new();

        // get a client builder
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        match client
            .get(state.replicate_source.clone() + query)
            .header("Cookie", state.replicate_credentials.clone())
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    if let Ok(result) = response.bytes().await {
                        return result.to_vec();
                    }
                } else {
                    println!("Failed Response status {}", response.status());
                }
            }
            Err(e) => {
                println!("Send error {}", e);
            }
        }
        // Wait before retrying.
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }
}

/// task for sleeping - calls timed.Run once sleep time has elapsed.
async fn sleep_loop(mut rx: mpsc::UnboundedReceiver<u64>, state: Arc<SharedState>) {
    let mut sleep_micro = 5000000;
    loop {
        tokio::select! {
            ns = rx.recv() => { sleep_micro = ns.unwrap(); }
            _ = tokio::time::sleep(core::time::Duration::from_micros(sleep_micro)) =>
            {
              if state.server_type == "master"
              {
                let mut st = ServerTrans::new();
                st.x.qy.sql = Arc::new("EXEC timed.Run()".to_string());
                state.process(st).await;
              }
            }
        }
    }
}

/// task that sends emails
async fn email_loop(mut rx: mpsc::UnboundedReceiver<()>, state: Arc<SharedState>) {
    loop {
        let mut send_list = Vec::new();
        {
            let _ = rx.recv().await;
            let apd = AccessPagedData::new_reader(state.spd.clone());
            let db = Database::new(apd, "", state.bmap.clone());
            let qt = db.table("email", "Queue");
            let mt = db.table("email", "Msg");
            let at = db.table("email", "SmtpAccount");

            for (pp, off) in qt.scan(&db) {
                let p = &pp.borrow();
                let a = qt.access(p, off);
                let msg = a.int(0) as u64;

                if let Some((pp, off)) = mt.id_get(&db, msg) {
                    let p = &pp.borrow();
                    let a = mt.access(p, off);
                    let from = a.str(&db, 0);
                    let to = a.str(&db, 1);
                    let title = a.str(&db, 2);
                    let body = a.str(&db, 3);
                    let format = a.int(4);
                    let account = a.int(5) as u64;

                    if let Some((pp, off)) = at.id_get(&db, account) {
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
            }
        }
        for (msg, from, to, title, body, format, server, username, password) in send_list {
            let blocking_task = tokio::task::spawn_blocking(move || {
                send_email(from, to, title, body, format, server, username, password)
            });
            let result = blocking_task.await.unwrap();
            match result {
                Ok(_) => email_sent(&state, msg).await,
                Err(e) => match e {
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
                },
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

/// Send an email using lettre.
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
        .credentials(Credentials::new(username, password))
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
            ext.sleep = if to <= 0 { 1 } else { to as u64 };
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

/// Compile call to TRANSWAIT.
fn c_trans_wait(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(TransWait {})
}

/// Compiled call to TRANSWAIT
struct TransWait {}
impl CExp<i64> for TransWait {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        let mut ext = ee.tr.get_extension();
        if let Some(mut ext) = ext.downcast_mut::<TransExt>() {
            ext.trans_wait = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}
