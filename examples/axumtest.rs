use mimalloc::MiMalloc;

/// Memory allocator ( MiMalloc ).
#[global_allocator]
static MEMALLOC: MiMalloc = MiMalloc;

use axum::{
    extract::{Extension, Form, Multipart, Path, Query},
    routing::get,
    AddExtensionLayer, Router,
};

use tower::ServiceBuilder;
use tower_cookies::{CookieManagerLayer, Cookies};

use tokio::sync::{mpsc, oneshot};

use rustdb::{
    c_value, check_types, AccessPagedData, Block, CExp, CExpPtr, CompileFunc, DataKind, Database,
    EvalEnv, Expr, GenQuery, Part, SharedPagedData, SimpleFileStorage, Value, DB, INITSQL,
};

use std::{collections::HashMap, rc::Rc, sync::Arc, thread};

/// Query to be sent to server thread, implements IntoResponse.
struct ServerQuery {
    pub x: Box<GenQuery>,
}

impl ServerQuery {
    pub fn new() -> Self {
        Self {
            x: Box::new(GenQuery::new()),
        }
    }
}

/// Message to server thread, includes oneshot Sender for reply.
struct ServerMessage {
    pub sq: ServerQuery,
    pub tx: oneshot::Sender<ServerQuery>,
}

/// State shared with handlers.
#[derive(Clone)]
struct SharedState {
    /// Sender channel for sending queries to server thread.
    tx: mpsc::Sender<ServerMessage>,
    /// Shared storage used for read-only queries.
    spd: Arc<SharedPagedData>,
}

/// Get database with extra registered builtin functions.
fn get_db(apd: AccessPagedData, sql: &str) -> DB {
    let db = Database::new(apd, sql);
    let list = [("ARGON", DataKind::Binary, CompileFunc::Value(c_argon))];
    for (name, typ, cf) in list {
        db.register(name, typ, cf);
    }
    db
}

#[tokio::main]
/// Execution starts here.
async fn main() {
    // console_subscriber::init();
    let sfs = Box::new(SimpleFileStorage::new(
        "C:/Users/pc/rust/sftest01.rustdb",
    ));
    let spd = Arc::new(SharedPagedData::new(sfs));

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(1);

    let state = Arc::new(SharedState { tx, spd });
    let wapd = state.spd.open_write();

    // This is the server thread (synchronous).
    thread::spawn(move || {
        let db = get_db(wapd, INITSQL);
        loop {
            let mut sm = rx.blocking_recv().unwrap();
            db.run_timed("EXEC web.Main()", &mut *sm.sq.x);
            db.save();
            let _x = sm.tx.send(sm.sq);
        }
    });

    // build our application with a single route
    let app = Router::new().route("/*key", get(h_get).post(h_post)).layer(
        ServiceBuilder::new()
            .layer(CookieManagerLayer::new())
            .layer(AddExtensionLayer::new(state)),
    );

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

/// Get HashMap of cookies from Cookies.
fn map_cookies(cookies: Cookies) -> HashMap<String, String> {
    let mut result = HashMap::new();
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
            result.push(Part {
                name,
                file_name,
                content_type,
                data: Arc::new(data),
                text,
            });
        }
    }
    result
}

/// Handler for http GET requests.
async fn h_get(
    state: Extension<Arc<SharedState>>,
    path: Path<String>,
    params: Query<HashMap<String, String>>,
    cookies: Cookies,
) -> ServerQuery {
    // Build the ServerQuery.
    let mut sq = ServerQuery::new();
    sq.x.path = path.0;
    sq.x.params = params.0;
    sq.x.cookies = map_cookies(cookies);

    let blocking_task = tokio::task::spawn_blocking(move || {
        // GET requests should be read-only.
        let apd = state.spd.open_read();
        let db = get_db(apd, "");
        db.run_timed("EXEC web.Main()", &mut *sq.x);
        sq
    });
    blocking_task.await.unwrap()
}

/// Handler for http POST requests.
async fn h_post(
    state: Extension<Arc<SharedState>>,
    path: Path<String>,
    params: Query<HashMap<String, String>>,
    cookies: Cookies,
    form: Option<Form<HashMap<String, String>>>,
    multipart: Option<Multipart>,
) -> ServerQuery {
    // Build the ServerQuery.
    let mut sq = ServerQuery::new();
    sq.x.path = path.0;
    sq.x.params = params.0;
    sq.x.cookies = map_cookies(cookies);
    if let Some(Form(form)) = form {
        sq.x.form = form;
    } else {
        sq.x.parts = map_parts(multipart).await;
    }

    // Send query to database thread ( and get it back ).
    let (tx, rx) = oneshot::channel::<ServerQuery>();
    let _err = state.tx.send(ServerMessage { sq, tx }).await;
    rx.await.unwrap()
}

use axum::{
    body::{Bytes, Full},
    http::{header::HeaderName, status::StatusCode, HeaderValue, Response},
    response::IntoResponse,
};

impl IntoResponse for ServerQuery {
    type Body = Full<Bytes>;
    type BodyError = std::convert::Infallible;

    fn into_response(self) -> Response<Self::Body> {
        let mut res = Response::new(Full::from(self.x.output));

        *res.status_mut() = StatusCode::from_u16(self.x.status_code).unwrap();

        for (name, value) in &self.x.headers {
            res.headers_mut().insert(
                HeaderName::from_lowercase(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        res
    }
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
