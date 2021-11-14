use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use axum::{
    extract::{Extension, Form, Multipart, Path, Query},
    routing::get,
    AddExtensionLayer, Router,
};

use tower::ServiceBuilder;
use tower_cookies::{CookieManagerLayer, Cookies};

use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};

use database::{
    genquery::{GenQuery, Part},
    Database,
};

use std::{collections::HashMap, sync::Arc, thread};

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

struct SharedState {
    tx: Sender<ServerQuery>,
    rx: Receiver<ServerQuery>,
}

#[tokio::main]
async fn main() {
    let (tx, mut server_rx): (Sender<ServerQuery>, Receiver<ServerQuery>) = channel(1);
    let (server_tx, rx): (Sender<ServerQuery>, Receiver<ServerQuery>) = channel(1);

    // This is the server thread (synchronous).
    thread::spawn(move || {
        let stg = Box::new(database::stg::SimpleFileStorage::new(
            "c:\\Users\\pc\\rust\\sftest01.rustdb",
        ));
        let db = Database::new(stg, database::init::INITSQL);
        loop {
            let mut q = server_rx.blocking_recv().unwrap();
            let sql = "EXEC web.Main()";
            db.run_timed(sql, &mut *q.x);
            let _x = server_tx.blocking_send(q);
            db.save();
        }
    });

    let state = Arc::new(Mutex::new(SharedState { tx, rx }));

    // build our application with a single route
    let app = Router::new()
        .route("/*key", get(my_get_handler).post(my_post_handler))
        .layer(
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

async fn my_get_handler(
    Extension(state): Extension<Arc<Mutex<SharedState>>>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    tcookies: Cookies,
) -> ServerQuery {
    
    // Get cookies into a HashMap.
    let mut cookies = HashMap::new();
    for cookie in tcookies.list() {
        let (name, value) = cookie.name_value();
        cookies.insert(name.to_string(), value.to_string());
    }

    // Build the ServerQuery.
    let mut sq = ServerQuery::new();
    sq.x.path = path;
    sq.x.query = params;
    sq.x.cookies = cookies;

    // Send query to database thread ( and get it back ).
    let mut state = state.lock().await;
    let _x = state.tx.send(sq).await;
    state.rx.recv().await.unwrap()
}

async fn my_post_handler(
    Extension(state): Extension<Arc<Mutex<SharedState>>>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    form: Option<Form<HashMap<String, String>>>,
    tcookies: Cookies,
    mp: Option<Multipart>,
) -> ServerQuery {

    // Get the cookies into a HashMap.
    let mut cookies = HashMap::new();
    for cookie in tcookies.list() {
        let (name, value) = cookie.name_value();
        cookies.insert(name.to_string(), value.to_string());
    }

    // Get Vec of Parts.
    let mut parts = Vec::new();
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
            parts.push(Part {
                name,
                file_name,
                content_type,
                data,
                text,
            });
        }
    }

    // Build the ServerQuery.
    let mut sq = ServerQuery::new();
    sq.x.path = path;
    sq.x.query = params;
    sq.x.cookies = cookies;
    sq.x.parts = parts;
    if let Some(Form(form)) = form {
        sq.x.form = form;
    }

    // Send the ServerQuery to database thread ( and get it back ).
    let mut state = state.lock().await;
    let _x = state.tx.send(sq).await;
    state.rx.recv().await.unwrap()
}

use axum::{
    body::{Bytes, Full},
    http::header::HeaderName,
    http::status::StatusCode,
    http::{HeaderValue, Response},
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
