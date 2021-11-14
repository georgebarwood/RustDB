use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use axum::{
    extract::{Extension, Form, Multipart, Path, Query},
    routing::get,
    AddExtensionLayer, Router,
};

use tower::ServiceBuilder;
use tower_cookies::{Cookie, CookieManagerLayer, Cookies};

use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use database::genquery::{GenQuery, Part};
use database::Database;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

struct GQ {
    pub x: Box<GenQuery>,
}

impl GQ {
    pub fn new() -> Self {
        Self {
            x: Box::new(GenQuery::new()),
        }
    }
}

struct SharedState {
    tx: mpsc::Sender<GQ>,
    rx: mpsc::Receiver<GQ>,
}

#[tokio::main]
async fn main() {
    let (tx, mut server_rx): (Sender<GQ>, Receiver<GQ>) = mpsc::channel(1);
    let (server_tx, rx): (Sender<GQ>, Receiver<GQ>) = mpsc::channel(1);

    // This is the server thread (synchronous).
    thread::spawn(move || {
        let stg = Box::new(database::stg::SimpleFileStorage::new(
            "c:\\Users\\pc\\rust\\sftest01.rustdb",
        ));
        let db = Database::new(stg, database::init::INITSQL);
        loop {
            let mut q = server_rx.blocking_recv().unwrap();
            let sql = "EXEC [handler].[".to_string() + &q.x.path + "]()";
            db.run_timed(&sql, &mut *q.x);
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
    cookies: Cookies,
) -> GQ {
    let mut c = Cookie::new("MyCookie", "Hi George");
    c.set_path("/");
    cookies.add(c);

    let mut q = GQ::new();
    q.x.path = path;
    q.x.query = params;

    let q: GQ = {
        if q.x.path == "/favicon.ico" {
            q
        } else {
            let mut state = state.lock().await;
            let _x = state.tx.send(q).await;
            state.rx.recv().await.unwrap()
        }
    };

    q
}

async fn my_post_handler(
    Extension(state): Extension<Arc<Mutex<SharedState>>>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    form: Option<Form<HashMap<String, String>>>,
    _cookies: Cookies,
    mp: Option<Multipart>,
) -> GQ {
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

    let mut q = GQ::new();
    q.x.path = path;
    q.x.query = params;
    q.x.parts = parts;
    if let Some(Form(form)) = form {
        q.x.form = form;
    }

    let q: GQ = {
        // Send the Query to the database server thread.
        let mut state = state.lock().await;
        let _x = state.tx.send(q).await;
        // Get the Query back again.
        state.rx.recv().await.unwrap()
    };

    q
}

use axum::body::{Bytes, Full};
use axum::http::header::HeaderName;
use axum::http::status::StatusCode;
use axum::http::{HeaderValue, Response};
use axum::response::IntoResponse;
use std::convert::Infallible;

impl IntoResponse for GQ {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

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
