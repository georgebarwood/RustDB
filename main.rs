use axum::{
    extract::{Extension, Form, Multipart, Path, Query},
    response::Html,
    routing::get,
    AddExtensionLayer, Router,
};

use tower::ServiceBuilder;
use tower_cookies::{Cookie, CookieManagerLayer, Cookies};

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use database::genquery::GenQuery;
use database::Database;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

type GQ = Box<GenQuery>;

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
            // println!("Server got query {}", q.path);
            let sql = "EXEC [handler].[".to_string() + &q.path + "]()";
            db.run_timed(&sql, &mut *q);
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
) -> Html<String> {
    // let result = tokio::task::block_in_place(|| "hello world!");

    let mut c = Cookie::new("MyCookie", "Hi George");
    c.set_path("/");
    cookies.add(c);

    let mut q = Box::new(database::genquery::GenQuery::new());
    q.path = path;
    q.query = params;

    let q : GQ = {
        if q.path == "/favicon.ico"
        {
          q
        }
        else
        {
          let mut state = state.lock().await;
          let _x = state.tx.send(q).await;
          state.rx.recv().await.unwrap()
        }
    };

    let s = std::str::from_utf8(&q.output).unwrap();
    Html(s.to_string())
}

async fn my_post_handler(
    Extension(state): Extension<Arc<Mutex<SharedState>>>,
    Path(path): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    form: Option<Form<HashMap<String, String>>>,
    _cookies: Cookies,
    _mp: Option<Multipart>,
) -> Html<String> {
/*
    let mut mpinfo = String::new();
    if let Some(mut mp) = mp {
        mpinfo += "multipart form!!";
        while let Some(field) = mp.next_field().await.unwrap() {
            let name = field.name().unwrap().to_string();
            let filename = match field.file_name() {
                Some(s) => s.to_string(),
                None => "No filename".to_string(),
            };
            let ct = match field.content_type() {
                Some(s) => s.to_string(),
                None => "".to_string(),
            };
            let mut datalen = 0;
            let mut text = "".to_string();
            if ct == "" {
                match field.text().await {
                    Ok(s) => text = s,
                    Err(_) => {}
                }
            } else {
                datalen = match field.bytes().await {
                    Ok(bytes) => bytes.len(),
                    Err(_) => 0,
                };
            }

            mpinfo += &format!(
                "<p>name is `{}` filename is `{}` ct is `{}` data len is {} bytes text is {}",
                name, filename, ct, datalen, text
            );
        }
    }
    let s = format!(
        "<p>Hi George path is '{}' and params are {:?}  <p>cookies {:?} form is {:?} mpinfo {}",
        path, params, cookies, form, mpinfo
    );
    Html(s)
*/

    let mut q = Box::new(database::genquery::GenQuery::new());
    q.path = path;
    q.query = params;
    if let Some(Form(form)) = form
    {
      q.form = form;
    }

    let q : GQ = {
        let mut state = state.lock().await;
        let _x = state.tx.send(q).await;
        state.rx.recv().await.unwrap()
    };

    let s = std::str::from_utf8(&q.output).unwrap();
    Html(s.to_string())
}
