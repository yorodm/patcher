use gumdrop::Options;
use hyper::body::{Buf, Bytes};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};
use log::{info, warn};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use stderrlog;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, Notify, RwLock};

type Producer<T> = Arc<Mutex<Sender<T>>>;
type Consumer<T> = Arc<Mutex<Receiver<T>>>;

struct Stream {
    body: Bytes,
    notify: Arc<Notify>,
}

impl Stream {
    pub fn new(body: Bytes) -> Stream {
        Stream {
            body,
            notify: Arc::new(Notify::new()),
        }
    }
}

async fn consume(data: Option<Stream>) -> Option<Bytes> {
    match data {
        Some(stream) => {
            let bytes = stream.body;
            let notif = Arc::clone(&stream.notify);
            notif.notify();
            Some(bytes)
        }
        None => None,
    }
}

async fn produce(data: Bytes, tx: Arc<Mutex<Sender<Stream>>>) -> Response<Body> {
    let mut response = Response::new(Body::default());
    let mut tx = tx.lock().await;
    let stream = Stream::new(data);
    let notif = Arc::clone(&stream.notify);
    match tx.send(stream).await {
        Ok(_) => {
            notif.notified().await;
        }
        Err(_) => {
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    response
}

async fn handle_single(
    channels: Arc<RwLock<ChannelMap>>,
    resp: &[&str],
    data: Bytes,
) -> Result<Response<Body>> {
    if resp.len() != 1 {
        let mut response = Response::new(Body::default());
        *response.status_mut() = StatusCode::BAD_REQUEST;
        *response.body_mut() = Body::from(format!("{:?}", resp));
        Ok(response)
    } else {
        let hash = channels.read().await;
        match hash.get(resp[0]) {
            Some(chan) => {
				info!("Producing data to channel {}", resp[0]);
                let tx = Arc::clone(&chan.tx);
                drop(hash);
                Ok(produce(data, tx).await)
            }
            None => {
                drop(hash);
                let mut hash = channels.write().await;
                let s = resp[0];
				info!("Creating consumer for channel {}", resp[0]);
                let chan = Channel::new();
                let tx = Arc::clone(&chan.tx);
                hash.insert(s.to_owned(), chan);
                drop(hash);
                Ok(produce(data, tx).await)
            }
        }
    }
}

async fn get_channel(channels: Arc<RwLock<ChannelMap>>, x: &str) -> Response<Body> {
    let mut response = Response::new(Body::default());
    let resp: Vec<&str> = x.split("/").skip(1).collect();
    if resp.len() != 1 {
        warn!("To many segments in request {}", x);
        *response.status_mut() = StatusCode::BAD_REQUEST;
        *response.body_mut() = Body::from(format!("{:?}", resp));
    } else {
        let channels = channels.clone();
        let hash = channels.read().await;
        match hash.get(resp[0]) {
            Some(chan) => {
                info!("Consuming channel {}", resp[0]);
                let rx = Arc::clone(&chan.rx);
                let mut rx = rx.lock().await;
                drop(hash);
                let data = rx.recv().await;
                match consume(data).await {
                    Some(x) => *response.body_mut() = Body::from(x),
                    _ => *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR,
                };
            }
            None => {
                drop(hash);
                let s = resp[0];
                info!("Creating new consumer for channel {}", s);
                let chan = Channel::new();
                let rx = Arc::clone(&chan.rx);
                let mut rx = rx.lock().await;
                let mut hash = channels.write().await;
                hash.insert(s.to_owned(), chan);
                drop(hash);
                let data = rx.recv().await;
                match consume(data).await {
                    Some(x) => *response.body_mut() = Body::from(x),
                    _ => *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR,
                };
            }
        };
    }
    response
}

async fn post_channel(
    req: Request<Body>,
    channels: Arc<RwLock<ChannelMap>>,
    x: &str,
) -> Result<Response<Body>> {
    let resp: Vec<&str> = x.split("/").skip(1).collect();
    let mut whole_body = hyper::body::aggregate(req).await?;
    let data = whole_body.to_bytes();
    let first = resp[0];
    match first {
        "pubsub" => {
            let mut response = Response::new(Body::default());
            *response.status_mut() = StatusCode::BAD_REQUEST;
            *response.body_mut() = Body::from(format!("{:?}", resp));
            warn!("Requesting pubsub channel!");
            Ok(response)
        }
        _ => {
            let channels = Arc::clone(&channels);
            handle_single(channels, &resp, data).await
        }
    }
}

async fn serve_request(
    req: Request<Body>,
    channels: Arc<RwLock<ChannelMap>>,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            warn!("GET request for / context");
            Ok::<_, hyper::Error>(response)
        }
        (&Method::POST, "/") => {
            warn!("POST request for / context");
            *response.status_mut() = StatusCode::BAD_REQUEST;
            Ok(response)
        }
        (&Method::GET, x) => Ok(get_channel(channels, x).await),
        (&Method::POST, x) => {
            let path = String::from(x);
            post_channel(req, channels, &path).await
        }
        _ => {
            *response.status_mut() = StatusCode::BAD_REQUEST;
            Ok(response)
        }
    }
}

#[derive(Debug)]
struct Channel {
    tx: Producer<Stream>,
    rx: Consumer<Stream>,
}

impl Channel {
    fn new() -> Channel {
        let (tx, rx) = channel::<Stream>(1);
        Channel {
            tx: Arc::new(Mutex::new(tx)),
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

type ChannelMap = HashMap<String, Channel>;

#[derive(Debug, Options)]
struct Opts {
    address: String,
    port: u16,
    help: bool,
}
#[tokio::main]
async fn main() {
    stderrlog::new()
        .module(module_path!())
        .verbosity(5)
        .init()
        .unwrap();
    let opts = Opts::parse_args_default_or_exit();
    let ip_addr: IpAddr = match opts.address.parse() {
        Ok(x) => x,
        _ => [0, 0, 0, 0].into(),
    };
    let port = match opts.port {
        0 => std::env::var("PORT")
            .unwrap()
            .parse()
            .expect("Heroku should provide the PORT environment variable"),
        x => x,
    };
    let channels = Arc::new(RwLock::new(ChannelMap::new()));
    let make_service = make_service_fn(|_| {
        let channels = Arc::clone(&channels);
        async {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                let channels = Arc::clone(&channels);
                serve_request(req, channels)
            }))
        }
    });
    let addr = (ip_addr, port).into();
    info!("Server started at {}", addr);
    let server = Server::bind(&addr)
        .serve(make_service)
        .with_graceful_shutdown(async { ctrl_c().await.expect("Error setting signal") });

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::TryStreamExt;
    use hyper::{Body, Request};
    use tokio;

    #[tokio::test]
    async fn test_get_creates_channel() {
        let channels = Arc::new(RwLock::new(ChannelMap::new()));
        let req = Request::new(Body::from("Hola mundo"));
        let (get, _) = tokio::join!(
            get_channel(Arc::clone(&channels), "/pepito"),
            post_channel(req, Arc::clone(&channels), "/pepito")
        );
        let f = get
            .into_body()
            .try_fold(Vec::new(), |mut acc, chunk| {
                acc.extend_from_slice(&*chunk);
                futures::future::ok::<_, hyper::Error>(acc)
            })
            .await
            .unwrap();
        let s = std::str::from_utf8(f.as_slice()).unwrap();
        assert_eq!(s, "Hola mundo")
    }

    #[tokio::test]
    async fn test_multiple_get_creates_channel() {
        let channels = Arc::new(RwLock::new(ChannelMap::new()));
        let req = Request::new(Body::from("Hola mundo"));
        let req1 = Request::new(Body::from("Hola mundo"));
        let (get1, _, get2, _) = tokio::join!(
            get_channel(Arc::clone(&channels), "/pepito"),
            post_channel(req1, Arc::clone(&channels), "/pepito"),
            get_channel(Arc::clone(&channels), "/cuquito"),
            post_channel(req, Arc::clone(&channels), "/cuquito")
        );
        let f1 = get1
            .into_body()
            .try_fold(Vec::new(), |mut acc, chunk| {
                acc.extend_from_slice(&*chunk);
                futures::future::ok::<_, hyper::Error>(acc)
            })
            .await
            .unwrap();
        let s1 = std::str::from_utf8(f1.as_slice()).unwrap();
        let f2 = get2
            .into_body()
            .try_fold(Vec::new(), |mut acc, chunk| {
                acc.extend_from_slice(&*chunk);
                futures::future::ok::<_, hyper::Error>(acc)
            })
            .await
            .unwrap();
        let s2 = std::str::from_utf8(f2.as_slice()).unwrap();
        assert_eq!(s1, "Hola mundo");
        assert_eq!(s2, "Hola mundo")
    }

    #[tokio::test]
    async fn test_post_creates_channel() {
        let channels = Arc::new(RwLock::new(ChannelMap::new()));
        let req = Request::new(Body::from("Hola mundo"));
        let (_, get) = tokio::join!(
            post_channel(req, Arc::clone(&channels), "/pepito"),
            get_channel(Arc::clone(&channels), "/pepito"),
        );
        let f = get
            .into_body()
            .try_fold(Vec::new(), |mut acc, chunk| {
                acc.extend_from_slice(&*chunk);
                futures::future::ok::<_, hyper::Error>(acc)
            })
            .await
            .unwrap();
        let s = std::str::from_utf8(f.as_slice()).unwrap();
        assert_eq!(s, "Hola mundo")
    }
}
