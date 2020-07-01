use gumdrop::Options;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};
use hyper::body::{Buf, Bytes};
use log::info;
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

async fn get_channel(channels: Arc<RwLock<ChannelMap>>, x: &str) -> Response<Body> {
    let mut response = Response::new(Body::default());
    let resp: Vec<&str> = x.split("/").skip(1).collect();
    if resp.len() != 1 {
        *response.status_mut() = StatusCode::BAD_REQUEST;
        *response.body_mut() = Body::from(format!("{:?}", resp));
    } else {
        let channels = channels.clone();
        let hash = channels.read().await;
        match hash.get(resp[0]) {
            Some(chan) => {
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

async fn produce(data: Bytes, tx: Arc<Mutex<Sender<Stream>>>) -> Response<Body>{
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


async fn post_channel(
    req: Request<Body>,
    channels: Arc<RwLock<ChannelMap>>,
    x: &str,
) -> Result<Response<Body>> {
    let resp: Vec<&str> = x.split("/").skip(1).collect();
	let mut whole_body = hyper::body::aggregate(req).await?;
	let data = whole_body.to_bytes();
    if resp.len() != 1 {
        let mut response = Response::new(Body::default());
        *response.status_mut() = StatusCode::BAD_REQUEST;
        *response.body_mut() = Body::from(format!("{:?}", resp));
        Ok(response)
    } else {
        let hash = channels.read().await;
        match hash.get(resp[0]) {
            Some(chan) => {
                let tx = Arc::clone(&chan.tx);
                drop(hash);
                Ok(produce(data, tx).await)
            }
            None => {
                drop(hash);
                let s = resp[0];
                let chan = Channel::new();
                let tx = Arc::clone(&chan.tx);
                let mut hash = channels.write().await;
                hash.insert(s.to_owned(), chan);
                drop(hash);
                Ok(produce(data, tx).await)
            }
        }
    }
}

async fn serve_request(
    req: Request<Body>,
    channels: Arc<RwLock<ChannelMap>>,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok::<_, hyper::Error>(response),
        (&Method::POST, "/") => {
            *response.status_mut() = StatusCode::BAD_REQUEST;
            Ok(response)
        },
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
    println!("{:?}", opts);
    let ip_addr: IpAddr = match opts.address.parse() {
        Ok(x) => x,
        _ => [127, 0, 0, 1].into(),
    };
    let port = match opts.port {
        0 => 8080,
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
	use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};
	use super::*;
	use tokio;
	use std::io::Read;
	use futures::TryStreamExt;

	#[tokio::test]
	async fn test_get_creates_channel(){
		let channels = Arc::new(RwLock::new(ChannelMap::new()));
		let  req = Request::new(Body::from("Hola mundo"));
		let (get, _) = tokio::join!(
			get_channel(Arc::clone(&channels), "/pepito"),
			post_channel(req, Arc::clone(&channels), "/pepito")
		);
		let f = get.into_body().try_fold(Vec::new(),| mut acc, chunk| {
			acc.extend_from_slice(&*chunk);
			futures::future::ok::<_,hyper::Error>(acc)
		}).await.unwrap();
		let s = std::str::from_utf8(f.as_slice()).unwrap();
		assert_eq!(s, "Hola mundo")
	}

	#[tokio::test]
	async fn test_post_creates_channel(){
		let channels = Arc::new(RwLock::new(ChannelMap::new()));
		let mut req = Request::new(Body::from("Hola mundo"));
		let (_, get) = tokio::join!(
			post_channel(req, Arc::clone(&channels), "/pepito"),
			get_channel(Arc::clone(&channels), "/pepito"),
		);
		let f = get.into_body().try_fold(Vec::new(),| mut acc, chunk| {
			acc.extend_from_slice(&*chunk);
			futures::future::ok::<_,hyper::Error>(acc)
		}).await.unwrap();
		let s = std::str::from_utf8(f.as_slice()).unwrap();
		assert_eq!(s, "Hola mundo")
	}


}
