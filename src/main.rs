use futures::stream::TryStreamExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{Mutex, Notify, RwLock};

type Producer<T> = Arc<Mutex<Sender<T>>>;
type Consumer<T> = Arc<Mutex<Receiver<T>>>;


struct Stream {
    body: Vec<u8>,
    notify: Arc<Notify>,
}

impl Stream {
    pub fn new(body: Vec<u8>) -> Stream {
        Stream {
            body,
            notify: Arc::new(Notify::new()),
        }
    }
}

async fn consume(data: Option<Stream>) -> Option<Vec<u8>> {
    match data {
        Some(stream) => {
            let mut data = Vec::<u8>::new();
            data.clone_from(&stream.body);
            let notif = Arc::clone(&stream.notify);
            notif.notify();
            Some(data)
        }
        None => None,
    }
}

async fn serve_request(
    req: Request<Body>,
    channels: Arc<RwLock<ChannelMap>>,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {}
        (&Method::POST, "/") => *response.status_mut() = StatusCode::BAD_REQUEST,
        (&Method::GET, x) => {
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
        }
        (&Method::POST, x) => {
            let resp: Vec<&str> = x.split("/").skip(1).collect();
            if resp.len() != 1 {
                *response.status_mut() = StatusCode::BAD_REQUEST;
                *response.body_mut() = Body::from(format!("{:?}", resp));
            } else {
                let hash = channels.read().await;
                match hash.get(resp[0]) {
                    Some(chan) => {
						let tx = Arc::clone(&chan.tx);
                        let mut tx = tx.lock().await;
                        let data: Result<Vec<u8>> = req
                            .into_body()
                            .map_ok(|x| x)
                            .try_fold(Vec::new(), |mut vec, chunk| {
                                vec.extend_from_slice(&chunk);
                                async { Ok(vec) }
                            })
                            .await;
                        match data {
                            Ok(v) => {
                                let stream = Stream::new(v);
                                //let rx = stream.consumer
                                tx.send(stream).await;
                            }
                            Err(_) => {}
                        }
                    }
                    None => {
                    }
                };
            }
        }
        _ => *response.status_mut() = StatusCode::BAD_REQUEST,
    };
    Ok(response)
}

struct Channel {
    tx: Producer<Stream>,
    rx: Consumer<Stream>,
}

impl Channel {
    fn new() -> Channel {
        let (mut tx, mut rx) = channel::<Stream>(1);
        Channel {
            tx: Arc::new(Mutex::new(tx)),
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

type ChannelMap = HashMap<String, Channel>;

#[tokio::main]
async fn main() {
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
    let addr = ([127, 0, 0, 1], 3000).into();
    let server = Server::bind(&addr).serve(make_service);
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
