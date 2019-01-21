use hyper::rt;
use hyper::service::service_fn_ok;
use hyper::{Body, Method, Request, Response, Server};
use rt::{Future, Stream};

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use ron;

use ieql::output::output::OutputBatch;
use ieql::QueryGroup;

use super::util;

pub fn main(addr: SocketAddr, debug: bool) {
    info!("running as master on `{}`...", addr);
    if debug {
        info!("running in debug mode!")
    }
    let mut url_queue: VecDeque<String> = VecDeque::new();
    let url_queue = Arc::new(Mutex::new(url_queue));
    let (output_transmitter, output_receiver) = mpsc::channel::<OutputBatch>();

    let server = Server::bind(&addr)
        .serve(move || {
            let queue = url_queue.clone();
            let outputs = output_transmitter.clone();
            service_fn_ok(move |req: Request<Body>| match req.uri().path() {
                "/queries" => {
                    info!("received query request");
                    get_queries()
                },
                "/data" => {
                    info!("received data request");
                    if debug {
                        get_debug_data()
                    } else {
                        get_data(&mut *queue.lock().unwrap())
                    }
                },
                "/output" => {
                    info!("received output");
                    post_output(req, &outputs)
                },
                "/handshake" => {
                    info!("received handshake");
                    Response::builder()
                        .status(200)
                        .body(Body::from("nice to meet you"))
                        .unwrap()
                },
                _ => {
                    info!("received unknown request `{}`", req.uri().path());
                    Response::builder()
                        .status(404)
                        .body(Body::from("not found"))
                        .unwrap()
                }
            })
        })
        .map_err(|e| error!("server error: {}", e));

    hyper::rt::run(server);
}

fn get_queries() -> Response<Body> {
    let queries = QueryGroup {
        queries: vec![util::get_query()]
    }; // TODO: link to database
    let response_str = ron::ser::to_string(&queries).unwrap(); // data is from a trusted source
    Response::builder()
        .status(200)
        .body(Body::from(response_str))
        .unwrap()
}

fn get_debug_data() -> Response<Body> {
    Response::new(Body::from(String::from("crawl-data/CC-MAIN-2018-51/segments/1544376823710.44/warc/CC-MAIN-20181212000955-20181212022455-00124.warc.gz")))
}

fn get_data(queue: &mut VecDeque<String>) -> Response<Body> {
    let item = queue.pop_front();
    info!(
        "data request returned `{:?}` (queue size: {})",
        item,
        queue.len()
    );
    match item {
        Some(value) => Response::builder()
            .status(200)
            .body(Body::from(value))
            .unwrap(),
        None => Response::builder()
            .status(204)
            .body(Body::from(""))
            .unwrap(),
    }
}

fn post_output(
    req: Request<Body>,
    output_transmitter: &mpsc::Sender<OutputBatch>,
) -> Response<Body> {
    let data: Vec<u8> = Vec::new();
    let output_batch: Result<OutputBatch, _> = ron::de::from_bytes(data.as_slice());
    return match output_batch {
        Ok(value) => {
            output_transmitter.send(value);
            Response::builder()
                .status(200)
                .body(Body::from(String::from("ok")))
                .unwrap()
        }
        Err(_) => Response::builder()
            .status(400)
            .body(Body::from(String::from("bad request")))
            .unwrap(),
    };
}
