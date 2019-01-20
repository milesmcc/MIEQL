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

pub fn main(addr: SocketAddr) {
    info!("running as master on `{}`...", addr);

    let mut url_queue: VecDeque<String> = VecDeque::new();
    url_queue.push_back(String::from("val 1"));
    url_queue.push_back(String::from("val 2"));
    url_queue.push_back(String::from("val 3"));
    url_queue.push_back(String::from("val 4"));
    url_queue.push_back(String::from("val 5"));
    url_queue.push_back(String::from("val 6"));
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
                    get_data(&mut *queue.lock().unwrap())
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
