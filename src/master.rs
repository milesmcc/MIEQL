use hyper::rt;
use hyper::service::service_fn_ok;
use hyper::{Body, Method, Request, Response, Server};
use rt::Future;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use ron;

use ieql::output::output::OutputBatch;

pub fn main(addr: SocketAddr) {
    info!("running as master...");

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
                }
                "/data" => {
                    info!("received data request");
                    get_data(&mut *queue.lock().unwrap())
                }
                "/output" => {
                    info!("received output");
                    post_output(&outputs)
                }
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
    Response::builder()
        .status(500)
        .body(Body::from("not yet implemented"))
        .unwrap()
}

fn get_data(queue: &mut VecDeque<String>) -> Response<Body> {
    let item = queue.pop_front();
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

fn post_output(output_transmitter: &mpsc::Sender<OutputBatch>) -> Response<Body> {
    Response::builder()
        .status(500)
        .body(Body::from("not yet implemented"))
        .unwrap()
}
