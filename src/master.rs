use futures::future;
use hyper::rt;
use hyper::service::service_fn_ok;
use hyper::{Body, Method, Request, Response, Server};
use postgres::Connection;
use rt::{Future, Stream};

use futures;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use ron;

use ieql::output::output::OutputBatch;
use ieql::{Query, QueryGroup};

use super::util;

pub fn main(
    addr: SocketAddr,
    debug: bool,
    debug_queries: usize,
    database_location: &str,
    remove_urls: bool,
) {
    info!("running as master on `{}`...", addr);
    if debug {
        info!("running in debug mode!")
    }

    let conn = match Connection::connect(database_location, postgres::TlsMode::None) {
        Ok(value) => value,
        Err(error) => {
            error!(
                "encountered error while connecting to the database: `{}`",
                error
            );
            return;
        }
    }; // connection should be on localhost

    match verify_database(&conn) {
        true => info!("successfully validated database"),
        false => error!("database validation failed; make sure the following tables are available: `queries`, `outputs`, `inputs`")
    }

    let sql_conn = Arc::new(Mutex::new(conn));
    let (output_transmitter, output_receiver) = mpsc::channel::<OutputBatch>();

    let queries: Arc<Mutex<Option<QueryGroup>>> = Arc::new(Mutex::new(match pull_queries_from_db(
        &sql_conn.lock().unwrap(),
    ) {
        Some(value) => {
            info!("loaded {} queries from the database", value.queries.len());
            if debug_queries > 0 && debug {
                warn!("because debug queries are specified and debug mode is enabled, the database queries will be ignored");
            }
            Some(value)
        }
        None => {
            error!("unable to pull queries from the database!");
            None
        }
    }));

    let conn = sql_conn.clone();
    let outputs = output_transmitter.clone();
    let query_group = queries.clone();

    let server = Server::bind(&addr)
        .serve(move || {
            let conn = conn.clone();
            let outputs = outputs.clone();
            let query_group = query_group.clone();
            service_fn_ok(move |req: Request<Body>| match req.uri().path() {
                "/queries" => {
                    info!("received query request");
                    get_queries(&*query_group.lock().unwrap(), debug_queries)
                }
                "/data" => {
                    info!("received data request");
                    if debug {
                        get_debug_data()
                    } else {
                        get_data(pull_url_from_db(&conn.lock().unwrap(), remove_urls))
                    }
                }
                "/output" => {
                    info!("received output");
                    post_output(req, &outputs)
                }
                "/handshake" => {
                    info!("received handshake");
                    Response::builder()
                        .status(200)
                        .body(Body::from("nice to meet you"))
                        .unwrap()
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

    let conn = sql_conn.clone();
    thread::spawn(move || loop {
        // database output thread for outputs
        let batch = match output_receiver.recv() {
            Ok(value) => value,
            Err(error) => break,
        };
        info!("received output batch; sending it to database...");
        push_output_batch_to_db(&conn.lock().unwrap(), &batch);
    });

    hyper::rt::run(server);

    info!("listening...")
}

fn get_queries(db_queries: &Option<QueryGroup>, debug_queries: usize) -> Response<Body> {
    let debug_query_group = {
        let mut query_vec: Vec<Query> = Vec::new();
        for _ in 0..debug_queries {
            query_vec.push(util::get_query());
        }
        QueryGroup { queries: query_vec }
    };
    let mut queries = match db_queries {
        Some(value) => value,
        None => &debug_query_group,
    };

    let response_str = ron::ser::to_string(&queries).unwrap(); // data is from a trusted source
    Response::builder()
        .status(200)
        .body(Body::from(response_str))
        .unwrap()
}

fn get_debug_data() -> Response<Body> {
    Response::new(Body::from(String::from("crawl-data/CC-MAIN-2018-51/segments/1544376823710.44/warc/CC-MAIN-20181212000955-20181212022455-00124.warc.gz")))
}

fn get_data(url: Option<String>) -> Response<Body> {
    match url {
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
    let transmitter = output_transmitter.clone();
    thread::spawn(move || {
        let data: String = match req
            .into_body()
            .fold(Vec::new(), |mut v, chunk| {
                v.extend(&chunk[..]);
                future::ok::<_, hyper::Error>(v)
            })
            .and_then(move |chunks| {
                let body = String::from_utf8(chunks).unwrap_or(String::from(""));
                future::ok(body)
            })
            .wait()
        {
            Ok(value) => value,
            Err(error) => {
                warn!("encountered error while loading POST data: `{}`", error);
                return;
            }
        };
        match ron::de::from_str(data.as_str()) {
            Ok(value) => {
                transmitter.send(value);
            }
            Err(error) => warn!("unable to deserialize output batch: `{}`", error),
        };
    });
    Response::builder()
        .status(200)
        .body(Body::from(String::from("ok")))
        .unwrap()
}

fn verify_database(conn: &postgres::Connection) -> bool {
    let result = match conn.query("SELECT tablename FROM pg_catalog.pg_tables;", &[]) {
        Ok(value) => value,
        Err(error) => {
            error!(
                "encountered an issue while trying to verify database: `{}`",
                error
            );
            return false;
        }
    };
    let mut queries_present = false;
    let mut outputs_present = false;
    let mut inputs_present = false;

    for row in &result {
        let tablename_string: String = row.get(0);
        let tablename = tablename_string.as_str();
        match tablename {
            "queries" => queries_present = true,
            "outputs" => outputs_present = true,
            "inputs" => inputs_present = true,
            _ => (),
        }
    }

    queries_present && outputs_present && inputs_present
}

fn push_output_batch_to_db(conn: &postgres::Connection, outputs: &OutputBatch) {
    let prepared_statement = conn
        .prepare("INSERT INTO outputs (json) VALUES ($1)")
        .unwrap();
    for output in &outputs.outputs {
        // TODO: do as a single batch
        let json = match serde_json::to_value(&output) {
            Ok(value) => value,
            Err(error) => {
                warn!("unable to serialize an output (`{}`), skipping...", error);
                continue;
            }
        };
        match prepared_statement.execute(&[&json]) {
            Ok(_) => (),
            Err(error) => warn!("unable to push data into database: `{}`", error),
        }
    }
}

fn pull_queries_from_db(conn: &postgres::Connection) -> Option<QueryGroup> {
    let query_response = match conn.query("SELECT ron FROM queries", &[]) {
        Ok(value) => value,
        Err(error) => {
            warn!("encountered error while retrieving queries: `{}`", error);
            return None;
        }
    };

    let mut queries: Vec<Query> = Vec::new();
    for row in &query_response {
        let ron_data: String = row.get(0);
        let query: Query = match ron::de::from_str(ron_data.as_str()) {
            Ok(value) => value,
            Err(error) => {
                warn!("{}", ron_data);
                warn!("unable to deserialize query: `{}`", error);
                continue;
            }
        };
        queries.push(query);
    }

    Some(QueryGroup { queries: queries })
}

fn pull_url_from_db(conn: &postgres::Connection, delete: bool) -> Option<String> {
    let response = match conn.query("SELECT url FROM inputs LIMIT 1", &[]) {
        Ok(value) => value,
        Err(error) => {
            warn!("encountered error while retrieving urls: `{}`", error);
            return None;
        }
    };

    let mut url = String::new();
    for row in &response {
        url = row.get(0);
    }

    if delete {
        let response = conn.execute("DELETE FROM inputs WHERE url = $1", &[&url]);
        match response {
            Ok(value) => info!("successfully purged URL from database ({})", value),
            Err(error) => warn!("unable to remove URL from the database (`{}`); duplicate work might be performed...", error)
        }
    }

    Some(url)
}
