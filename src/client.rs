use flate2::read::MultiGzDecoder;
use ieql::common::compilation::CompilableTo;
use ieql::output::output::OutputBatch;
use ieql::query::query::{Query, QueryGroup};
use ieql::scan::scanner::{AsyncScanInterface, Scanner};
use rusoto_s3;
use rusoto_s3::S3;
use serde_json::Value;
use std::io::Read;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

enum RequestMethod {
    Get,
    Post,
}

fn get_authenticated(access_key: &str, url: &str, method: RequestMethod) -> Result<Value, String> {
    let client = reqwest::Client::new();
    let res = match method {
        RequestMethod::Get => client.get(url).header("X-Access-Key", access_key).send(),
        RequestMethod::Post => client.post(url).header("X-Access-Key", access_key).send(),
    };
    match res {
        Ok(mut data) => match data.text() {
            Ok(json_value) => match serde_json::from_str(json_value.as_str()) {
                Ok(inner_text_value) => Ok(inner_text_value),
                Err(_) => Err(format!("invalid json returned")),
            },
            Err(_) => Err(String::from("unable to parse response text")),
        },
        Err(_) => Err(String::from("unable to extract response text")),
    }
}

fn post_outputs(access_key: &str, url: &str, outputs: OutputBatch) -> Result<u64, String> {
    let client = reqwest::Client::new();
    let data = match serde_json::to_string_pretty(&outputs) {
        Ok(value) => {
            // println!("{}", value);
            value
        }
        Err(_) => {
            return Err(String::from("unable to serialize outputs"));
        }
    };
    let res = client
        .post(url)
        .header("X-Access-Key", access_key)
        .header("Content-Type", "application/json")
        .body(data)
        .send();
    match res {
        Ok(mut data) => match data.text() {
            Ok(json_value) => match serde_json::from_str(json_value.as_str()) {
                Ok(parsed_value) => {
                    let value: Value = parsed_value;
                    match value["data"]["new_outputs"].as_u64() {
                        Some(num) => Ok(num),
                        None => return Err(format!("malformed json returned")),
                    }
                }
                Err(_) => Err(format!("invalid json returned")),
            },
            Err(_) => Err(String::from("unable to parse response text")),
        },
        Err(_) => Err(String::from("unable to connect")),
    }
}

pub fn main(
    master_url: String,
    secret_key: String,
    threads: u8,
    queue_size: isize,
    update_interval: u64,
) {
    // Establish connection & get access key
    //
    // Note that because any given instance will never last more than 24 hours
    // and access keys last 48 hours, re-establishing the connection is never
    // necessary.
    let registration_url = format!("{}/register/{}", &master_url, &secret_key);
    let registration_response = (match reqwest::get(registration_url.as_str()) {
        Ok(value) => value,
        Err(error) => {
            error!("unable to connect to master server: `{}`", error);
            std::process::exit(101);
        }
    })
    .text()
    .unwrap();
    // TODO: proper error handling here

    let registration_data: Value = serde_json::from_str(registration_response.as_str()).unwrap();

    let access_key: String = registration_data["data"]["access_key"].to_string();

    info!(
        "successfully established access key with master: {}",
        access_key
    );
    // Create dataset client
    let s3_client = rusoto_s3::S3Client::new(rusoto_core::region::Region::UsEast1);

    // Get queries
    let queries_url = format!("{}/queries/", &master_url);
    let queries_response = match get_authenticated(
        access_key.as_str(),
        queries_url.as_str(),
        RequestMethod::Get,
    ) {
        Ok(value) => value,
        Err(issue) => {
            error!("unable to get queries: {}", issue);
            std::process::exit(101);
        }
    };

    let mut query_vec: Vec<Query> = Vec::new();

    for query_val in queries_response["data"]["queries"].as_array().unwrap() {
        let id = String::from(query_val["id"].as_str().unwrap());
        println!("{}", id);
        let mut query: Query = match ron::de::from_str(query_val["ieql"].as_str().unwrap()) {
            Ok(parsed_query) => parsed_query,
            Err(_) => {
                error!("unable to parse query; dying...");
                std::process::exit(101);
            }
        };
        query.id = Some(id);
        query_vec.push(query);
    }
    let queries: QueryGroup = QueryGroup { queries: query_vec };
    info!(
        "successfully loaded {} queries from master",
        queries.queries.len()
    );

    // Create scan engine
    let compiled_queries = match queries.compile() {
        Ok(value) => value,
        Err(error) => {
            error!("unable to compile queries: {}", error);
            std::process::exit(101);
        }
    };
    let scan_interface: AsyncScanInterface = compiled_queries.scan_concurrently(threads);

    // Analytics
    let mut documents_processed = 0u64;
    let mut total_outputs = 0;
    let mut start_time = SystemTime::now();

    // Reqwest client
    let client = reqwest::Client::new();

    // Stream and process an archive
    loop {
        // Stream loop
        let data_url = format!("{}/source/", &master_url);
        let (url_to_stream, data_id) = match get_authenticated(
            access_key.as_str(),
            data_url.as_str(),
            RequestMethod::Get,
        ) {
            Ok(value) => match (
                value["data"]["location"].as_str(),
                value["data"]["id"].as_str(),
            ) {
                (Some(location), Some(id)) => (String::from(location), String::from(id)),
                _ => {
                    error!("data queue is empty; sleeping for five minutes, refreshing authorization, and then trying again...");
                    thread::sleep(Duration::from_millis(60000 * 5));
                    let revoke_url = format!("{}/unregister/", &master_url);
                    match get_authenticated(
                        access_key.as_str(),
                        revoke_url.as_str(),
                        RequestMethod::Get,
                    ) {
                        Ok(_) => info!("successfully revoked authorization; will try again..."),
                        Err(err) => error!("unable to revoke authorization: `{}`", err),
                    }
                    main(master_url, secret_key, threads, queue_size, update_interval);
                    return;
                }
            },
            Err(error) => {
                error!("unable to get data location from master: `{}`", error);
                std::process::exit(101);
            }
        };
        // Reset stats
        start_time = SystemTime::now();
        total_outputs = 0;
        documents_processed = 0;

        info!("found data `{}` to process", url_to_stream);
        let request = rusoto_s3::GetObjectRequest {
            bucket: String::from("commoncrawl"), // TODO: make this configurable
            key: url_to_stream,
            ..Default::default()
        };
        let result = match s3_client.get_object(request).sync() {
            Ok(value) => value,
            Err(err) => {
                error!(
                    "encountered issue while loading object (`{}`), skipping...",
                    err
                );
                continue;
            }
        };
        let stream = (match result.body {
            Some(value) => value,
            None => {
                error!("unable to get response body");
                continue;
            }
        })
        .into_blocking_read();
        let mut decoder = MultiGzDecoder::new(stream);
        let crlf = [13, 10, 13, 10]; // carraige return, line feed
        let mut current_document_batch: Vec<ieql::Document> = Vec::new();
        loop {
            // Check if queue size is too big
            let mut currently_processing = scan_interface.batches_pending_processing();
            let mut instances = 1;
            while queue_size <= currently_processing {
                warn!(
                    "maximum queue sized reached ({} >= {}); sleeping for 1s... (#{})",
                    currently_processing, queue_size, instances
                );
                instances += 1;
                thread::sleep(Duration::from_millis(1000));
                currently_processing = scan_interface.batches_pending_processing();
            }

            // On-the-fly gzip decode loop
            // good network buffer size: 30K
            let mut buf = [0u8; 32768];
            // also: be sure to keep this out of the loop; no need to re-allocate memory on the stack
            let mut data: Vec<u8> = Vec::new();
            loop {
                // buffer-level infile read
                let resp = decoder.read(&mut buf);
                let bytes_read = match resp {
                    Ok(value) => value,
                    Err(_) => {
                        error!("encountered issue while streaming...");
                        break;
                    }
                };
                data.extend_from_slice(&buf[0..bytes_read]);
                if data.ends_with(&crlf) || bytes_read == 0 {
                    break;
                }
            }
            if data.len() == 0 {
                // finished archive
                info!("finished archive!");
                break;
            }
            let record_result = warc_parser::record(data.as_slice());
            if !record_result.is_done() {
                debug!("finished read before finishing WARC!");
                continue;
            }
            if record_result.is_err() {
                error!("encountered issue while parsing, skipping...");
                continue;
            }
            let record: warc_parser::Record = record_result.unwrap().1;
            if record.headers.get("WARC-Type") != Some(&String::from("response")) {
                // info!(
                //     "WARC-Type was not response; it was {:?}",
                //     record.headers.get("WARC-Type")
                // );
                continue;
            }
            let document = match warc_to_document(record) {
                Ok(value) => value,
                Err(error) => {
                    error!("encountered issue while parsing (`{}`), skipping...", error);
                    continue;
                }
            };
            documents_processed += 1;

            // Send for scanning
            debug!("processing asynchronously: {:?}", document.url);
            current_document_batch.push(document);
            if current_document_batch.len() >= 64 {
                match scan_interface.process(docs_to_doc_reference(current_document_batch)) {
                    Ok(_) => (),
                    Err(_) => {
                        error!("unable to scan document batch!");
                    }
                }
                current_document_batch = Vec::new();
            }

            if documents_processed % update_interval == 0 {
                let old_outputs = total_outputs;
                let mut output_batch = OutputBatch {
                    outputs: Vec::new(),
                };
                for output in scan_interface.outputs() {
                    output_batch.merge_with(output);
                }
                let new_outputs = output_batch.outputs.len();
                total_outputs = old_outputs + new_outputs;
                let mut time_elapsed = SystemTime::now()
                    .duration_since(start_time)
                    .expect("time went backwards!")
                    .as_secs();
                if time_elapsed == 0 {
                    time_elapsed += 1; // for now...
                }
                let docs_per_second = documents_processed / time_elapsed;

                if new_outputs > 0 {
                    let output_url = format!("{}/output/", master_url);
                    match post_outputs(access_key.as_str(), output_url.as_str(), output_batch) {
                        Ok(num) => info!("successfully sent {} outputs to master server", num),
                        Err(issue) => {
                            error!("could not send outputs to master server: `{}`", issue)
                        }
                    }
                }

                info!(
                    "[{} queued] [{} docs/second] [{} processed] [{} outputs, Δ{}]",
                    scan_interface.batches_pending_processing(),
                    docs_per_second,
                    documents_processed,
                    total_outputs,
                    new_outputs
                );
            }
        }
        // Send remaining documents
        match scan_interface.process(docs_to_doc_reference(current_document_batch)) {
            Ok(_) => (),
            Err(_) => {
                error!("unable to scan document batch!");
            }
        }

        // Mark source as completed
        let completion_url = format!("{}/complete_source/{}", &master_url, &data_id);
        match get_authenticated(
            access_key.as_str(),
            completion_url.as_str(),
            RequestMethod::Post,
        ) {
            Ok(_) => info!("marked source id `{}` as completed", data_id),
            Err(_) => error!("unable to mark source id `{}` as completed", data_id),
        }
    }
}

fn docs_to_doc_reference(
    docs: Vec<ieql::Document>,
) -> ieql::input::document::DocumentReferenceBatch {
    let mut doc_references: Vec<ieql::input::document::DocumentReference> = Vec::new();
    for item in docs {
        doc_references.push(ieql::input::document::DocumentReference::Populated(item));
    }
    ieql::input::document::DocumentReferenceBatch::from(doc_references)
}

fn warc_to_document(record: warc_parser::Record) -> Result<ieql::Document, String> {
    let url = match record.headers.get("WARC-Target-URI") {
        Some(value) => Some(value.clone()),
        None => None,
    };
    // TODO: add mime support, parse headers
    Ok(ieql::Document {
        data: record.content,
        url: url,
        mime: Some(String::from("text/html")), // most likely; in any case, it's a safe bet.
    })
}
