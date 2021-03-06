use flate2::read::MultiGzDecoder;
use ieql::common::compilation::CompilableTo;
use ieql::output::output::OutputBatch;
use ieql::query::query::{CompiledQueryGroup, Query, QueryGroup};
use ieql::scan::scanner::{AsyncScanInterface, Scanner};
use ieql::ScopeContent;
use itertools::Itertools;
use rusoto_s3;
use rusoto_s3::S3;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

const DOCUMENT_BATCH_SIZE: usize = 64;

fn max_queue_size(scan_interfaces: &Vec<AsyncScanInterface>) -> isize {
    scan_interfaces
        .iter()
        .map(|x| x.batches_pending_processing())
        .collect::<Vec<isize>>()
        .into_iter()
        .fold(0, |acc, b| acc.max(b))
}

fn push_new_outputs(
    access_key: &String,
    output_url: &String,
    scan_interfaces: &mut Vec<AsyncScanInterface>,
) -> usize {
    let mut output_batch = OutputBatch {
        outputs: Vec::new(),
    };
    for scan_interface in scan_interfaces {
        for output in scan_interface.outputs() {
            output_batch.merge_with(output);
        }
    }

    let total_outputs = output_batch.outputs.len();

    if output_batch.outputs.len() > 0 {
        match post_outputs(access_key.as_str(), output_url.as_str(), output_batch) {
            Ok(num) => info!("successfully sent {} outputs to master server", num),
            Err(issue) => error!("could not send outputs to master server: `{}`", issue),
        };
    }

    return total_outputs;
}

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
    'primary: loop {
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

        let output_url = format!("{}/output/", master_url);

        let registration_data: Value =
            serde_json::from_str(registration_response.as_str()).unwrap();

        let access_key: String = registration_data["data"]["access_key"].to_string();

        info!(
            "successfully established access key with master: {}",
            access_key
        );
        // Create dataset client
        let s3_client = rusoto_s3::S3Client::new(rusoto_core::region::Region::UsEast1);

        // Stream and process an archive
        loop {
            // Stream loop

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
                let mut query: Query = match ron::de::from_str(query_val["ieql"].as_str().unwrap())
                {
                    Ok(parsed_query) => parsed_query,
                    Err(_) => {
                        error!("unable to parse query; dying...");
                        std::process::exit(101);
                    }
                };
                query.id = Some(id);
                query_vec.push(query);
            }

            let mut query_groups: HashMap<ScopeContent, QueryGroup> = HashMap::new();

            info!(
                "successfully loaded {} queries from master",
                query_vec.len()
            );

            for query in query_vec {
                match query_groups.get_mut(&query.scope.content) {
                    Some(query_group) => {
                        query_group.queries.push(query);
                    }
                    None => {
                        query_groups.insert(
                            query.scope.content,
                            QueryGroup {
                                optimized_content: query.scope.content,
                                queries: vec![query],
                            },
                        );
                    }
                }
            }

            let compiled_query_groups: Vec<CompiledQueryGroup> = query_groups
                .values()
                .map(|query_group| match query_group.compile() {
                    Ok(value) => value,
                    Err(error) => {
                        error!("unable to compile queries: {}", error);
                        std::process::exit(101);
                    }
                })
                .collect();

            // Create scan engines
            let threads_per_group: u8 = threads / (compiled_query_groups.len() as u8);
            let mut scan_interfaces: Vec<AsyncScanInterface> = compiled_query_groups
                .into_iter()
                .map(|group| group.scan_concurrently(threads_per_group))
                .collect();
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
                        continue 'primary;
                    }
                },
                Err(error) => {
                    error!("unable to get data location from master: `{}`", error);
                    std::process::exit(101);
                }
            };
            // Reset stats
            let mut documents_processed = 0u64;
            let mut total_outputs = 0;
            let start_time = SystemTime::now();

            info!("found data `{}` to process", url_to_stream);
            let paths: Vec<&str> = url_to_stream.split("/").collect();
            let request = rusoto_s3::GetObjectRequest {
                bucket: String::from(*paths.get(0).unwrap_or(&"commoncrawl")),
                key: String::from(paths.iter().skip(1).join("/")),
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
                let mut instances = 1;

                // Check if any queue size is too big
                let mut currently_processing = max_queue_size(&scan_interfaces);
                while queue_size <= currently_processing {
                    warn!(
                        "maximum queue sized reached ({} >= {}); sleeping for 5s... (#{})",
                        currently_processing, queue_size, instances
                    );
                    instances += 1;
                    thread::sleep(Duration::from_millis(5000));
                    currently_processing = max_queue_size(&scan_interfaces);
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
                current_document_batch.push(document);
                if current_document_batch.len() >= DOCUMENT_BATCH_SIZE {
                    for scan_interface in &scan_interfaces {
                        match scan_interface
                            .process(docs_to_doc_reference(current_document_batch.to_vec()))
                        {
                            Ok(_) => (),
                            Err(_) => {
                                error!("unable to scan document batch!");
                            }
                        }
                    }
                    current_document_batch = Vec::new();
                }

                if documents_processed % update_interval == 0 {
                    let old_outputs = total_outputs;
                    let new_outputs =
                        push_new_outputs(&access_key, &output_url, &mut scan_interfaces);
                    let documents_queued = max_queue_size(&scan_interfaces) * DOCUMENT_BATCH_SIZE as isize;
                    let documents_completed = documents_processed
                        - documents_queued as u64;
                    total_outputs = old_outputs + new_outputs;
                    let mut time_elapsed = SystemTime::now()
                        .duration_since(start_time)
                        .expect("time went backwards!")
                        .as_secs();
                    if time_elapsed == 0 {
                        time_elapsed += 1; // for now...
                    }
                    let docs_per_second = documents_completed / time_elapsed;
                    info!(
                        "[{} docs queued] [{} docs/second] [{} docs done] [{} outputs, Δ{}]",
                        documents_queued,
                        docs_per_second,
                        documents_completed,
                        total_outputs,
                        new_outputs
                    );
                }
            }
            // Send remaining documents
            for scan_interface in &scan_interfaces {
                match scan_interface.process(docs_to_doc_reference(current_document_batch.to_vec()))
                {
                    Ok(_) => (),
                    Err(_) => {
                        error!("unable to scan document batch!");
                    }
                }
            }

            info!("finished archive; waiting for final documents to be processed...");
            let mut waiting = 0;
            while max_queue_size(&scan_interfaces) > 0 {
                if waiting >= 300 {
                    info!("graceful cleanup is taking too long, forcing end...");
                    break;
                }
                info!(
                    "{} items left in queue; waiting...",
                    max_queue_size(&scan_interfaces)
                );
                waiting += 1;
                thread::sleep(Duration::from_millis(1000));
            }

            info!("cleaning up...");
            thread::sleep(Duration::from_millis(5000));
            push_new_outputs(&access_key, &output_url, &mut scan_interfaces);

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
