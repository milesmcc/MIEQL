use flate2::read::MultiGzDecoder;
use ieql::common::compilation::CompilableTo;
use ieql::output::output::OutputBatch;
use ieql::query::query::QueryGroup;
use ieql::scan::scanner::Scanner;
use rusoto_s3;
use rusoto_s3::S3;
use std::io::Read;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

pub fn main(master_url: String, threads: u8, queue_size: isize, update_interval: u64) {
    // Test connection
    let handshake_url = format!("{}/handshake", &master_url);
    let handshake_response = (match reqwest::get(handshake_url.as_str()) {
        Ok(value) => value,
        Err(error) => {
            error!("unable to connect to master server: `{}`", error);
            std::process::exit(101);
        }
    })
    .text()
    .unwrap();

    match handshake_response.as_str() {
        "nice to meet you" => info!("successfully connected to server"),
        _ => {
            error!("unable to connect to server");
            std::process::exit(101);
        }
    }

    // Create dataset client
    let s3_client = rusoto_s3::S3Client::new(rusoto_core::region::Region::UsEast1);

    // Get queries
    let queries_url = format!("{}/queries", &master_url);
    let queries_response = (match reqwest::get(queries_url.as_str()) {
        Ok(value) => value,
        Err(error) => {
            error!("unable to get queries: `{}`", error);
            std::process::exit(101);
        }
    })
    .text()
    .unwrap();
    let queries: QueryGroup = match ron::de::from_str(queries_response.as_str()) {
        Ok(value) => value,
        Err(error) => {
            error!("unable to deserialize queries: `{}`", error);
            std::process::exit(101);
        }
    };
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
    let scan_interface = compiled_queries.scan_concurrently(threads);

    // Analytics
    let mut documents_processed = 0u64;
    let mut total_outputs = 0;
    let start_time = SystemTime::now();

    // Reqwest client
    let client = reqwest::Client::new();

    // Stream and process an archive
    loop {
        // Stream loop
        let data_url = format!("{}/data", &master_url);
        let mut data_response: reqwest::Response = match reqwest::get(data_url.as_str()) {
            Ok(value) => value,
            Err(error) => {
                error!("unable to get data location from master: `{}`", error);
                std::process::exit(101);
            }
        };
        if data_response.status().as_u16() == 204 {
            info!("no more data left!");
            info!("shutting down...");
            break;
        }
        let url_to_stream = data_response.text().unwrap();
        info!("found data `{}` to process", url_to_stream);
        let request = rusoto_s3::GetObjectRequest {
            bucket: String::from("commoncrawl"),
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
                    match client
                        .post(format!("{}/output", &master_url).as_str())
                        .body(ron::ser::to_string(&output_batch).unwrap())
                        .send()
                    {
                        Ok(_) => (),
                        Err(error) => warn!(
                            "encountered error while pushing outputs to master: `{}`",
                            error
                        ),
                    };
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
