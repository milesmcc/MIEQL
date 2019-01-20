use flate2::read::GzDecoder;
use futures::{Future, IntoFuture};
use ieql::query::query::QueryGroup;
use nom::{MemProducer, Producer};
use rusoto_s3;
use rusoto_s3::S3;
use std::io::Read;

pub fn main(master_url: String) {
    // Test connection
    let handshake_url = format!("{}/handshake", &master_url);
    let handshake_response = reqwest::get(handshake_url.as_str())
        .expect("unable to connect to master server")
        .text()
        .unwrap();

    match handshake_response.as_str() {
        "nice to meet you" => info!("successfully connected to server"),
        _ => {
            error!("unable to connect to server");
            return;
        }
    }

    // Create dataset client
    let s3_client = rusoto_s3::S3Client::new(rusoto_core::region::Region::UsEast1);
    info!("Launched S3 client...");

    // Get queries
    let queries_url = format!("{}/queries", &master_url);
    let queries_response = reqwest::get(queries_url.as_str())
        .expect("unable to retrieve queries")
        .text()
        .unwrap();
    let queries: QueryGroup =
        ron::de::from_str(queries_response.as_str()).expect("unable to read queries");
    info!(
        "successfully loaded {} queries from master",
        queries.queries.len()
    );

    // Stream and process an archive
    loop {
        let data_url = format!("{}/data", &master_url);
        let mut data_response: reqwest::Response =
            reqwest::get(data_url.as_str()).expect("unable to retrieve data url!");
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
        let mut stream = result.body.unwrap().into_blocking_read();
        let mut decoder = GzDecoder::new(stream);
        let mut consumer = warc_parser::WarcConsumer::new();
        let mut buffer = [0u8; 65536];
        let resp = decoder.read(&mut buffer);
        info!("{:?}", String::from_utf8_lossy(&buffer));
        break;
        // for _ in 0..1024 {
        //     decoder.read(&mut buffer);
        //     let mut producer = MemProducer::new(&buffer, 4096);
        //     producer.apply(&mut consumer);
        // }
        info!("{} records found in buffer", consumer.counter);
        info!("{:?}", consumer.last_record);
    }
}
