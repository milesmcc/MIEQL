use ieql::query::query::QueryGroup;

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
        let mut data_response: reqwest::Response = reqwest::get(data_url.as_str())
            .expect("unable to retrieve data url!");
        if data_response.status().as_u16() == 204 {
            info!("no more data left!");
            info!("shutting down...");
            break;
        }
        let url_to_stream = data_response.text().unwrap();
        info!(
            "found data `{}` to process",
            url_to_stream
        );
    }
}
