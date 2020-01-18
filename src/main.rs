extern crate ieql;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate futures;
extern crate hyper;
extern crate ron;
extern crate reqwest;
extern crate rusoto_s3;
extern crate flate2;
extern crate warc_parser;
extern crate nom;
extern crate httparse;
extern crate env_logger;
extern crate sys_info;
extern crate serde_json;
extern crate itertools;

use clap::{App};

mod client;
mod util;

fn main() {
    env_logger::init();

    let matches = App::new("MIEQL Command Line Interface")
        .version(crate_version!())
        .about("IEQL client (S3 and container bindings for IEQL)")
        .author(crate_authors!())
                .args_from_usage("-t, --threads=[# of threads] 'The number of threads to use (default 8)'")
                .args_from_usage("-m, --master=[master url] 'The url of the master (default <http://localhost:8000/mieql>)'")
                .args_from_usage("-s, --secret-key=<secret key> 'The server group secret key for the master server'")
                .args_from_usage("-q, --queue=[max queue size] 'Maximum number of items in the queue at any given time (default 256)'")
                .args_from_usage("-u, --update-interval=[update frequency] 'How frequently to log a status update, in terms of documents (default 512)")
        .get_matches();
    run(matches);
}

fn run(m: clap::ArgMatches) {
    let master_url = m.value_of("master").unwrap_or("http://localhost:8000/mieql");
    let secret_key = m.value_of("secret-key").expect("The secret key is required for client operation!");
    let threads: u8 = match m.value_of("threads").unwrap_or("8").parse() {
        Ok(value) => value,
        Err(error) => {
            error!("invalid number of threads `{}` (`{}`)!", m.value_of("threads").unwrap(), error);
            std::process::exit(101);
        }
    };
    let queue_size: isize = match m.value_of("queue").unwrap_or("256").parse() {
        Ok(value) => value,
        Err(error) => {
            error!("invalid max queue size `{}` (`{}`)!", m.value_of("queue").unwrap(), error);
            std::process::exit(101);
        }
    };
    let update_interval: u64 = match m.value_of("update-interval").unwrap_or("512").parse() {
        Ok(value) => value,
        Err(error) => {
            error!("invalid update interval `{}` (`{}`)!", m.value_of("update-interval").unwrap(), error);
            std::process::exit(101);
        }
    };
    client::main(String::from(master_url), String::from(secret_key), threads, queue_size, update_interval);
}
