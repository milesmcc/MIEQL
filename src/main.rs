extern crate ieql;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate simplelog;
extern crate hyper;
extern crate ron;

use clap::{App, Arg, SubCommand};

use ieql::query::query::{Query, QueryGroup};
use std::path::Path;

mod master;
mod client;

fn main() {
    simplelog::CombinedLogger::init(vec![simplelog::TermLogger::new(
        simplelog::LevelFilter::Info,
        simplelog::Config::default(),
    )
    .unwrap()])
    .unwrap();

    let matches = App::new("MIEQL Command Line Interface")
        .version(crate_version!())
        .about("S3 and container bindings for IEQL.")
        .author(crate_authors!())
        .subcommand(
            SubCommand::with_name("client")
                .about("Act as the client")
                .args_from_usage("-m, --master=<master url> 'The url of the master'")
        )
        .subcommand(
            SubCommand::with_name("master")
                .about("Act as the master")
        )
        .get_matches();
    run(matches);
}

fn run(matches: clap::ArgMatches) {
    match matches.subcommand() {
        ("client", Some(m)) => {
            let master_url = m.value_of("master").expect("master URL is required");
            client::main(master_url);
        },
        ("master", Some(m)) => {
            master::main(([0,0,0,0], 3000).into());
        },
        _ => error!("no valid command specified; try running with `--help`."),
    }
}