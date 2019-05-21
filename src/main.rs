//! A simple (but quick) tool for backing up Elasticsearch documents.
//!
//! It's designed for efficient import/export of Elasticsearch indices,
//! rather than complex use cases. If you want anything more than simple
//! backup/restore, you probably want to look somewhere else (or extend
//! this tool as necessary).
//!
//! Limber is only built as a command line tool, as it's simply a small
//! CLI binding around the fairly low-level Elasticsearch library APIs.
#![doc(html_root_url = "https://docs.rs/limber/1.0.0")]
use clap::{App, AppSettings, Arg, ArgSettings, SubCommand};
use failure::Error;
use tokio::runtime::current_thread::Runtime;

mod cmd;
use cmd::*;

fn main() -> Result<(), Error> {
    // We're mostly IO bound, so we just use a Runtime on the current
    // thread. This lets us be a little more flexible in our Futures
    // as they don't have to implement `Send`, which isn't always easy.
    let mut rt = Runtime::new().unwrap();

    // Delegate to the subcommand, or print the help menu and exit. Each
    // subcommand can be spawned and blocked on via the Runtime to allow
    // for asynchronous execution for better throughput/performance.
    match build_cli().get_matches().subcommand() {
        ("export", Some(args)) => rt.block_on(export::run(args)),
        _ => build_cli().print_help().map_err(Into::into),
    }
}

/// Creates a parser used to generate `Options`.
///
/// All command line usage information can be found in the definitions
/// below, and follows the API of the `clap` library.
///
/// In terms of visibility, this method is defined on the struct due to
/// the parser being specifically designed around the `Options` struct.
fn build_cli<'a, 'b>() -> App<'a, 'b> {
    App::new("")
        // package metadata from cargo
        .name(env!("CARGO_PKG_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        // export subcommand
        .subcommand(
            SubCommand::with_name("export")
                .about("Export documents from an Elasticsearch cluster")
                .args(&[
                    // compression: -c, --compress
                    Arg::with_name("compression")
                        .help("Whether to compress documents using GZIP")
                        .short("c")
                        .long("compress")
                        .takes_value(false)
                        .set(ArgSettings::HideDefaultValue),
                    // size: -q, --query [{}]
                    Arg::with_name("query")
                        .help("Query to use to filter exported documents")
                        .short("q")
                        .long("query")
                        .takes_value(true)
                        .set(ArgSettings::HideDefaultValue),
                    // size: -s, --size [100]
                    Arg::with_name("size")
                        .help("Batch sizes to use")
                        .short("s")
                        .long("size")
                        .takes_value(true)
                        .set(ArgSettings::HideDefaultValue),
                    // workers: -w [num_cpus::get()]
                    Arg::with_name("workers")
                        .help("Number of worker threads to use")
                        .short("w")
                        .long("workers")
                        .takes_value(true)
                        .set(ArgSettings::HideDefaultValue),
                    // source: +required
                    Arg::with_name("source")
                        .help("Source to export documents from")
                        .required(true),
                ]),
        )
        // settings required for parsing
        .settings(&[
            AppSettings::ArgRequiredElseHelp,
            AppSettings::HidePossibleValuesInHelp,
        ])
}
