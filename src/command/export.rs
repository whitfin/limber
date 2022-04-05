//! Export command module for Limber.
//!
//! This module exposes functions to export an Elasticsearch target index to
//! `stdio`. This allows the caller to pipe into any compression algorithms
//! they may wish to use, and store in any container they might wish to use.
//!
//! This interface also allows chaining into another instance of Limber, to
//! enable piping from one cluster/index to another in a streaming fashion.
use anyhow::Result;
use clap::{Arg, ArgMatches, Command};
use elasticsearch::{Elasticsearch, ScrollParts, SearchParts};
use futures::prelude::*;
use serde_json::{json, Value};

use std::sync::Arc;

use crate::remote;
use crate::stats::Counter;

/// Returns the definition for this command in the CLI.
///
/// This function dictates options available to this command and what
/// can be asserted to exist, as well as the other optional arguments.
pub fn cmd<'a>() -> Command<'a> {
    Command::new("export")
        .about("Export documents from an Elasticsearch cluster")
        .args(&[
            // concurrency: -c [1]
            Arg::new("concurrency")
                .help("A concurrency weighting to tune throughput")
                .short('c')
                .long("concurrency")
                .takes_value(true)
                .default_value("1")
                .hide_default_value(true),
            // size: -q, --query [{}]
            Arg::new("query")
                .help("A query to use to filter exported documents")
                .short('q')
                .long("query")
                .takes_value(true)
                .default_value("{\"match_all\":{}}")
                .hide_default_value(true),
            // size: -s, --size [100]
            Arg::new("size")
                .help("The amount of documents to pull per request")
                .short('s')
                .long("size")
                .takes_value(true)
                .default_value("100")
                .hide_default_value(true),
            // source: +required
            Arg::new("source")
                .help("Source host to export documents from")
                .required(true),
        ])
}

/// Constructs a `Future` to execute the `export` command.
///
/// This future should be spawned on a Runtime to carry out the exporting
/// process. The returned future will be a combination of several futures
/// to represent the concurrency flags provided via the CLI arguments.
pub async fn run(args: &ArgMatches) -> Result<()> {
    // fetch the source from the arguments, should always be possible
    let source = args.value_of("source").expect("guaranteed by CLI");

    // fetch the concurrency factor to use for export, default to single handle
    let concurrency = args.value_of_t::<usize>("concurrency").unwrap_or(1);

    // parse arguments into a host/index pairing for later
    let (host, index) = remote::parse_cluster(source)?;

    // shim the index value when needed by defaulting to all
    let index = index.unwrap_or_else(|| "_all".to_string());

    // construct a single client instance for all tasks
    let client = Arc::new(remote::create_client(&host)?);

    // create iterable state
    let counter = Counter::shared(0);
    let mut tasks = Vec::with_capacity(concurrency);

    // create all worker tasks
    for idx in 0..concurrency {
        // take ownership of stuff
        let index = index.to_owned();
        let client = client.to_owned();
        let counter = counter.to_owned();

        // spawn a new worker task for idx
        let handle = tokio::spawn(scroll(
            client,
            counter,
            index,
            construct_query(args, idx, concurrency)?,
        ));

        // push the handle
        tasks.push(handle);
    }

    // attempt to join all task handles
    future::try_join_all(tasks).await?;

    // complete!
    Ok(())
}

/// Executes an async scroll against a given index set using a provided query.
///
/// This is separated out from the main loop so it can be spawned multiple times on a Tokio
/// worker pool to allow for easy concurrency control, instead of the (previous) single thread.
async fn scroll(client: Arc<Elasticsearch>, counter: Arc<Counter>, index: String, query: Value) {
    // scroll params
    let scroll = "1m";

    // initialize the search request
    let mut response = client
        .search(SearchParts::Index(&[&index]))
        .scroll(scroll)
        .body(query.clone())
        .send()
        .await
        .expect("unable to initialize search");

    loop {
        // parse the response body
        let mut body = response
            .json::<Value>()
            .await
            .expect("unable to parse scroll page");

        // fetch the value
        let value = body
            .pointer_mut("/hits/hits")
            .expect("unable to locate hits");

        // turn the hits back into an array
        let hits = value.as_array_mut().unwrap();

        // empty hits means we're done
        if hits.is_empty() {
            break;
        }

        // store hit length
        let length = hits.len();

        // iterate docs
        for hit in hits {
            // grab a mutable reference to the document
            let container = hit.as_object_mut().unwrap();

            // drop some query based fields
            container.remove("sort");
            container.remove("_score");

            // drop it to stdout
            println!("{}", hit);
        }

        // increment the counter and print the state to stderr
        eprintln!(
            "Fetched another batch, have now processed {}",
            counter.increment(length)
        );

        // fetch the new scroll_id
        let scroll_id = value
            .get("_scroll_id")
            .expect("unable to locate scroll_id")
            .as_str()
            .expect("scroll_id is of wrong type")
            .to_owned();

        // fetch next page
        response = client
            .scroll(ScrollParts::None)
            .body(json!({
                "scroll": scroll,
                "scroll_id": scroll_id
            }))
            .send()
            .await
            .expect("unable to continue search");
    }
}

/// Constructs a query instance based on the handle count and identifier.
///
/// This can technically fail if the query provided is invalid, which is why
/// the return type is a `Result`. This is the safest option, as the user will
/// expect their results to be correctly filtered.
///
/// An error in this case should halt all progress by the main export process.
fn construct_query(args: &ArgMatches, id: usize, max: usize) -> Result<Value> {
    // fetch the configured batch size, or default to 100
    let size = args.value_of_t::<usize>("size").unwrap_or(100);

    // fetch the query filter to use to limit matches (defaults to all docs)
    let filter = args.value_of("query").unwrap();
    let filter = serde_json::from_str::<Value>(filter)?;

    // construct query
    let mut query = json!({
        "query": filter,
        "size": size,
        "sort": [
            "_doc"
        ]
    });

    // handle multiple handles...
    if max > 1 {
        // ... by adding the slice identifier
        query.as_object_mut().unwrap().insert(
            "slice".to_owned(),
            json!({
                "id": id,
                "max": max
            }),
        );
    }

    // pass back!
    Ok(query)
}
