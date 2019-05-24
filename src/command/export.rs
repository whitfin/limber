//! Export command module for Limber.
//!
//! This module exposes functions to export an Elasticsearch target index to
//! `stdio`. This allows the caller to pipe into any compression algorithms
//! they may wish to use, and store in any container they might wish to use.
//!
//! This interface also allows chaining into another instance of Limber, to
//! enable piping from one cluster/index to another in a streaming fashion.
use clap::{value_t, App, Arg, ArgMatches, ArgSettings, SubCommand};
use elastic::client::requests::{ScrollRequest, SearchRequest};
use elastic::prelude::*;
use futures::future::{self, Either, Loop};
use futures::prelude::*;
use serde_json::{json, Value};

use crate::errors::{self, Error};
use crate::ft_err;
use crate::remote;
use crate::stats::Counter;

/// Returns the definition for this command in the CLI.
///
/// This function dictates options available to this command and what
/// can be asserted to exist, as well as the other optional arguments.
pub fn cmd<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("export")
        .about("Export documents from an Elasticsearch cluster")
        .args(&[
            // concurrency: -c [1]
            Arg::with_name("concurrency")
                .help("Concurrency weighting to tune throughput")
                .short("c")
                .long("concurrency")
                .takes_value(true)
                .default_value("1")
                .set(ArgSettings::HideDefaultValue),
            // size: -q, --query [{}]
            Arg::with_name("query")
                .help("Query to use to filter exported documents")
                .short("q")
                .long("query")
                .takes_value(true)
                .default_value("{}")
                .set(ArgSettings::HideDefaultValue),
            // size: -s, --size [100]
            Arg::with_name("size")
                .help("Batch sizes to use")
                .short("s")
                .long("size")
                .takes_value(true)
                .default_value("100")
                .set(ArgSettings::HideDefaultValue),
            // source: +required
            Arg::with_name("source")
                .help("Source to export documents from")
                .required(true),
        ])
}

/// Constructs a `Future` to execute the `export` command.
///
/// This future should be spawned on a Runtime to carry out the exporting
/// process. The returned future will be a combination of several futures
/// to represent the concurrency flags provided via the CLI arguments.
pub fn run(args: &ArgMatches) -> Box<Future<Item = (), Error = Error>> {
    // fetch the source from the arguments, should always be possible
    let source = args.value_of("source").expect("guaranteed by CLI");

    // fetch the concurrency factor to use for export, default to single worker
    let concurrency = value_t!(args, "concurrency", usize).unwrap_or_else(|_| 1);

    // parse arguments into a host/index pairing for later
    let (host, index) = ft_err!(remote::parse_cluster(&source));

    // shim the index value
    let index = match index {
        Some(ref idx) => idx,
        None => "_all",
    };

    // construct a single client instance for all tasks
    let client = ft_err!(remote::create_client(host));

    // create counter to track docs
    let counter = Counter::shared(0);

    // create vec to store worker task futures
    let mut tasks = Vec::with_capacity(concurrency);

    // construct worker task
    for idx in 0..concurrency {
        // take ownership of stuff
        let index = index.to_owned();
        let client = client.to_owned();
        let counter = counter.to_owned();

        // create our initial search request to trigger scrolling
        let query = ft_err!(construct_query(&args, idx, concurrency));
        let request = SearchRequest::for_index(index, query);

        let execute = client
            .request(request)
            .params_fluent(|p| p.url_param("scroll", "1m"))
            .send()
            .and_then(AsyncResponseBuilder::into_response)
            .and_then(|value: Value| {
                future::loop_fn((counter, client, value), |(counter, client, mut value)| {
                    // fetch the hits back
                    let hits = value
                        .pointer_mut("/hits/hits")
                        .expect("unable to locate hits")
                        .as_array_mut()
                        .expect("hits are of wrong type");

                    // empty hits means we're done
                    if hits.is_empty() {
                        let ctx = (counter, client, value);
                        let brk = Loop::Break(ctx);
                        let okr = future::ok(brk);
                        return Either::B(okr);
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

                    // construct the request for the next batch
                    let request = ScrollRequest::for_scroll_id(
                        scroll_id,
                        json!({
                            "scroll": "1m"
                        }),
                    );

                    // loop on the next batch
                    Either::A(
                        client
                            .request(request)
                            .send()
                            .and_then(AsyncResponseBuilder::into_response)
                            .and_then(|value: Value| Ok(Loop::Continue((counter, client, value)))),
                    )
                })
            });

        // push the worker
        tasks.push(execute);
    }

    // join all tasks, ignoring the actual output and coercing errors
    Box::new(future::join_all(tasks).map_err(errors::raw).map(|_| ()))
}

/// Constructs a query instance based on the worker count and identifier.
///
/// This can technically fail if the query provided is invalid, which is why
/// the return type is a `Result`. This is the safest option, as the user will
/// expect their results to be correctly filtered.
///
/// An error in this case should halt all progress by the main export process.
fn construct_query(args: &ArgMatches, id: usize, max: usize) -> Result<Value, Error> {
    // fetch the configured batch size, or default to 100
    let size = value_t!(args, "size", usize).unwrap_or(100);

    // fetch the query filter to use to limit matches (defaults to all docs)
    let filter = args.value_of("query").unwrap_or("{\"match_all\":{}}");
    let filter = serde_json::from_str::<Value>(filter)?;

    // construct query
    let mut query = json!({
        "query": filter,
        "size": size,
        "sort": [
            "_doc"
        ]
    });

    // handle multiple workers...
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
