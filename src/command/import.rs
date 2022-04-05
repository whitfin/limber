//! Import command module for Limber.
//!
//! This module exposes functions to import a set of documents to a target
//! Elasticsearch cluster/index. Input is received from `stdin` as it enables
//! easy chaining against other tools (such as those for compression).
//!
//! This interface also allows chaining from another instance of Limber, to
//! enable piping from one cluster/index to another in a streaming fashion.
use anyhow::Result;
use bytelines::*;
use clap::{Arg, ArgMatches, Command};
use elasticsearch::indices::IndicesRefreshParts;
use elasticsearch::{BulkOperation, BulkParts};
use futures::stream::StreamExt;
use serde_json::Value;
use tokio::io::{self, BufReader};

use std::sync::Arc;

use crate::remote;
use crate::stats::Counter;

/// Returns the definition for this command in the CLI.
///
/// This function dictates options available to this command and what
/// can be asserted to exist, as well as the other optional arguments.
pub fn cmd<'a>() -> Command<'a> {
    Command::new("import")
        .about("Import documents to an Elasticsearch cluster")
        .args(&[
            // concurrency: c [1]
            Arg::new("concurrency")
                .help("A concurrency weighting to tune throughput")
                .short('c')
                .long("concurrency")
                .takes_value(true)
                .default_value("1")
                .hide_default_value(true),
            // size: s, size [100]
            Arg::new("size")
                .help("The amount of documents to index per request")
                .short('s')
                .long("size")
                .takes_value(true)
                .default_value("100")
                .hide_default_value(true),
            // target: +required
            Arg::new("target")
                .help("Target host to import documents to")
                .required(true),
        ])
}

/// Constructs a `Future` to execute the `import` command.
///
/// This future should be spawned on a Runtime to carry out the importing process.
pub async fn run(args: &ArgMatches) -> Result<()> {
    // fetch the configured batch size, or default to 100
    let size = args.value_of_t::<usize>("size").unwrap_or(100);

    // fetch the target from the arguments, should always be possible
    let target = args.value_of("target").expect("guaranteed by CLI");

    // fetch the concurrency factor to use for export, default to single worker
    let concurrency = args.value_of_t::<usize>("concurrency").unwrap_or(1);

    // parse arguments into a host/index pairing for later
    let (host, index) = remote::parse_cluster(target)?;
    let client = Arc::new(remote::create_client(&host)?);

    // create a counter to track docs
    let counter = Counter::shared(0);

    // fetch stdin as lines
    let stdin = BufReader::new(io::stdin());
    let lines = AsyncByteLines::new(stdin);

    // start streaming the lines and map into bulk operations
    let filter = lines.into_stream().filter_map(|input| async {
        // parsed the bytes into a `Value` so we can fetch JSON data back from it
        let mut parsed = serde_json::from_slice::<Value>(&input.ok()?).ok()?;

        // shim the index to the doc index
        let index = match index {
            Some(ref index) => index.to_owned(),
            None => parsed.get("_index")?.as_str()?.to_owned(),
        };

        Some(
            // create our bulk request using the source
            BulkOperation::index(parsed["_source"].take())
                .id(parsed.get("_id")?.as_str()?.to_owned())
                .index(index)
                .into(),
        )
    });

    // chunk the stream into batches
    let chunk = filter.chunks(size);

    // handle each batch concurrently and send each buffer to Elasticsearch directly
    let worker = chunk.for_each_concurrent(concurrency, |batch: Vec<BulkOperation<_>>| {
        async {
            // grab counter for later
            let total = batch.len();

            // index the batch
            let response = client
                .bulk(BulkParts::None)
                .body(batch)
                .send()
                .await
                .expect("unable to import batch")
                .error_for_status_code()
                .expect("unable to import batch");

            // increment the counter and print the state to stderr
            eprintln!(
                "Indexed another batch, have now processed {}",
                counter.increment(total)
            );

            // turn the body back into an array of items to work with
            let body = response.json::<Value>().await.unwrap();

            // skip out if none of the requests returned an error
            if !body.get("errors").unwrap().as_bool().unwrap_or(false) {
                return;
            }

            // iterate through all items which came back in the response
            for item in body.get("items").unwrap().as_array().unwrap() {
                // fetch the failed shard counter to check errors
                let failed = item.pointer("/index/_shards/failed");

                // log errors if any happened (based on shards)
                if failed.unwrap().as_u64().unwrap_or(1) > 0 {
                    eprintln!("err: {:?}", item);
                }
            }
        }
    });

    // await all!
    worker.await;

    // execute a refresh against the cluster
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&["_all"]))
        .send()
        .await?
        .error_for_status_code()?;

    // done!
    Ok(())
}
