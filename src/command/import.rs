//! Import command module for Limber.
//!
//! This module exposes functions to import a set of documents to a target
//! Elasticsearch cluster/index. Input is received from `stdin` as it enables
//! easy chaining against other tools (such as those for compression).
//!
//! This interface also allows chaining from another instance of Limber, to
//! enable piping from one cluster/index to another in a streaming fashion.
use bytelines::*;
use clap::{value_t, App, Arg, ArgMatches, ArgSettings, SubCommand};
use elastic::client::requests::bulk::*;
use futures::future::{self, Either, Loop};
use futures::prelude::*;
use futures::sync::mpsc;
use serde_json::Value;

use std::io::{self, BufReader};

use crate::errors::{self, Error};
use crate::ft_err;
use crate::remote;
use crate::stats::Counter;

/// Returns the definition for this command in the CLI.
///
/// This function dictates options available to this command and what
/// can be asserted to exist, as well as the other optional arguments.
pub fn cmd<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("import")
        .about("Import documents to an Elasticsearch cluster")
        .args(&[
            // concurrency: -c [1]
            Arg::with_name("concurrency")
                .help("Concurrency weighting to tune throughput")
                .short("c")
                .long("concurrency")
                .takes_value(true)
                .default_value("1")
                .set(ArgSettings::HideDefaultValue),
            // size: -s, --size [100]
            Arg::with_name("size")
                .help("Batch sizes to use")
                .short("s")
                .long("size")
                .takes_value(true)
                .default_value("100")
                .set(ArgSettings::HideDefaultValue),
            // target: +required
            Arg::with_name("target")
                .help("Target to import documents to")
                .required(true),
        ])
}

/// Constructs a `Future` to execute the `import` command.
///
/// This future should be spawned on a Runtime to carry out the importing process.
pub fn run(args: &ArgMatches) -> Box<Future<Item = (), Error = Error>> {
    // fetch the configured batch size, or default to 100
    let size = value_t!(args, "size", usize).unwrap_or(100);

    // fetch the target from the arguments, should always be possible
    let target = args.value_of("target").expect("guaranteed by CLI");

    // fetch the configured concurrency factor to use when importing events
    let concurrency = value_t!(args, "concurrency", usize).unwrap_or_else(|_| 1);

    // parse arguments into a host/index pairing for later
    let (host, index) = ft_err!(remote::parse_cluster(target));

    // construct a single client instance for all tasks
    let client = ft_err!(remote::create_client(host));

    // construct a channel with bounded concurrency
    let (tx, rx) = mpsc::channel(size * concurrency);

    // Spawn a new thread to forward stdin through to our worker
    //
    // This is necessary because Tokio's FS support requires a multithreaded
    // Runtime due to `stdin` streaming being blocking. We can't (yet) use a
    // Runtime off of the current thread due to limitations in the Elastic
    // library - so we just use synchronous IO on another thread and pass on.
    std::thread::spawn(move || {
        // fetch stdin as lines
        let stdin = io::stdin();
        let stdin = stdin.lock();
        let stdin = BufReader::new(stdin);
        let stdin = stdin.byte_lines().into_iter();

        // create a future to iterate all lines of stdin and pass them to the channel
        let lines = future::loop_fn((tx, stdin), |(tx, mut lines)| match lines.next() {
            Some(line) => {
                let forward = tx
                    .send(line.unwrap())
                    .and_then(|tx| Ok(Loop::Continue((tx, lines))));
                Either::B(forward)
            }
            None => {
                let ctx = (tx, lines);
                let brk = Loop::Break(ctx);
                let okr = future::ok(brk);
                Either::A(okr)
            }
        });

        // block on the loop, shouldn't really error?
        lines.wait().expect("unable to forward stdin");
    });

    // create a counter to track docs
    let counter = Counter::shared(0);

    // iterate all lines
    let worker = rx
        // parse all line input
        .filter_map(move |line| {
            // parsed the bytes into a `Value` so we can fetch JSON data back from it
            let mut parsed = serde_json::from_slice::<Value>(&line).ok()?;

            // shim the index to the doc index
            let index = match index {
                Some(ref index) => index.to_owned(),
                None => parsed.get("_index")?.as_str()?.to_owned(),
            };

            // fetch the values of the document _id and _type
            let id = parsed.get("_id")?.as_str()?.to_owned();
            let ty = parsed.get("_type")?.as_str()?.to_owned();

            // create a request
            let req = bulk_raw()
                .index(parsed["_source"].take())
                .index(index)
                .id(id)
                .ty(ty);

            // pass back
            Some(req)
        })
        .chunks(size)
        .map_err(|_| unreachable!())
        .for_each(move |operations| {
            let counter = counter.clone();

            client
                .bulk()
                .extend(operations)
                .send()
                .map_err(errors::raw)
                .and_then(move |response| {
                    // increment the counter and print the state to stderr
                    eprintln!(
                        "Indexed another batch, have now processed {}",
                        counter.increment(size)
                    );

                    // skip if we're done
                    if response.is_ok() {
                        return Ok(());
                    }

                    // log errors if any happened
                    for item in response.iter() {
                        if item.is_err() {
                            eprintln!("err: {:?}", item);
                        }
                    }

                    // kill the future to bail out early (for now)
                    Err(errors::raw("Received errors in bulk import"))
                })
        });

    // ignore the actual output and coerce errors
    Box::new(worker.map_err(errors::raw).map(|_| ()))
}
