//! Utility functions for dealing with remote locations.
//!
//! This module offers functions for interacting with a remote cluster,
//! such as hostname parsing, client creation, etc.
use elastic::client::sender::NodeAddress;
use elastic::client::AsyncClientBuilder;
use elastic::prelude::*;
use failure::{format_err, Error};
use url::Url;

use std::sync::Arc;

/// Creates a new client based on the provided hostname.
///
/// The returned client is contained in an `Arc` to avoid cloning
/// the client directly as this would be undesired behaviour.
pub fn create_client<N>(host: N) -> Result<Arc<AsyncClient>, Error>
where
    N: Into<NodeAddress>,
{
    AsyncClientBuilder::new()
        .static_node(host)
        .build()
        .map(Arc::new)
        .map_err(|e| format_err!("{}", e.to_string()))
}

/// Attempts to parse a host/index pair out of the CLI arguments.
///
/// This logic is pretty vague; we don't actually test connection beyond
/// looking to see if the provided scheme is HTTP(S). The index string
/// returned will never be empty; if no index is provided, we'll use the
/// ES "_all" alias to avoid having to deal with `Option` types for now.
pub fn parse_cluster(target: &str) -> Result<(String, String), Error> {
    // attempt to parse the resource
    let mut url = Url::parse(target)?;

    // this is invalid, so not entirely sure what to do here
    if !url.has_host() || !url.scheme().starts_with("http") {
        return Err(format_err!("Invalid cluster resource provided"));
    }

    // fetch index from path, trimming the prefix
    let index = url.path().trim_start_matches('/');

    // set default index
    if index.is_empty() {
        "_all"
    } else {
        index
    };

    // take ownership to enable mut url
    let index = index.to_owned();

    // trim the path
    url.set_path("");

    // assume we have a cluster now, so pass it back
    Ok((url.as_str().trim_end_matches('/').to_owned(), index))
}
