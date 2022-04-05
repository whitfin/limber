//! Utility functions for dealing with remote locations.
//!
//! This module offers functions for interacting with a remote cluster,
//! such as hostname parsing, client creation, etc.
use anyhow::{anyhow, Result};
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use url::Url;

/// Creates a new client based on the provided hostname.
pub fn create_client(host: &str) -> Result<Elasticsearch> {
    Ok(Elasticsearch::new(Transport::single_node(host)?))
}

/// Attempts to parse a host/index pair out of the CLI arguments.
///
/// This logic is pretty vague; we don't actually test connection beyond
/// looking to see if the provided scheme is HTTP(S). The index string
/// returned will never be empty; if no index is provided, we'll use an
/// empty `Option` type to allow the caller to decide how to handle it.
pub fn parse_cluster(target: &str) -> Result<(String, Option<String>)> {
    // attempt to parse the resource
    let mut url = Url::parse(target)?;

    // this is invalid, so not entirely sure what to do here
    if !url.has_host() || !url.scheme().starts_with("http") {
        return Err(anyhow!("Invalid cluster resource provided"));
    }

    // fetch index from path, trimming the prefix
    let index = url.path().trim_start_matches('/');

    // set default index
    let index = if index.trim().is_empty() {
        None
    } else {
        Some(index.to_owned())
    };

    // trim the path
    url.set_path("");

    // assume we have a cluster now, so pass it back
    Ok((url.as_str().trim_end_matches('/').to_owned(), index))
}
