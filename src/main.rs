//! A simple (but quick) tool for backing up Elasticsearch documents.
//!
//! It's designed for efficient import/export of Elasticsearch indices,
//! rather than complex use cases. If you want anything more than simple
//! backup/restore, you probably want to look somewhere else (or extend
//! this tool as necessary).
//!
//! Limber is only built as a command line tool, as it's simply a small
//! CLI binding around the fairly low-level Elasticsearch library APIs.
#![doc(html_root_url = "https://docs.rs/limber/1.1.1")]
use anyhow::Result;
use clap::Command;

mod command;
use command::*;

mod remote;
mod stats;

#[tokio::main]
async fn main() -> Result<()> {
    match build_cli().get_matches().subcommand() {
        Some(("export", args)) => export::run(args).await,
        Some(("import", args)) => import::run(args).await,
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
fn build_cli<'a>() -> Command<'a> {
    Command::new("")
        // package metadata from cargo
        .name(env!("CARGO_PKG_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        // attach all commands
        .subcommand(export::cmd())
        .subcommand(import::cmd())
        // settings required for parsing
        .arg_required_else_help(true)
        .hide_possible_values(true)
}
