# Limber
[![Build Status](https://img.shields.io/github/workflow/status/whitfin/limber/CI)](https://github.com/whitfin/limber/actions)
[![Crates.io](https://img.shields.io/crates/v/limber.svg)](https://crates.io/crates/limber)

A simple (but quick) tool for backing up Elasticsearch documents.

Limber offers a very minimal interface to export and import documents from
Elasticsearch clusters. Other tools do exist for this purpose, but they're
fairly slow and thus unusable on larger clusters. This tool is intended to
be as lightweight and fast as possible (given that it's a CLI tool). Usage
is meant to align with Unix style pipes to allow piping directly into other
clusters, or your own compression algorithms, etc.

The feature set is deliberately small as other tools exist for those who
are interested in sugar rather than speed. If you would like to request a
new feature, please file an issue and we can discuss inclusion.

### Installation

Limber will be available via [Crates.io](https://crates.io/crates/limber),
so you can install it directly with `cargo`:

```shell
$ cargo install limber
```

Once I become more familiar with cross compilation, I'll try to attach some
pre-built binaries to the repository to make it easier for those outside of
the Rust ecosystem.

### Usage

This section will cover (at a very high level), the most relevant commands
offered by Limber. If you want up to date usage, please check the help menu
generated via `limber -h` in your terminal session.

#### Exporting Documents

To export documents from an Elasticsearch cluster, you can use the `export`
subcommand. This is formed in the following pattern:

```shell
$ limber export <source>
```

The `source` parameter is required, and much contain the Elasticsearch URL
of the cluster you wish to export from. It is assumed that the cluster is
reachable from the machine you're running this tool on. If you wish to
export from a specific index, you can also include this on the URL in the
form of:

```text
http://localhost:9200/my_index
http://localhost:9200/my_first_index,my_second_index
```

Exported documents are dumped directly to `stdout`, and progress will be
reported to `stderr`. This allows you to pipe the results into whatever
destination you wish in a streaming fashion. As such, invocation of the
export command typically looks something like this:

```shell
$ limber export http://localhost:9200 | gzip -9 > export.jsonl.gz
```

The above command will compress all exported data into the `export.jsonl.gz`
file, whilst occasionally reporting progress to your terminal session (as
long as you don't also redirect `stderr`).

There are several options which can be used to customize the export, such
as the concurrency factor, batch sizes, document filtering, etc. All of
these options can be found via `limber export -h`.

#### Importing Documents

The process of importing documents is extremely similar to exporting them,
except it operates in reverse. Just like the `export` command, this is
formed in the following pattern:

```shell
$ limber import <target>
```

The `target` parameter is required, and works in the same way as the `source`
parameter to control which index you place your documents into. If you do
not provide an index, the documents will be placed into the same index name
as the one they were exported from. Naturally, using multiple index names no
longer makes sense when indexing documents.

In similar fashion to exports, documents to import are read from `stdin` in
order to allow you to stream from one location to another. Below is the an
example of importing the documents exported in the example further up:

```shell
$ gzcat export.jsonl.gz | limber import http://localhost:9200
```

You might notice that the API here allows you to pipe from one cluster or
index directly into another. As an example:

```shell
$ limber export http://localhost:9200/my_first_index | \
    limber import http://localhost:9200/my_second_index
```

The import command also allows for customization of concurrency factor and
batch sizes. For all available options, please see `limber import -h`.

