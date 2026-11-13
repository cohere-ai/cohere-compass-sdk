# Cohere Compass SDK

[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)

The Compass SDK is a Python library that allows you to parse documents and insert them
into a Compass index.

In order to parse documents, the Compass SDK relies on the Compass Parser API, which is
a RESTful API that receives files and returns parsed documents. This requires a hosted
Compass server.

The Compass SDK provides a `CompassParserClient` that allows to interact with the parser
API from your Python code in a convenient manner. The `CompassParserClient` provides
methods to parse single and multiple files, as well as entire folders, and supports
multiple file types (e.g., `pdf`, `docx`, `json`, `csv`, etc.) as well as different file
systems (e.g., local, S3, GCS, etc.).

To insert parsed documents into a `Compass` index, the Compass SDK provides a
`CompassClient` class that allows to interact with a Compass API server. The Compass API
is also a RESTful API that allows to create, delete and search documents in a Compass
index.

## Table of Contents

<!--
Do NOT remove the line below; it is used by markdown-toc to automatically generate the
Table of Contents.

To update the Table Of Contents, execute the following command in the repo root dir:

markdown-toc -i README.md

If you don't have the markdown-toc tool, you can install it with:

npm i -g markdown-toc # use sudo if you use a system-wide node installation.
>

<!-- toc -->

- [Getting Started](#getting-started)
  * [Installation](#installation)
  * [Extensions](#extensions)
- [V2 Migration Guide](#v2-migration-guide)
- [Local Development](#local-development)
  * [Create Python Virtual Environment](#create-python-virtual-environment)
  * [Running Tests Locally](#running-tests-locally)
    + [VSCode Users](#vscode-users)
  * [Pre-commit](#pre-commit)

<!-- tocstop -->

## Getting Started

### Installation

To install the SDK using `pip`:

```bash
pip install cohere-compass-sdk
```

If you are using a package management tool like `poetry` or `uv`:

```
poetry add cohere-compass-sdk
```

or

```
uv add cohere-compass-sdk
```

Once you install it, the best way to learn how to use the SDK is to head over to [our
examples](https://github.com/cohere-ai/cohere-compass-sdk/tree/main/examples). For the
API reference, you can visit this
[link](https://private.docs.cohere.com/compass-standalone/reference).

### Extensions

Extensions are optional, higher-level features built on the clients, under
`cohere_compass.ext`. Each has its own extra for the dependencies it needs, and
the `all` extra installs every extension's:

```bash
pip install "cohere-compass-sdk[uploads]"
pip install "cohere-compass-sdk[all]"
```

`cohere_compass.ext.uploads` uploads a local directory or `gs://` prefix to an
index. Local sources need no extra; `gs://` sources need the `uploads` extra.

- Each file gets a document ID derived from its content and parser options, so
  documents the index already has are skipped.
- A bounded number of workers each own a file until Compass finishes
  processing it.
- With `prune=True`, the upload then deletes every index document that is not
  one of the files it uploads. Those are the source's files that pass
  `include_patterns` and `exclude_patterns`, so documents uploaded earlier from
  files the patterns now leave out are deleted too. `dry_run=True` reports what
  an upload would do without changing the index.

Jobs are saved in a store the caller chooses, so a failed or cancelled job can
be run again and continue where it stopped:

- `SqliteUploadJobStore(path)` keeps jobs in a SQLite database. Saving a file's
  status takes well under a millisecond, however large the job. Keep the
  database on a local disk, not a network file system such as NFS.
- `FileUploadJobStore(directory)` keeps one JSON file per job, which people
  can read and edit. Saving a file's status rewrites the whole file: about 25 ms
  in a job of 1,000 files and 250 ms in a job of 10,000, and a fresh upload
  saves each file's status about six times. Use it for jobs of up to a few
  thousand files.
- `InMemoryUploadJobStore()` keeps jobs only for the life of the process.

Stores create a missing state directory with mode 0700 and leave the mode of an
existing one unchanged. Close a store when done with it, with `store.close()` or
`async with`.

A caller can name its jobs with `create_upload_job(job_id=...)` and record its
own details with `metadata`; the workflow never reads them. The `uploads` module
docstring shows a complete example.

The uploads extension is asynchronous and uses `CompassAsyncClient`. Pruning keeps every
document ID in memory, so it is meant for indexes of up to tens of thousands of
documents.

## V2 Migration Guide

To improve the quality of the SDK and address multiple long-standing issues, as well
as supporting async clients, we decided to introduce v2.0, a new major version. v2.0 has
breaking changes and will require code changes. Fortunately, the changes are minimal
and can frequently be deduced just by looking at the new signatures of the APIs. Below
is a summary:

- Previously, we had multiple methods that relied on return values for error handling.
  This is no more the case, and almost all methods now raise exceptions in case of errors.
  This means that instead of a code like:

```python
result = compass_client.create_index(...)
if result.error:
    # do something about the error
```

you instead do:

```python
try:
    result = compass_client.create_index(...)
except:
    # do something about the error
```

- v2.0 supports async clients. Async clients maintain the same signature as their sync
  counterparts. So, where you would do the following to create an index:

```python
client = CompassClient(index_url=api_url, bearer_token=bearer_token)
client.create_index(...)
```

In async, you simply do:

```
client = CompassAsyncClient(index_url=api_url, bearer_token=bearer_token)
await client.create_index(...)
```

## Local Development

### Create Python Virtual Environment

We use Poetry to manage our Python environment. To create the virtual environment use
the following command:

```
poetry sync
```

### Running Tests Locally

We use `pytest` for testing. So, you can simply run tests using the following command:

```
poetry run python -m pytest
```

#### VSCode Users

We provide `.vscode` folder for those developers who prefer to use VSCode. You just need
to open the folder in VSCode and VSCode should pick our settings.

### Pre-commit

We love and appreciate Coding Standards and so we enforce them in our code base.
However, without automation, enforcing Coding Standards usually result in a lot of
frustration for developers when they publish Pull Requests and our linters complain. So,
we automate our formatting and linting with [pre-commit](https://pre-commit.com/). All
you need to do is install our `pre-commit` hook so the code gets formatted automatically
when you commit your changes locally:

```bash
poetry run pre-commit install
```
