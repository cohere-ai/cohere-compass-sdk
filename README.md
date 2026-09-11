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
- [V3 Migration Guide](#v3-migration-guide)
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
[link](https://cohere-preview-d28024ac-1edf-416c-95be-73c5fe85a7c5.docs.buildwithfern.com/compass/reference/list-indexes-v-1-indexes-get).

## V3 Migration Guide

v3.0 is a breaking release. Upgrade it together with a Compass deployment that
emits the current API shapes (`document_id`, dict-shaped `metadata.meta`,
integer `sort_id`). Older Compass builds that still send `doc_id`, list-shaped
`meta`, or string `sort_id` will fail validation.

### What you need to change in your code

**Search hits are one type now.** `search_documents` still returns documents;
`search_chunks` / `direct_search` still return chunks. There are no longer
separate `RetrievedScoredDocument` / `RetrievedChunkExtended` classes. Use
`RetrievedDocument` and `RetrievedChunk`. `score` is an optional field on both.

**Use `document_id` only.** Compass treats parent and document id as the same
value. `parent_document_id` is gone from parser and search models. If you were
reading `parent_document_id` or `doc_id`, switch to `document_id`.

**Do not rely on `chunk_id`.** Search and get-document chunks no longer expose
it. Identify a chunk by `document_id` + `sort_id`. Fetch assets with
`document_id` + `asset_id` via `get_asset_presigned_urls`.

**Presigned URLs can be missing.** `assets_info[].presigned_url` is
`str | None`. Audio and video assets return `None` — call
`get_asset_presigned_urls` (with `start_time` / `end_time` for clips) instead
of treating an empty string as a URL. A missing URL is not a parse error.

**Upload status uses `index_name`.** `UploadDocumentsStatus.destinations` is
gone. Read `index_name`. A `timeline` of lifecycle timestamps is also present.

**Stop sending `content_length_bytes`.** Compass ignored it; the SDK field is
removed. Upload bytes or a `file_data_uuid`, not both.

**Parser documents are stricter.** `CompassDocument.metadata.meta` is a
`dict[str, Any]`, not a list of singleton dicts. `sort_id` is an `int`.
`elements`, `markdown`, and `ignore_metadata_errors` are gone. If you were
branching on `has_markdown()` or those flags, drop that code.

**Retention policy responses are wrapped.** `get_retention_policy` expects
`{"retention_policy": {...}}` and returns the inner policy (or `None`). A bare
policy object is no longer accepted.

**Direct search no longer invents `size` or `scroll`.** If you omit them, Compass
uses its own defaults (`size=10`, `scroll="1m"`). The SDK previously sent
`size=100` and `scroll="1m"` itself. Pass `size` / `scroll` only when you want
to override the server.

**This SDK needs a matching Compass.** Point v3 at a Compass that has dropped
the old SDK shims. Downloaded parsed documents are served through the Compass
API, which should normalize stored blobs before they reach the client.

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
