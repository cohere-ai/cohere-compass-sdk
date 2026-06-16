# compass-sdk-examples

## Overview

This package contains various examples for interacting with the Compass SDK. The
examples provided can mostly be run directly or with minor modifications.

## Getting Started

Like the `cohere-compass-sdk`, this package uses `poetry` for dependency
management. To start using it, execute:

```
poetry sync
```

This package references the `cohere-compass-sdk` locally (i.e. installed via `poetry add -e
../`), so you might need to execute `poetry sync` on the parent folder as well.

Note: If you don't have `poetry`, head over to the [poetry installation
page](https://python-poetry.org/docs/#installation).

Next, you need to update your .env file with the URLs for the target Compass API
and Parser you want to run the examples against. Optionally, you could also set
the bearer tokens if your Compass instance has multi-tenancy enabled. Example
.env file below:

```
COMPASS_API_URL=https://<your compass API URL>
COMPASS_API_BEARER_TOKEN=
COMPASS_PARSER_URL=https://<your compass parser URL>
COMPASS_PARSER_BEARER_TOKEN=
```

## Running Examples

To run an example, e.g. `list_indexes.py`, simply execute the following command:

```
poetry run python -m compass_sdk_examples.list_indexes
```
