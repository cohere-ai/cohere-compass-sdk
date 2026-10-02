# AGENTS.md

## Project Overview

Cohere Compass SDK is a Python library for parsing documents and interacting with a Compass index. It targets Python >=3.11 and is managed with Poetry. The package name is `cohere-compass-sdk` (importable as `cohere_compass`).

The SDK provides two main sets of client classes:
- **CompassClient / CompassAsyncClient** -- synchronous and asynchronous clients for the Compass index API (CRUD, search, RBAC)
- **CompassParserClient / CompassParserAsyncClient** -- synchronous and asynchronous clients for the Compass Parser API (document parsing)
- **CompassRootClient** -- synchronous client for RBAC (Role-Based Access Control) administration

## Commands

### Install dependencies
```bash
poetry install
```

### Run tests
```bash
poetry run pytest -sv
```

### Run linting and formatting
```bash
poetry run ruff check --fix .
poetry run ruff format .
```

### Run type checking
```bash
poetry run pyright
```

### Run pre-commit hooks manually
```bash
pre-commit run --all-files
```

### Install pre-commit hooks (local dev setup)
```bash
poetry run pre-commit install
```

## Repository Structure

```
cohere-compass-sdk/
  cohere_compass/                    # Main SDK package
    __init__.py                      # Package version, legacy param models (ProcessFileParameters, GroupAuthorizationInput, etc.)
    constants.py                     # Default configs, timeouts, retry settings, regex patterns
    exceptions.py                    # Exception hierarchy + httpx-to-Compass exception mapper
    models/                          # Pydantic data models
      __init__.py                    # ValidatedModel base class, re-exports
      access_control.py              # RBAC models: User, Role, Group, Policy, Pagination
      config.py                      # ParserConfig, EnrichmentConfig, IndexConfig, strategy enums
      documents.py                   # CompassDocument, chunks, upload/download models, ContentTypeEnum
      enrichments.py                 # Webhook enrichment request/response models
      indexes.py                     # IndexInfo, IndexDetails, RetentionPolicy
      search.py                      # SearchInput, SearchFilter, RetrievedChunk/Document, DirectSearchInput
    clients/                         # API client implementations
      __init__.py                    # Re-exports all 5 client classes
      compass.py                     # CompassClient (sync) -- index CRUD, document ops, search, assets
      compass_async.py               # CompassAsyncClient (async) -- mirrors CompassClient
      parser.py                      # CompassParserClient (sync) -- file/folder parsing
      parser_async.py                # CompassParserAsyncClient (async) -- mirrors CompassParserClient
      access_control.py              # CompassRootClient (sync) -- RBAC admin
    utils/                           # Internal utility modules
      __init__.py
      asyn.py                        # async_enumerate, async_map, async_apply
      documents.py                   # generate_doc_id_from_bytes, partition_documents, partition_documents_async
      fs.py                          # get_fs, open_document, scan_folder (fsspec-based)
      iter.py                        # imap_parallel (bounded parallel map)
      retry.py                       # is_retryable_httpx_exception, is_retryable_compass_exception
    py.typed                         # PEP 561 marker
  tests/                             # Test suite
    test_compass_client.py           # CompassClient + CompassAsyncClient tests (parametrized sync/async)
    test_parser_client.py           # CompassParserClient tests
    test_access_control.py           # CompassRootClient RBAC tests
    test_context_management.py       # Context manager and resource cleanup tests for all 4 main clients
    test_utils.py                    # Tests for utils: asyn, iter, fs, documents
    utils.py                         # Test helpers: create_test_doc, SyncifiedCompassAsyncClient
    docs/                            # Sample test documents (pdf, docx, xlsx, etc.)
  examples/                          # Standalone Poetry project with example scripts
  .github/workflows/                 # CI: test.yml, pre-commit-checks.yml, lint-pr.yaml, check-version-bump.yml, autogenerate-release.yml
  pyproject.toml                     # Project config, dependencies, tool settings
  poetry.toml                        # Poetry local config (in-project venv)
  .pre-commit-config.yaml            # Ruff + Pyright hooks
```

## Architecture

### Client Design Pattern

Each client wraps an `httpx.Client` (sync) or `httpx.AsyncClient` (async) and follows a consistent pattern:
- Constructor accepts `compass_url`/`index_url`/`parser_url`, optional `bearer_token`, `timeout`, and optional externally-owned `httpx_client`
- Clients track ownership of the httpx client (`_own_httpx_client` flag) -- they only close clients they created
- All HTTP calls are wrapped with `handle_httpx_exceptions()` from `exceptions.py`, which converts httpx errors to typed Compass exceptions
- Retry logic uses `tenacity` with `is_retryable_httpx_exception` (for raw httpx errors in Compass clients) or `is_retryable_compass_exception` (for already-converted errors in Parser clients)
- Default timeouts are defined in `constants.py` per client type

### Sync/Async Parity

`CompassClient` and `CompassAsyncClient` share the same `API_DEFINITIONS` dict (defined in `compass.py`, imported by `compass_async.py`). They have identical public APIs. `CompassParserClient` and `CompassParserAsyncClient` similarly mirror each other. The async versions use `async_apply`/`async_map` from `utils/asyn.py` instead of `imap_parallel`/`joblib.Parallel`.

### Error Handling (V2 Breaking Change)

V2+ uses exception-based error handling. All HTTP errors are converted via `handle_httpx_exceptions()`:
- `httpx.TimeoutException` -> `CompassTimeoutError`
- `httpx.NetworkError` -> `CompassNetworkError`
- 401/403 -> `CompassAuthError`
- Other 4xx -> `CompassClientError`
- 5xx -> `CompassServerError`
- `CompassInsertionError` for batch insertion failures
- `CompassMaxErrorRateExceeded` for sliding-window error rate thresholds

### Models

All data models are Pydantic v2 models. `ValidatedModel` (in `models/__init__.py`) extends `BaseModel` with attribute-name validation and `arbitrary_types_allowed`/`use_enum_values` config. Models are organized by domain: `access_control.py`, `config.py`, `documents.py`, `enrichments.py`, `indexes.py`, `search.py`.

### Document Batch Insertion

`CompassClient.insert_docs()` uses `partition_documents()` from `utils/documents.py` to split documents into request-sized chunks (respecting `max_chunks_per_request`). It tracks errors with a sliding window and supports parallel insertion via `joblib.Parallel`.

### Document Parsing Pipeline

`CompassParserClient.process_file()` -> opens file via `utils/fs.py` -> sends bytes to parser API -> deserializes response into `CompassDocument` objects. `process_folder()` scans via fsspec, then processes in parallel using `imap_parallel`.

## Code Conventions

### Style and Formatting
- **Ruff** is the linter and formatter. Line length is **120** (root project) / **88** (examples).
- Target Python 3.10+ for Ruff rules, but project requires >=3.11.
- Rules: mccabe (max complexity 15), pydocstyle, pycodestyle, isort, flake8-quotes, Ruff-specific, pyupgrade.
- Ignored docstring rules: D100, D104, D212 (root); D100, D103, D104, D200, D212 (examples).
- Import sorting identifies `cohere_compass` as first-party.

### Type Checking
- **Pyright** in strict mode.
- `reportMissingImports` is suppressed.
- Tests use `# pyright: reportPrivateUsage=false` when accessing private attributes.

### Testing
- **pytest** with `pytest-asyncio` and `pytest-cov`.
- 80% branch coverage minimum is enforced.
- All HTTP tests use **respx** for mocking (no integration tests against a real server).
- Sync/async parity is tested via `SyncifiedCompassAsyncClient` (in `tests/utils.py`) which wraps async methods with `asyncio.run()`.
- The `client` fixture in `test_compass_client.py` is parametrized to run every test against both sync and async clients.
- `mock_endpoint` decorator (in `test_compass_client.py`) provides declarative HTTP mock setup.
- Test utilities in `tests/utils.py`: `create_test_doc()` factory, `SyncifiedCompassAsyncClient` adapter.

### Version Bumping
- Every PR that modifies `cohere_compass/` or `pyproject.toml` **must** bump the version in `pyproject.toml`.
- A CI check (`check-version-bump.yml`) enforces this. PRs can be exempted with a `skip-version-check` label.

### Commit Conventions
- PR titles must follow **Conventional Commits** (e.g., `feat:`, `fix:`, `docs:`), enforced by `lint-pr.yaml`.

### Code Ownership
- All files are owned by `@cohere-ai/compass`. All PRs require approval from this team.

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `httpx` | HTTP client (sync + async) |
| `pydantic` v2 | Data models and validation |
| `tenacity` | Retry logic with configurable backoff |
| `fsspec` | Filesystem abstraction (local, S3, GCS) |
| `joblib` | Parallel batch document insertion |

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `test.yml` | Push to main, PRs to main | Runs `pytest -sv` across Python 3.11/3.12/3.13 |
| `pre-commit-checks.yml` | All PRs | Runs ruff + pyright across Python 3.11/3.12/3.13 |
| `lint-pr.yaml` | PR events | Enforces semantic PR titles |
| `check-version-bump.yml` | PRs to main | Blocks PRs without version bump |
| `autogenerate-release.yml` | Push to main (pyproject.toml change) | Auto-creates GitHub release + publishes to PyPI |

## Release Process

1. Bump version in `pyproject.toml`
2. Merge PR to `main` (must pass all CI checks)
3. `autogenerate-release.yml` creates GitHub release tag and publishes to PyPI via Poetry

## Common Tasks

### Adding a new API endpoint to CompassClient
1. Add the endpoint to the `API_DEFINITIONS` dict in `cohere_compass/clients/compass.py`
2. Add a method to `CompassClient` that calls `_send_request()` with the new API name
3. Add the equivalent async method to `CompassAsyncClient` (using `await _send_request()`)
4. Add Pydantic request/response models in `cohere_compass/models/` if needed
5. Add tests using `mock_endpoint` decorator in `test_compass_client.py` (parametrized for sync/async)
6. Bump version in `pyproject.toml`

### Adding a new model
1. Create or edit the appropriate file in `cohere_compass/models/`
2. Inherit from `ValidatedModel` (from `models/__init__.py`) or `BaseModel` (from pydantic) as appropriate
3. Re-export from `cohere_compass/models/__init__.py` if needed
4. Add tests for the model

### Adding a new exception
1. Add to `cohere_compass/exceptions.py`, inheriting from the appropriate base (`CompassError`, `CompassNetworkError`, `CompassClientError`, etc.)
2. Update `handle_httpx_exceptions()` if the exception should be auto-raised from HTTP errors
3. Add tests verifying the exception is raised in the correct scenarios
