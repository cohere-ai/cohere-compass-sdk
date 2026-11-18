"""
Filesystem access for the SDK, local and remote.

This is the only module in the SDK that may import fsspec or gcsfs. Ruff's
banned-API rule (TID251) rejects those imports in every other module, so that
the rule below is enforced in one place.

fsspec caches filesystem objects for the whole process: `fsspec.filesystem("gs")`
returns the same `GCSFileSystem` to every caller, including the synchronous
helpers in this module, pandas and `fsspec.open`. gcsfs creates that object's
aiohttp session on the event loop that makes its first request, and the session
fails with "attached to a different loop" when it is used from any other loop.
fsspec runs synchronous calls on its own background loop. If async code awaited
the shared object's coroutine methods (`_info`, `_cat_file` and so on) on its own
loop, it would bind the session to that loop and break every synchronous caller
in the process, and a session first created by a synchronous caller would break
the async code in the same way. So:

- Synchronous code uses fsspec's shared objects through their public methods, as
  `get_fs`, `open_document` and `scan_folder` do.
- Async code uses `open_gcs_filesystem`, which creates a private filesystem that
  fsspec never hands to another caller, and closes its connections when the block exits.
"""

import glob
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Protocol, cast

import fsspec  # type: ignore
from fsspec import AbstractFileSystem  # type: ignore

from cohere_compass.models import (
    CompassDocument,
    CompassDocumentMetadata,
    CompassSdkStage,
)

GCS_EXTRA_HINT = "Reading gs:// paths needs gcsfs. Install it with `pip install 'cohere-compass-sdk[uploads]'`."


class AsyncFilesystem(Protocol):
    """
    The coroutine methods of an async fsspec filesystem that the SDK uses.

    These are fsspec's underscore-prefixed async implementations. Only call them on
    a filesystem from `open_gcs_filesystem`, never on one from `fsspec.filesystem`
    or `get_fs`; the module docstring explains why.
    """

    async def _info(self, path: str) -> Mapping[str, object]: ...

    async def _isdir(self, path: str) -> bool: ...

    async def _find(self, path: str, *, detail: bool) -> Mapping[str, Mapping[str, object]]: ...

    async def _ls(self, path: str, *, detail: bool) -> Sequence[Mapping[str, object]]: ...

    async def _cat_file(self, path: str) -> bytes: ...


@asynccontextmanager
async def open_gcs_filesystem(**storage_options: object) -> AsyncIterator[AsyncFilesystem]:
    """
    Open a private async GCS filesystem for the running event loop.

    The filesystem is not fsspec's shared cached instance, so using it cannot break
    other code in the process that reads ``gs://`` paths. Use it only on the event
    loop that opened it and only inside the ``async with`` block, whose exit closes
    its connections. Paths are bucket-and-key, without the ``gs://`` scheme.

    *storage_options* are passed to `gcsfs.GCSFileSystem`, for example ``project``
    or ``token``. By default it uses Application Default Credentials.

    Raises `ImportError` if gcsfs is not installed.
    """
    try:
        filesystem = fsspec.filesystem(  # type: ignore
            "gs", asynchronous=True, skip_instance_cache=True, **storage_options
        )
    except ImportError as error:
        raise ImportError(GCS_EXTRA_HINT) from error
    try:
        yield cast(AsyncFilesystem, filesystem)
    finally:
        await _close_connections(filesystem)  # type: ignore


async def _close_connections(filesystem: object) -> None:
    # gcsfs has no public coroutine that releases a filesystem's connections, so
    # this uses two private members. `_close_resources` exists on
    # `ExtendedGcsFileSystem`, which gcsfs returns by default, and closes the
    # gRPC clients it creates to look up each bucket's type. `_session` is the
    # aiohttp session that every gcsfs filesystem creates on its first request.
    # Closing both here, on the loop that created them, releases their
    # connections now. Otherwise they would stay open until garbage collection,
    # and closing them then fails with "Event loop is closed" once their loop
    # has stopped.
    close_resources = getattr(filesystem, "_close_resources", None)
    if close_resources is not None:
        await close_resources()
    session = getattr(filesystem, "_session", None)
    if session is not None and not session.closed:
        await session.close()


def get_fs(document_path: str) -> AbstractFileSystem:
    """
    Get an fsspec filesystem object for the given document path.

    Supports various filesystem types including local, S3, GCS, and others
    via the fsspec library.

    :param document_path: Path to the document, can include filesystem prefix
        (e.g., "s3://bucket/file" or "/local/path").

    :return: AbstractFileSystem instance appropriate for the given path.

    """
    if document_path.find("://") >= 0:
        file_system = document_path.split("://")[0]
        fs = fsspec.filesystem(file_system)  # type: ignore
    else:
        fs = fsspec.filesystem("local")  # type: ignore
    return fs  # type: ignore


def open_document(document_path: str) -> CompassDocument:
    """
    Open the document at the given path and return a CompassDocument object.

    :param document_path: the path to the document

    :return: a CompassDocument object.
    """
    doc = CompassDocument(metadata=CompassDocumentMetadata(filename=document_path))
    try:
        fs = get_fs(document_path)
        with fs.open(document_path, "rb") as f:  # type: ignore
            val = f.read()  # type: ignore
            if isinstance(val, bytes):
                doc.filebytes = val
            else:
                raise Exception(f"Expected bytes, got {type(val)}")  # type: ignore
    except Exception as e:
        doc.errors = [{CompassSdkStage.Parsing: str(e)}]
    return doc


def scan_folder(
    folder_path: str,
    allowed_extensions: list[str] | None = None,
    recursive: bool = False,
) -> list[str]:
    """
    Scan a folder for files with the given extensions.

    :param folder_path: the path of the folder to scan.
    :param allowed_extensions: the extensions to look for. If None, all files will be
        considered.
    :param recursive: whether to scan the folder recursively or to stick to the top
        level.

    :return: A list of file paths.
    """
    fs = get_fs(folder_path)
    all_files: list[str] = []
    path_prepend = f"{folder_path.split('://')[0]}://" if folder_path.find("://") >= 0 else ""

    if allowed_extensions is None:
        allowed_extensions = [""]
    else:
        allowed_extensions = [f".{ext}" if not ext.startswith(".") else ext for ext in allowed_extensions]

    for ext in allowed_extensions:
        rec_glob = "**/" if recursive else ""
        pattern = os.path.join(glob.escape(folder_path), f"{rec_glob}*{ext}")
        scanned_files = fs.glob(pattern, recursive=recursive)  # type: ignore
        all_files.extend([f"{path_prepend}{f}" for f in scanned_files])  # type: ignore
    return all_files
