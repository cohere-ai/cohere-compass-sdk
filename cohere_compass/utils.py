"""
Utility functions for the Cohere Compass SDK.

This module provides utility functions for parallel processing, filesystem abstraction,
document loading, and other common operations used throughout the SDK.
"""

# Python imports
import asyncio
import base64
import glob
import logging
import os
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from concurrent import futures
from concurrent.futures import Executor
from typing import TypeVar

# 3rd party imports
import fsspec  # type: ignore
from fsspec import AbstractFileSystem  # type: ignore

# Local imports
from cohere_compass.constants import UUID_NAMESPACE
from cohere_compass.models import (
    CompassDocument,
    CompassDocumentMetadata,
    CompassSdkStage,
)
from cohere_compass.models.documents import Chunk, CompassDocumentStatus, Document

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")

logger = logging.getLogger(__name__)


async def async_enumerate(
    iterable: AsyncIterable[T], start: int = 0
) -> AsyncIterator[tuple[int, T]]:
    """
    Enumerate an async iterable, just like Python's `enumerate()` but for async.

    :param iterable: The iterable to enumerate.
    :param start: The start index.

    :return: An async iterator of tuples of the index and the item.
    """
    i = start
    async for item in iterable:
        yield i, item
        i += 1


def imap_parallel(
    executor: Executor, f: Callable[[T], U], it: Iterable[T], max_parallelism: int
):
    """
    Map a function over an iterable using parallel execution with parallelism limit.

    Similar to Python's built-in map(), but uses an executor to parallelize
    the function calls and limits the number of concurrent futures.

    :param executor: The executor to use for parallel execution.
    :param f: The function to apply to each item.
    :param it: The iterable to map over.
    :param max_parallelism: Maximum number of futures to keep in flight.

    :return: Yields results from applying f to each item in it.

    :raises ValueError: If max_parallelism is less than 1.

    """
    if max_parallelism < 1:
        raise ValueError("max_parallelism must be at least 1")
    futures_set: set[futures.Future[U]] = set()

    for x in it:
        futures_set.add(executor.submit(f, x))
        while len(futures_set) > max_parallelism:
            done, futures_set = futures.wait(
                futures_set, return_when=futures.FIRST_COMPLETED
            )

            for future in done:
                try:
                    yield future.result()
                except Exception as e:
                    logger.exception(f"Error in processing file: {e}")

    for future in futures.as_completed(futures_set):
        try:
            yield future.result()
        except Exception as e:
            logger.exception(f"Error in processing file: {e}")


async def async_map(
    func: Callable[[T], Awaitable[R]],
    iterable: Iterable[T],
    limit: int | None = None,
) -> list[R]:
    """
    Run an async function over an iterable with a limit on concurrent executions.

    The function preserves the original order of results.

    :param func: An async function to apply to each item.
    :param iterable: An iterable of input items.
    :param limit: Maximum number of concurrent tasks.

    :return: A list of results, in the same order as the input iterable.

    """
    results: dict[int, R] = {}

    if limit is not None:
        semaphore = asyncio.Semaphore(limit)

        async def _task(idx: int, item: T):
            async with semaphore:
                results[idx] = await func(item)
    else:

        async def _task(idx: int, item: T):
            results[idx] = await func(item)

    tasks = [_task(idx, item) for idx, item in enumerate(iterable)]
    await asyncio.gather(*tasks)

    return [results[i] for i in sorted(results)]


async def async_apply(
    func: Callable[..., Awaitable[None]],
    iterable: Iterable[T] | AsyncIterable[T],
    limit: int | None = None,
) -> None:
    """
    Apply an async function over an iterable with a limit on concurrent executions.

    :param func: An async function to apply to the arguments.
    :param iterable: One or more iterables whose elements will be passed to func.
    :param limit: Maximum number of concurrent tasks.
    """
    if limit is not None:
        semaphore = asyncio.Semaphore(limit)

        async def _task(item: T):
            async with semaphore:
                await func(item)
    else:

        async def _task(item: T):
            await func(item)

    if isinstance(iterable, AsyncIterable):
        tasks = [_task(item) async for item in iterable]
    else:
        tasks = [_task(item) for item in iterable]
    await asyncio.gather(*tasks)


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
    return fs


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
            val = f.read()
            if isinstance(val, bytes):
                doc.filebytes = val
            else:
                raise Exception(f"Expected bytes, got {type(val)}")
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
    path_prepend = (
        f"{folder_path.split('://')[0]}://" if folder_path.find("://") >= 0 else ""
    )

    if allowed_extensions is None:
        allowed_extensions = [""]
    else:
        allowed_extensions = [
            f".{ext}" if not ext.startswith(".") else ext for ext in allowed_extensions
        ]

    for ext in allowed_extensions:
        rec_glob = "**/" if recursive else ""
        pattern = os.path.join(glob.escape(folder_path), f"{rec_glob}*{ext}")
        scanned_files = fs.glob(pattern, recursive=recursive)  # type: ignore
        all_files.extend([f"{path_prepend}{f}" for f in scanned_files])  # type: ignore
    return all_files


def generate_doc_id_from_bytes(filebytes: bytes) -> uuid.UUID:
    """
    Generate a UUID based on the provided file bytes.

    This function encodes the given file bytes into a base64 string and then generates a
    UUID using the uuid5 method with a predefined namespace.

    :param filebytes: The bytes of the file to generate the UUID from.

    :return: The generated UUID based on the file bytes.
    """
    b64_string = base64.b64encode(filebytes).decode("utf-8")
    namespace = uuid.UUID(UUID_NAMESPACE)
    return uuid.uuid5(namespace, b64_string)


def partition_documents(
    docs: Iterable[CompassDocument],
    max_chunks_per_request: int,
):
    """
    Create request blocks to send to the Compass API.

    :param docs: the documents to send
    :param max_chunks_per_request: the maximum number of chunks to send in a single
        API request
    :return: an iterator over the request blocks
    """
    request_block: list[tuple[CompassDocument, Document]] = []
    errors: list[dict[str, str]] = []
    num_chunks = 0
    for doc in docs:
        if doc.status != CompassDocumentStatus.Success:
            logger.error(
                f"Document {doc.metadata.document_id} has errors: {doc.errors}"
            )
            for error in doc.errors:
                errors.append({doc.metadata.document_id: next(iter(error.values()))})
        else:
            num_chunks += (
                len(doc.chunks) if doc.status == CompassDocumentStatus.Success else 0
            )
            if num_chunks > max_chunks_per_request:
                yield request_block, errors
                request_block, errors = [], []
                num_chunks = 0

            request_block.append(
                (
                    doc,
                    Document(
                        document_id=doc.metadata.document_id,
                        parent_document_id=doc.metadata.parent_document_id,
                        path=doc.metadata.filename,
                        content=doc.content,
                        chunks=[Chunk(**c.model_dump()) for c in doc.chunks],
                        index_fields=doc.index_fields,
                    ),
                )
            )

    if len(request_block) > 0 or len(errors) > 0:
        yield request_block, errors


async def partition_documents_async(
    docs: AsyncIterable[CompassDocument],
    max_chunks_per_request: int,
):
    """
    Create request blocks to send to the Compass API.

    :param docs: the documents to send
    :param max_chunks_per_request: the maximum number of chunks to send in a single
        API request
    :return: an iterator over the request blocks
    """
    request_block: list[tuple[CompassDocument, Document]] = []
    errors: list[dict[str, str]] = []
    num_chunks = 0
    async for doc in docs:
        if doc.status != CompassDocumentStatus.Success:
            logger.error(
                f"Document {doc.metadata.document_id} has errors: {doc.errors}"
            )
            for error in doc.errors:
                errors.append({doc.metadata.document_id: next(iter(error.values()))})
        else:
            num_chunks += (
                len(doc.chunks) if doc.status == CompassDocumentStatus.Success else 0
            )
            if num_chunks > max_chunks_per_request:
                yield request_block, errors
                request_block, errors = [], []
                num_chunks = 0

            request_block.append(
                (
                    doc,
                    Document(
                        document_id=doc.metadata.document_id,
                        parent_document_id=doc.metadata.parent_document_id,
                        path=doc.metadata.filename,
                        content=doc.content,
                        chunks=[Chunk(**c.model_dump()) for c in doc.chunks],
                        index_fields=doc.index_fields,
                    ),
                )
            )

    if len(request_block) > 0 or len(errors) > 0:
        yield request_block, errors
