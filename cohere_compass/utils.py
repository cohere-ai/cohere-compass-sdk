# Python imports
import base64
import glob
import logging
import os
import uuid
from collections.abc import Iterable, Iterator
from concurrent import futures
from concurrent.futures import Executor
from typing import Callable, Optional, TypeVar

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

T = TypeVar("T")
U = TypeVar("U")

logger = logging.getLogger(__name__)


def imap_queued(
    executor: Executor, f: Callable[[T], U], it: Iterable[T], max_queued: int
) -> Iterator[U]:
    """
    Similar to Python's `map`, but uses an executor to parallelize the calls.

    :param f: the function to call.
    :param it: the iterable to map over.
    :param max_queued: the maximum number of futures to keep in flight.
    :returns: an iterator over the results.
    """
    assert max_queued >= 1
    futures_set: set[futures.Future[U]] = set()

    for x in it:
        futures_set.add(executor.submit(f, x))
        while len(futures_set) > max_queued:
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


def get_fs(document_path: str) -> AbstractFileSystem:
    """
    Get an fsspec's filesystem object for the given document path.

    :param document_path: the path to the document

    :returns: the filesystem object.
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

    :returns: a CompassDocument object.
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
    allowed_extensions: Optional[list[str]] = None,
    recursive: bool = False,
) -> list[str]:
    """
    Scan a folder for files with the given extensions.

    :param folder_path: the path of the folder to scan.
    :param allowed_extensions: the extensions to look for. If None, all files will be
        considered.
    :param recursive: whether to scan the folder recursively or to stick to the top
        level.

    :returns: A list of file paths.
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

    :returns: The generated UUID based on the file bytes.
    """
    b64_string = base64.b64encode(filebytes).decode("utf-8")
    namespace = uuid.UUID(UUID_NAMESPACE)
    return uuid.uuid5(namespace, b64_string)
