import base64
import glob
import os
import uuid
from concurrent import futures
from concurrent.futures import Executor
from typing import Callable, Iterable, Iterator, List, Optional, TypeVar

import fsspec
from fsspec import AbstractFileSystem

from compass_sdk import CompassDocument, CompassDocumentMetadata, CompassSdkStage
from compass_sdk.constants import UUID_NAMESPACE

T = TypeVar("T")
U = TypeVar("U")


def imap_queued(executor: Executor, f: Callable[[T], U], it: Iterable[T], max_queued: int) -> Iterator[U]:
    assert max_queued >= 1
    futures_set = set()

    for x in it:
        futures_set.add(executor.submit(f, x))
        while len(futures_set) > max_queued:
            done, futures_set = futures.wait(futures_set, return_when=futures.FIRST_COMPLETED)
            for future in done:
                yield future.result()

    for future in futures.as_completed(futures_set):
        yield future.result()


def get_fs(document_path: str) -> AbstractFileSystem:
    """
    Get the filesystem object for the given document path
    :param document_path: the path to the document
    :return: the filesystem object
    """
    if document_path.find("://") >= 0:
        file_system = document_path.split("://")[0]
        fs = fsspec.filesystem(file_system)
    else:
        fs = fsspec.filesystem("local")
    return fs


def open_document(document_path) -> CompassDocument:
    """
    Opens a document regardless of the file system (local, GCS, S3, etc.) and returns a file-like object
    :param document_path: the path to the document
    :return: a file-like object
    """
    doc = CompassDocument(metadata=CompassDocumentMetadata(filename=document_path))
    try:
        fs = get_fs(document_path)
        with fs.open(document_path, "rb") as f:
            val = f.read()
            if val is not None and isinstance(val, bytes):
                doc.filebytes = val
            else:
                raise Exception(f"Expected bytes, got {type(val)}")
    except Exception as e:
        doc.errors = [{CompassSdkStage.Parsing: str(e)}]
    return doc


def scan_folder(folder_path: str, allowed_extensions: Optional[List[str]] = None, recursive: bool = False) -> List[str]:
    """
    Scans a folder for files with the given extensions
    :param folder_path: the path to the folder
    :param allowed_extensions: the allowed extensions
    :param recursive: whether to scan the folder recursively or to only scan the top level
    :return: a list of file paths
    """
    fs = get_fs(folder_path)
    all_files = []
    path_prepend = f"{folder_path.split('://')[0]}://" if folder_path.find("://") >= 0 else ""

    if allowed_extensions is None:
        allowed_extensions = [""]
    else:
        allowed_extensions = [f".{ext}" if not ext.startswith(".") else ext for ext in allowed_extensions]

    for ext in allowed_extensions:
        rec_glob = "**/" if recursive else ""
        pattern = os.path.join(glob.escape(folder_path), f"{rec_glob}*{ext}")
        scanned_files = fs.glob(pattern, recursive=recursive)
        all_files.extend([f"{path_prepend}{f}" for f in scanned_files])
    return all_files


def generate_doc_id_from_bytes(filebytes: bytes) -> uuid.UUID:
    b64_string = base64.b64encode(filebytes).decode("utf-8")
    namespace = uuid.UUID(UUID_NAMESPACE)
    return uuid.uuid5(namespace, b64_string)
