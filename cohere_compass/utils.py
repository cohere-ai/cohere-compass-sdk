# Python imports
import base64
import glob
import logging
import os
import uuid
from collections.abc import Iterable, Iterator
from concurrent import futures
from concurrent.futures import Executor
from typing import Callable, Optional, TypeVar, Generator, BinaryIO, cast, Mapping

# 3rd party imports
import fsspec  # type: ignore
from fsspec import AbstractFileSystem  # type: ignore

# Local imports
from cohere_compass.constants import (
    DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES,
    DEFAULT_PROCESSING_CHUNK_SIZE,
    UUID_NAMESPACE,
)
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


def get_file_size(document_path: str) -> int:
    """
    Get the size of a file in bytes.

    :param document_path: the path to the document
    :returns: the size of the file in bytes
    """
    fs = get_fs(document_path)
    return fs.size(document_path)  # type: ignore


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


def iter_file_chunks(
    file_obj: BinaryIO,
    chunk_size: int,
) -> Generator[bytes, None, None]:
    """
    Read a file in chunks.
    
    This function reads a file in chunks of chunk_size bytes and yields each chunk.
    The function is memory-efficient as it only reads chunk_size bytes at a time.
    
    :param file_obj: file-like object supporting read method
    :param chunk_size: size of each chunk in bytes
    :yields: chunks of the file as bytes
    """
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        yield chunk


def open_document_in_chunks(
    document_path: str, 
    chunk_size: int = DEFAULT_PROCESSING_CHUNK_SIZE,
    original_filename: Optional[str] = None,
    total_chunks: Optional[int] = None,
) -> Generator[tuple[CompassDocument, int, int], None, None]:
    """
    Open the document at the given path and yield chunks as CompassDocument objects.

    This function is memory-efficient as it only reads chunk_size bytes at a time
    using streaming file access.
    
    :param document_path: the path to the document
    :param chunk_size: size of each chunk in bytes
    :param original_filename: original filename (for metadata)
    :param total_chunks: total number of chunks (pre-calculated)
    
    :yields: tuples of (CompassDocument, chunk_number, total_chunks)
    """
    try:
        fs = get_fs(document_path)
        file_size = fs.size(document_path)  # type: ignore
        
        # If total_chunks wasn't provided, calculate it
        actual_chunks: int = total_chunks if total_chunks is not None else (file_size + chunk_size - 1) // chunk_size
        
        display_name = str(original_filename or document_path)
        
        with fs.open(document_path, "rb") as f:  # type: ignore
            file_obj = cast(BinaryIO, f)
            for chunk_num, chunk_bytes in enumerate(iter_file_chunks(file_obj, chunk_size), 1):
                # Create a document for this chunk
                chunk_name = f"{display_name}_part_{chunk_num:03d}_of_{actual_chunks:03d}"
                
                # Create metadata dictionary with explicit string values
                chunk_metadata = {
                    "compass_original_filename": display_name,
                    "compass_chunk_number": str(chunk_num),
                    "compass_total_chunks": str(actual_chunks),
                }
                
                # Create the document with appropriate metadata
                doc = CompassDocument(
                    metadata=CompassDocumentMetadata(filename=chunk_name),
                    filebytes=chunk_bytes,
                    content=chunk_metadata,  # Store chunking info in content
                )
                
                yield doc, chunk_num, actual_chunks
                
    except Exception as e:
        # Create an error document
        doc = CompassDocument(metadata=CompassDocumentMetadata(filename=document_path))
        doc.errors = [{CompassSdkStage.Parsing: str(e)}]
        yield doc, 1, 1


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
