import glob
import os

import fsspec  # type: ignore
from fsspec import AbstractFileSystem  # type: ignore

from cohere_compass.models import (
    CompassDocument,
    CompassDocumentMetadata,
    CompassSdkStage,
)


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
