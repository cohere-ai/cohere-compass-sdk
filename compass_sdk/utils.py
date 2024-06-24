import glob
import hashlib
import json
import os
from collections import Counter
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import fsspec
import pandas as pd
import pyarrow.parquet as pq
from compass_parser import logger
from compass_parser.constants import FILETYPES_TO_CONTENT_TYPES
from compass_parser.types import CompassDocument, CompassDocumentMetadata, CompassFileType, CompassSdkStage
from fsspec import AbstractFileSystem
from unstructured.documents.elements import Element
from unstructured.file_utils.filetype import FileType, detect_filetype

SUPPORTED_DATASET_FILETYPES = {
    CompassFileType.Parquet,
    CompassFileType.Csv,
    CompassFileType.Jsonl,
    CompassFileType.Tsv,
    CompassFileType.Dat,
    CompassFileType.Xls,
    CompassFileType.Xlsx,
}


def has_valid_dimensions(element: Element) -> bool:
    """Check if an element has valid dimensions, i.e., height and width are greater than 0"""
    coords = element.metadata.coordinates
    if not coords or not coords.points:
        return True

    height = abs(coords.points[1][1] - coords.points[0][1])
    width = abs(coords.points[2][0] - coords.points[0][0])
    return height > 0 and width > 0


def is_horizontal_text(element: Element, threshold=0.6) -> bool:
    """Check if a text element is in horizontal orientation."""
    coords = element.metadata.coordinates
    if not coords or not coords.points:
        return True

    height = abs(coords.points[1][1] - coords.points[0][1])
    width = abs(coords.points[2][0] - coords.points[0][0])
    return width / (height + 1e-10) > threshold


def are_elements_unique(elements: List[Any]) -> bool:
    """
    Check if all elements in a list are unique (ignoring None)
    :param elements: the list of elements
    :return: True if all elements are unique, False otherwise
    """

    count = Counter(elements)
    count.pop(None)
    return all(v == 1 for v in count.values())


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
    doc = CompassDocument(
        metadata=CompassDocumentMetadata(filename=document_path))
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
    path_prepend = f"{folder_path.split(
        '://')[0]}://" if folder_path.find("://") >= 0 else ""

    if allowed_extensions is None:
        allowed_extensions = [""]
    else:
        allowed_extensions = [f".{ext}" if not ext.startswith(
            ".") else ext for ext in allowed_extensions]

    for ext in allowed_extensions:
        rec_glob = "**/" if recursive else ""
        pattern = os.path.join(glob.escape(folder_path), f"{rec_glob}*{ext}")
        scanned_files = fs.glob(pattern, recursive=recursive)
        all_files.extend([f"{path_prepend}{f}" for f in scanned_files])
    return all_files


def is_binary(document_path: str) -> bool:
    """
    Returns True if the file is binary (e.g., PDF).
    :param document_path: the path to the document
    :return: True if the file is binary, False otherwise
    """
    fs: fsspec.AbstractFileSystem = get_fs(document_path)
    with fs.open(document_path, "rb") as f:
        byte_sample = f.read(1024)
        if isinstance(byte_sample, bytes):
            return b"\x00" in byte_sample
        return False


def get_dataset_type(doc_bytes: BytesIO, doc_path: str) -> CompassFileType:
    """
    Get the type of the dataset file
    :param doc_bytes: the file-like object
    :param doc_path: the path to the document
    :return: the type of the dataset file
    """
    filetype = CompassFileType(Path(doc_path).suffix)
    # JSONL is a special case of JSON
    if filetype == CompassFileType.Json and is_jsonl(doc_bytes):
        return CompassFileType.Jsonl
    elif filetype in SUPPORTED_DATASET_FILETYPES:
        return filetype

    return CompassFileType.Unsupported


def is_jsonl(filebytes: BytesIO) -> bool:
    """
    Returns True if the file is a JSONL file.
    :param filebytes: the file-like object
    :return: True if the file is a JSONL file, False otherwise
    """
    try:
        pd.read_json(filebytes, lines=True, nrows=2)
        return True
    except ValueError:
        return False


def read_excel_batches(filepath: str, chunksize: Optional[int] = None) -> Iterator[pd.DataFrame]:
    """
    Read an Excel file using the correct engine
    :param filepath: the path to the Excel file
    :param chunksize: the chunk size
    """
    skip = 0
    while True:
        df = pd.read_excel(filepath, skiprows=skip, nrows=chunksize)
        skip += chunksize if chunksize else len(df)
        if df.empty:
            break
        yield df


DATASET_LOAD_FUNCTIONS = {
    CompassFileType.Jsonl: pd.read_json,
    CompassFileType.Csv: pd.read_csv,
    CompassFileType.Tsv: pd.read_csv,
    CompassFileType.Dat: pd.read_csv,
    CompassFileType.Parquet: pq.ParquetFile,
    CompassFileType.Xls: pd.read_excel,
    CompassFileType.Xlsx: pd.read_excel,
}


# Functions that allow to load datasets in batches. Notice that some dataset formats,
# like JSON, can be loaded with the same Pandas function to return either the whole dataset or in batches.
BATCHED_DATASET_LOAD_FUNCTIONS = {
    CompassFileType.Jsonl: pd.read_json,
    CompassFileType.Csv: pd.read_csv,
    CompassFileType.Tsv: pd.read_csv,
    CompassFileType.Dat: pd.read_csv,
    CompassFileType.Parquet: pq.ParquetFile,
    CompassFileType.Xls: read_excel_batches,
    CompassFileType.Xlsx: read_excel_batches,
}


def load_dataset(filebytes: BytesIO, filepath: str, dataset_type: CompassFileType) -> Tuple[Iterator, List[str]]:
    """
    Load a dataset from a file using the appropriate function
    :param filebytes: the file-like object
    :param filepath: the path to the dataset
    :param dataset_type: the type of the dataset
    :return: a tuple with the dataset and the column names (if any)
    """
    load_func = DATASET_LOAD_FUNCTIONS.get(dataset_type, None)
    batched_load_func = BATCHED_DATASET_LOAD_FUNCTIONS.get(dataset_type, None)
    if load_func is None or batched_load_func is None:
        logger.error(f"Unknown dataset type for {filepath}")
        return iter([]), []

    load_args = {}
    if dataset_type == CompassFileType.Jsonl:
        load_args["lines"] = True
    elif dataset_type == CompassFileType.Tsv:
        load_args["sep"] = "\t"

    header, load_args = get_dataset_header(
        filebytes, dataset_type, load_func, load_args)

    # Parquet files are loaded with Pyarrow and need a different iterator. The rest are loaded with Pandas
    if dataset_type == CompassFileType.Parquet:
        batches_iterator = batched_load_func(filebytes).iter_batches(10000)
    else:
        batches_iterator = batched_load_func(
            filebytes, **(load_args | {"chunksize": 10000}))
    return batches_iterator, header


def get_dataset_header(
    filebytes: BytesIO, dataset_type: CompassFileType, load_func: Callable, load_args: Dict
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Extract the dataset header if it exists (only tabular data, i.e., not JSON). Otherwise, create a default one
    :param filebytes: the file-like object
    :param dataset_type: the type of the dataset
    :param load_func: the function to use to load the dataset
    :param load_args: the arguments to pass to the load
    :return: the dataset header
    """
    # JSON files do not have headers, and Parquet files always have headers
    header = []
    if dataset_type in {CompassFileType.Json, CompassFileType.Jsonl, CompassFileType.Parquet}:
        return header, load_args

    df = load_func(filebytes, **(load_args | {"header": None, "nrows": 10}))
    filebytes.seek(0)
    df_header = load_func(filebytes, **(load_args | {"nrows": 10}))
    filebytes.seek(0)
    if tuple(df.dtypes) != tuple(df_header.dtypes):
        header = df_header.columns.tolist()

    if not header:
        header = [f"Column {i}" for i in range(len(df.columns))]

    return header, load_args


def iter_batch_rows(batch_iterator: Any, dataset_type: CompassFileType) -> Iterator:
    """
    Iterate over the rows of a dataset
    :param batch_iterator: the batch iterator
    :param dataset_type: the type of the dataset
    :return: an iterator over the rows
    """
    if dataset_type == CompassFileType.Parquet:
        batch_iterator = batch_iterator.to_pandas()
    return batch_iterator.iterrows()


def load_nested_json(json_object: Union[Dict[str, Any], List, str]) -> Any:
    """
    Load a nested JSON and decode and navigate its nested JSON strings
    :param json_object: the JSON string
    :return: the loaded JSON
    """
    if isinstance(json_object, dict):
        return {k: load_nested_json(v) for k, v in json_object.items()}
    elif isinstance(json_object, list):
        return [load_nested_json(v) for v in json_object]
    elif isinstance(json_object, str):
        try:
            decoded_str = json.loads(json_object)
            return load_nested_json(decoded_str)
        except json.JSONDecodeError:
            pass
    return json_object


def get_content_type(file: BytesIO, filepath: str) -> str:
    """
    Wrapper function for Unstructured's detect_filetype to get a file's
    type and fix the bug when determining the content type for LibreOffice files.
    This function first attempts to determine the file type using the file-like object.
    If the file type is MSG but the file extension says otherwise, then it will attempt to
    determine the file type using the file's extension. Finally, it will return the content type
    based on the file type, which is eventually passed to Unstructured to override its buggy content type logic
    :param file: the file-like object
    :param filepath: the path to the file
    """
    filetype = detect_filetype(file=file) or FileType.EMPTY
    ext = Path(filepath).suffix.lower()

    # If the file type is unknown, then try to detect the file type using the file's extension.
    # If the file type is MSG but the file extension says otherwise, then use the file's extension.
    # This is a workaround for the bug in Unstructured's content type detection for LibreOffice files.
    if filetype == FileType.UNK or filetype == FileType.MSG and ext and not ext == ".msg":
        filetype = detect_filetype(filepath)

    # If the file is a valid JSON, we treat it as text because Unstructured does not support JSON content type
    filetype = FileType.TXT if filetype == FileType.JSON else filetype

    if filetype not in FILETYPES_TO_CONTENT_TYPES:
        return "application/octet-stream"

    return FILETYPES_TO_CONTENT_TYPES[filetype]


def generate_file_id(doc_text: str, is_dataset: bool, filepath: Optional[str] = None) -> str:
    """
    Generate a unique document id. If a document id is provided, then it is used right away.
    Otherwise, a document id is generated based on the document's text or file path. If a file path exists,
    a document id is generated based on:
    - the file path and text if it's a dataset
    - the file path if it's a regular document
    This prevents different documents coming out of the same dataset ending up having the same id.
    If a file path does not exist, a document id is generated based on the document's text.

    :param filepath: the path of the document
    :param doc_text: the text of the document
    :param is_dataset: whether the document is a dataset
    :return: a unique document id
    """
    if filepath:
        text = f"{filepath}_{doc_text}" if is_dataset else filepath
    else:
        text = doc_text
    return hashlib.sha256(text.encode()).hexdigest()
