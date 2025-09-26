import asyncio
import base64
import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cohere_compass.constants import UUID_NAMESPACE
from cohere_compass.models import (
    CompassDocument,
    CompassSdkStage,
)
from cohere_compass.models.documents import Chunk, Document
from cohere_compass.utils import (
    async_apply,
    async_map,
    generate_doc_id_from_bytes,
    imap_parallel,
    open_document,
    partition_documents,
    scan_folder,
)
from tests.utils import create_test_doc


def test_imap_queued_basic_functionality():
    def square(x: int) -> int:
        return x * x

    executor = ThreadPoolExecutor(max_workers=2)
    results = list(imap_parallel(executor, square, range(5), max_parallelism=2))
    executor.shutdown()

    assert sorted(results) == [0, 1, 4, 9, 16]


def test_imap_queued_with_exception_handling():
    def may_fail(x: int) -> int:
        if x == 2:
            raise ValueError("Test error")
        return x * 2

    executor = ThreadPoolExecutor(max_workers=2)
    results = list(imap_parallel(executor, may_fail, range(4), max_parallelism=2))
    executor.shutdown()

    # Should get results for all except the failed one
    assert len(results) == 3
    assert 0 in results  # 0 * 2
    assert 2 in results  # 1 * 2
    assert 6 in results  # 3 * 2


def test_imap_queued_max_queued_limit():
    call_count = 0

    def track_calls(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x

    executor = ThreadPoolExecutor(max_workers=2)
    results = list(imap_parallel(executor, track_calls, range(10), max_parallelism=3))
    executor.shutdown()

    assert len(results) == 10
    assert call_count == 10


def test_empty_iterable():
    executor = ThreadPoolExecutor(max_workers=2)
    empty_iterable: list[int] = []
    results = list(
        imap_parallel(executor, lambda x: x, empty_iterable, max_parallelism=2)
    )
    executor.shutdown()

    assert results == []


@pytest.mark.asyncio
async def test_async_map_basic_functionality():
    async def square(x: int) -> int:
        return x * x

    results = await async_map(square, range(5))
    assert results == [0, 1, 4, 9, 16]


@pytest.mark.asyncio
async def test_async_map_with_limit():
    call_times: list[int] = []

    async def track_time(x: int) -> int:
        call_times.append(x)
        await asyncio.sleep(0.01)
        return x * 2

    results = await async_map(track_time, range(5), limit=2)

    assert results == [0, 2, 4, 6, 8]
    assert len(call_times) == 5


@pytest.mark.asyncio
async def test_async_map_empty_iterable():
    async def identity(x: int) -> int:
        return x

    results = await async_map(identity, [])
    assert results == []


@pytest.mark.asyncio
async def test_async_map_preserves_order():
    async def delayed_return(x: int) -> int:
        # Delay inversely proportional to x to test order preservation
        await asyncio.sleep((10 - x) * 0.001)
        return x

    results = await async_map(delayed_return, range(10), limit=5)
    assert results == list(range(10))


@pytest.mark.asyncio
async def test_async_map_without_limit():
    async def identity(x: int) -> int:
        return x

    results = await async_map(identity, range(100))
    assert results == list(range(100))


@pytest.mark.asyncio
async def test_async_apply_basic_functionality():
    results: list[int] = []

    async def append_to_results(x: int) -> None:
        results.append(x)

    await async_apply(append_to_results, range(5))
    assert sorted(results) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_async_apply_with_limit():
    results: list[int] = []

    async def append_with_delay(x: int) -> None:
        await asyncio.sleep(0.01)
        results.append(x)

    await async_apply(append_with_delay, range(5), limit=2)
    assert sorted(results) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_async_apply_empty_iterable():
    results: list[int] = []

    async def append_to_results(x: int) -> None:
        results.append(x)

    await async_apply(append_to_results, [])
    assert results == []


@patch("cohere_compass.utils.get_fs")
def test_open_document_successful_open(mock_get_fs: Any):
    mock_fs = MagicMock()
    mock_file = MagicMock()
    mock_file.read.return_value = b"test content"
    mock_fs.open.return_value.__enter__.return_value = mock_file
    mock_get_fs.return_value = mock_fs

    result = open_document("/path/to/file.txt")

    assert isinstance(result, CompassDocument)
    assert result.filebytes == b"test content"
    assert result.metadata.filename == "/path/to/file.txt"
    assert result.errors == []


@patch("cohere_compass.utils.get_fs")
def test_open_document_with_error(mock_get_fs: Any):
    mock_get_fs.side_effect = Exception("File not found")

    result = open_document("/path/to/nonexistent.txt")

    assert isinstance(result, CompassDocument)
    assert result.filebytes == b""
    assert result.metadata.filename == "/path/to/nonexistent.txt"
    assert len(result.errors) == 1
    assert CompassSdkStage.Parsing in result.errors[0]


@patch("cohere_compass.utils.get_fs")
def test_open_document_non_bytes_content(mock_get_fs: Any):
    mock_fs = MagicMock()
    mock_file = MagicMock()
    mock_file.read.return_value = "not bytes"  # Wrong type
    mock_fs.open.return_value.__enter__.return_value = mock_file
    mock_get_fs.return_value = mock_fs

    result = open_document("/path/to/file.txt")

    assert isinstance(result, CompassDocument)
    assert result.filebytes == b""
    assert len(result.errors) == 1
    assert "Expected bytes" in str(result.errors[0][CompassSdkStage.Parsing])


def test_scan_folder_local_folder():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files
        open(os.path.join(tmpdir, "test1.txt"), "w").close()
        open(os.path.join(tmpdir, "test2.pdf"), "w").close()
        open(os.path.join(tmpdir, "test3.doc"), "w").close()

        # Create subdirectory with file
        subdir = os.path.join(tmpdir, "subdir")
        os.makedirs(subdir)
        open(os.path.join(subdir, "test4.txt"), "w").close()

        # Test without recursion
        files = scan_folder(tmpdir, allowed_extensions=["txt"])
        assert len(files) == 1
        assert any("test1.txt" in f for f in files)

        # Test with recursion
        files = scan_folder(tmpdir, allowed_extensions=["txt"], recursive=True)
        assert len(files) == 2
        assert any("test1.txt" in f for f in files)
        assert any("test4.txt" in f for f in files)

        # Test with multiple extensions
        files = scan_folder(tmpdir, allowed_extensions=["txt", "pdf"])
        assert len(files) == 2

        # Test with no extension filter
        files = scan_folder(tmpdir)
        assert len(files) >= 3  # At least our 3 files in root


@patch("cohere_compass.utils.get_fs")
def test_scan_folder_remote_folder(mock_get_fs: Any):
    mock_fs = MagicMock()
    mock_fs.glob.return_value = [
        "bucket/file1.txt",
        "bucket/file2.txt",
        "bucket/subdir/file3.txt",
    ]
    mock_get_fs.return_value = mock_fs

    files = scan_folder("s3://bucket", allowed_extensions=["txt"])

    assert len(files) == 3
    assert all(f.startswith("s3://") for f in files)


@patch("cohere_compass.utils.get_fs")
def test_scan_folder_with_extension_normalization(mock_get_fs: Any):
    mock_fs = MagicMock()
    mock_fs.glob.return_value = ["file1.txt", "file2.txt"]
    mock_get_fs.return_value = mock_fs

    # Test with and without dot prefix
    files1 = scan_folder("/path", allowed_extensions=["txt"])
    files2 = scan_folder("/path", allowed_extensions=[".txt"])

    assert len(files1) == 2
    assert len(files2) == 2


def test_consistent_uuid_generation():
    test_bytes = b"test content"
    uuid1 = generate_doc_id_from_bytes(test_bytes)
    uuid2 = generate_doc_id_from_bytes(test_bytes)

    assert uuid1 == uuid2
    assert isinstance(uuid1, uuid.UUID)


def test_different_bytes_different_uuid():
    bytes1 = b"content 1"
    bytes2 = b"content 2"

    uuid1 = generate_doc_id_from_bytes(bytes1)
    uuid2 = generate_doc_id_from_bytes(bytes2)

    assert uuid1 != uuid2


def test_uses_correct_namespace():
    test_bytes = b"test"
    b64_string = base64.b64encode(test_bytes).decode("utf-8")
    namespace = uuid.UUID(UUID_NAMESPACE)

    expected_uuid = uuid.uuid5(namespace, b64_string)
    actual_uuid = generate_doc_id_from_bytes(test_bytes)

    assert actual_uuid == expected_uuid


def test_empty_bytes():
    empty_bytes = b""
    result = generate_doc_id_from_bytes(empty_bytes)

    assert isinstance(result, uuid.UUID)


def test_partition_large_batch():
    docs = [
        create_test_doc("doc1", 5),
        create_test_doc("doc2", 6),  # Total 11 chunks, exceeds max of 10
        create_test_doc("doc3", 2),
    ]

    partitions = list(partition_documents(docs, max_chunks_per_request=10))

    assert len(partitions) == 2

    # First partition should have doc1 only (5 chunks)
    request_block1, errors1 = partitions[0]
    assert len(request_block1) == 1
    assert len(errors1) == 0

    # Second partition should have doc2 and doc3
    request_block2, errors2 = partitions[1]
    assert len(request_block2) == 2
    assert len(errors2) == 0


def test_partition_with_errors():
    docs = [
        create_test_doc("doc1", 2),
        create_test_doc("doc2", 3, has_errors=True),
        create_test_doc("doc3", 2),
    ]

    partitions = list(partition_documents(docs, max_chunks_per_request=10))

    assert len(partitions) == 1
    request_block, errors = partitions[0]
    assert len(request_block) == 2  # Only successful docs
    assert len(errors) == 1  # One error from doc2
    assert "doc2" in str(errors[0])


def test_partition_empty_input():
    partitions = list(partition_documents([], max_chunks_per_request=10))
    assert len(partitions) == 0


def test_partition_all_errors():
    docs = [
        create_test_doc("doc1", 0, has_errors=True),
        create_test_doc("doc2", 0, has_errors=True),
    ]

    partitions = list(partition_documents(docs, max_chunks_per_request=10))

    assert len(partitions) == 1
    request_block, errors = partitions[0]
    assert len(request_block) == 0
    assert len(errors) == 2


def test_partition_creates_correct_document_objects():
    doc = create_test_doc("doc1", 2)
    docs = [doc]

    partitions = list(partition_documents(docs, max_chunks_per_request=10))
    request_block, _ = partitions[0]

    compass_doc, document = request_block[0]
    assert compass_doc == doc
    assert isinstance(document, Document)
    assert document.document_id == "doc1"
    assert document.parent_document_id == "parent_doc1"
    assert document.path == "doc1.txt"
    assert len(document.chunks) == 2
    assert all(isinstance(chunk, Chunk) for chunk in document.chunks)


def test_partition_exact_boundary():
    # Test when chunks exactly match max_chunks_per_request
    docs = [
        create_test_doc("doc1", 5),
        create_test_doc("doc2", 5),  # Exactly 10 chunks total
    ]

    partitions = list(partition_documents(docs, max_chunks_per_request=10))

    assert len(partitions) == 1
    request_block, errors = partitions[0]
    assert len(request_block) == 2
    assert len(errors) == 0
