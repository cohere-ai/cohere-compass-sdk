"""
Utility functions for the Cohere Compass SDK.

This module provides utility functions for parallel processing, filesystem abstraction,
document loading, and other common operations used throughout the SDK.
"""

# Python imports
import base64
import logging
import uuid
from collections.abc import AsyncIterable, Iterable

# 3rd party imports
# Local imports
from cohere_compass.constants import UUID_NAMESPACE
from cohere_compass.models import (
    CompassDocument,
)
from cohere_compass.models.documents import Chunk, CompassDocumentStatus, Document

logger = logging.getLogger(__name__)


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
    docs: Iterable[CompassDocument | tuple[str, Exception]],
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
        if isinstance(doc, tuple):
            # If the doc is a tuple, it means there was an error during parsing
            filename, error = doc
            logger.error(f"Skipping, error processing document {filename}: {error}")
            continue
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
    docs: AsyncIterable[CompassDocument | tuple[str, Exception]],
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
        if isinstance(doc, tuple):
            # If the doc is a tuple, it means there was an error during parsing
            filename, error = doc
            logger.error(f"Error processing document {filename}: {error}")
            continue
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
