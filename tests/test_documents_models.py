import uuid

import pytest
from pydantic import ValidationError

from cohere_compass.models.documents import (
    AssetPresignedUrlDetails,
    AssetType,
    CompassDocument,
    CompassDocumentChunk,
    CompassDocumentMetadata,
)


def test_presigned_url_is_required_on_asset_presigned_url_details():
    with pytest.raises(ValidationError):
        AssetPresignedUrlDetails.model_validate({"document_id": "doc-1", "asset_id": uuid.uuid4()})


def test_presigned_url_cannot_be_null_on_asset_presigned_url_details():
    with pytest.raises(ValidationError):
        AssetPresignedUrlDetails.model_validate(
            {"document_id": "doc-1", "asset_id": uuid.uuid4(), "presigned_url": None}
        )


def test_presigned_url_preserved_when_present():
    asset_id = uuid.uuid4()
    details = AssetPresignedUrlDetails.model_validate(
        {
            "document_id": "doc-1",
            "asset_id": asset_id,
            "presigned_url": "https://example.com/asset",
        }
    )
    assert details.presigned_url == "https://example.com/asset"


def test_compass_document_validates_minimal_parser_payload():
    doc = CompassDocument.model_validate(
        {
            "metadata": {
                "document_id": "doc-1",
                "filename": "note.txt",
                "meta": {"author": "ada"},
            },
            "content": {"text": "hello"},
            "chunks": [
                {
                    "sort_id": 0,
                    "content": {"text": "hello"},
                    "chunk_id": "ignored",
                    "document_id": "ignored",
                    "parent_document_id": "ignored",
                    "path": "ignored",
                }
            ],
        }
    )
    assert doc.metadata.document_id == "doc-1"
    assert doc.metadata.meta == {"author": "ada"}
    assert doc.chunks[0].sort_id == 0
    assert not hasattr(doc.chunks[0], "chunk_id") or "chunk_id" not in doc.chunks[0].model_fields


def test_to_index_document_fills_required_write_fields():
    doc = CompassDocument(
        metadata=CompassDocumentMetadata(document_id="doc-1", filename="note.txt"),
        content={"text": "hello"},
        chunks=[CompassDocumentChunk(sort_id=0, content={"text": "hello"})],
    )
    written = doc.to_index_document()
    assert written.document_id == "doc-1"
    assert written.parent_document_id == "doc-1"
    assert written.path == "note.txt"
    assert written.chunks[0].chunk_id == "doc-1_0"
    assert written.chunks[0].path == "note.txt"
    assert written.chunks[0].sort_id == 0


def test_asset_type_accepts_unknown_values():
    asset_type = AssetType("future_asset")
    assert asset_type == "future_asset"
    assert str(asset_type) == "future_asset"
