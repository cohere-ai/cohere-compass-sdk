import uuid

from cohere_compass.models.documents import AssetPresignedUrlDetails


def test_presigned_url_defaults_to_empty_when_missing():
    asset_id = uuid.uuid4()
    details = AssetPresignedUrlDetails.model_validate({"document_id": "doc-1", "asset_id": asset_id})
    assert details.presigned_url == ""


def test_presigned_url_defaults_to_empty_when_none():
    asset_id = uuid.uuid4()
    details = AssetPresignedUrlDetails.model_validate(
        {"document_id": "doc-1", "asset_id": asset_id, "presigned_url": None}
    )
    assert details.presigned_url == ""


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
