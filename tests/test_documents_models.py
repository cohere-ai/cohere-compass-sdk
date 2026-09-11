import uuid

from cohere_compass.models.documents import AssetPresignedUrlDetails, AssetType, CompassDocumentChunkAsset
from cohere_compass.models.indexes import IndexDetails, ListIndexesResponse


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


def test_asset_type_accepts_unknown_values():
    asset = CompassDocumentChunkAsset.model_validate(
        {"asset_type": "future_asset", "content_type": "application/octet-stream"}
    )
    assert asset.asset_type == "future_asset"
    assert str(AssetType("future_asset")) == "future_asset"


def test_index_details_ignores_unknown_response_fields():
    details = IndexDetails.model_validate(
        {
            "name": "another",
            "count": 15,
            "parent_doc_count": 1,
            "dense_model": "embed-english-v3.0",
            "sparse_model": "sparse-multilingual-v1.0",
            "analyzer": "icu_analyzer",
            "dense_model_dims": 1024,
            "collection": "some-collection",
            "is_collection": False,
            "store_size_bytes": 825388,
            "primary_store_size_bytes": 412694,
            "primary_shard_count": 3,
            "replica_count": 1,
            "health": "green",
            "retention_policy": None,
        }
    )
    assert details.name == "another"
    assert not hasattr(details, "dense_model_dims")
    assert "dense_model_dims" not in details.model_dump()


def test_list_indexes_ignores_unknown_response_fields():
    response = ListIndexesResponse.model_validate(
        {
            "indexes": [
                {
                    "name": "another",
                    "count": 15,
                    "parent_doc_count": 1,
                    "dense_model_dims": 1024,
                    "collection": "x",
                    "is_collection": True,
                }
            ]
        }
    )
    assert response.indexes[0].name == "another"
    assert "collection" not in response.indexes[0].model_dump()
