import json
import uuid
from typing import Any

import pytest
import pytest_asyncio
from aioresponses import aioresponses

from cohere_compass import GroupAuthorizationActions, GroupAuthorizationInput
from cohere_compass.clients.compass_async import CompassAsyncClient
from cohere_compass.exceptions import (
    CompassAuthError,
    CompassClientError,
    CompassError,
    CompassTimeoutError,
)
from cohere_compass.models import SearchFilter
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.documents import (
    AssetPresignedUrlRequest,
    AssetType,
    ContentTypeEnum,
    DocumentAttributes,
    UploadDocumentsResult,
)
from cohere_compass.models.indexes import IndexDetails, IndexInfo
from cohere_compass.models.search import SortBy

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def client():
    c = CompassAsyncClient(index_url="http://test.com")
    try:
        yield c
    finally:
        await c.aclose()


async def test_delete_url_formatted_with_doc_and_index(client: CompassAsyncClient):
    with aioresponses() as m:
        m.delete("http://test.com/v1/indexes/test_index/documents/test_id", status=201)
        await client.delete_document(index_name="test_index", document_id="test_id")


async def test_create_index_formatted_with_index(client: CompassAsyncClient):
    with aioresponses() as m:
        m.put("http://test.com/v1/indexes/test_index", status=200)
        await client.create_index(index_name="test_index")


async def test_create_index_with_index_config(client: CompassAsyncClient):
    with aioresponses() as m:
        m.put("http://test.com/v1/indexes/test_index", status=200)
        await client.create_index(index_name="test_index", index_config=IndexConfig(number_of_shards=5))

        request = next(iter(m.requests.values()))[0]
        assert request.kwargs["json"] == {"number_of_shards": 5}


async def test_create_index_400s_propagated_to_caller(client: CompassAsyncClient):
    with aioresponses() as m:
        m.put(
            "http://test.com/v1/indexes/test-index",
            status=400,
            body=json.dumps({"error": "invalid request"}),
        )
        with pytest.raises(CompassError, match=r'Client error 400: \{"error": "invalid request"\}'):
            await client.create_index(index_name="test-index")


async def test_get_document_is_valid(client: CompassAsyncClient):
    response_body: dict[str, Any] = {
        "document": {
            "document_id": "test-document-id",
            "path": "test-path",
            "parent_document_id": "test-parent-document-id",
            "content": {"field-1": "value-1", "field-2": "value-2"},
            "index_fields": ["field-1", "field-2"],
            "authorized_groups": ["group-1", "group-2"],
            "chunks": [
                {
                    "chunk_id": "test-chunk-id",
                    "sort_id": 1,
                    "parent_document_id": "test-parent-document-id",
                    "path": "test-path",
                    "content": {"field-1": "value-1", "field-2": "value-2"},
                    "origin": {"field-1": "value-1", "field-2": "value-2"},
                    "assets_info": [
                        {
                            "asset_id": "test-asset-id",
                            "asset_type": AssetType.PAGE_IMAGE,
                            "content_type": "test-content-type",
                            "presigned_url": "test-presigned-url",
                        },
                        {
                            "asset_id": "test-asset-id",
                            "asset_type": AssetType.PAGE_MARKDOWN,
                            "content_type": "test-content-type",
                            "presigned_url": "test-presigned-url",
                        },
                    ],
                }
            ],
        }
    }
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes/test_index/documents/test_id",
            status=200,
            payload=response_body,
        )
        document = await client.get_document(index_name="test_index", document_id="test_id")

    assert document.document_id == "test-document-id"
    assert document.path == "test-path"
    assert document.parent_document_id == "test-parent-document-id"
    assert document.content == {"field-1": "value-1", "field-2": "value-2"}
    assert document.index_fields == ["field-1", "field-2"]
    assert document.authorized_groups == ["group-1", "group-2"]
    assert document.chunks[0].chunk_id == "test-chunk-id"
    assert document.chunks[0].assets_info is not None
    assert len(document.chunks[0].assets_info) == 2


async def test_list_indexes(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes",
            status=200,
            payload={"indexes": [{"name": "test_index", "count": 1, "parent_doc_count": 1}]},
        )
        result = await client.list_indexes()

    assert result.indexes == [IndexInfo(name="test_index", count=1, parent_doc_count=1)]


async def test_refresh_is_valid(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post("http://test.com/v1/indexes/test_index/_refresh", status=200)
        await client.refresh_index(index_name="test_index")


async def test_add_attributes_is_valid(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/test_id/_add_attributes",
            status=200,
        )
        attrs = DocumentAttributes()
        attrs.fake = "context"
        await client.add_attributes(
            index_name="test_index",
            document_id="test_id",
            attributes=attrs,
        )

        request = next(iter(m.requests.values()))[0]
        assert request.kwargs["json"] == {"fake": "context"}


async def test_get_document_asset_with_json_asset(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id",
            status=200,
            payload={"test": "test"},
            headers={"Content-Type": "application/json"},
        )
        asset, content_type = await client.get_document_asset(
            index_name="test_index", document_id="test_id", asset_id="test_asset_id"
        )

    assert isinstance(asset, dict)
    assert asset == {"test": "test"}
    assert content_type == "application/json"


async def test_get_document_asset_markdown(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id",
            status=200,
            body="# Test",
            headers={"Content-Type": "text/markdown"},
        )
        asset, content_type = await client.get_document_asset(
            index_name="test_index", document_id="test_id", asset_id="test_asset_id"
        )

    assert isinstance(asset, str)
    assert asset == "# Test"
    assert content_type == "text/markdown"


async def test_get_document_asset_image(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id",
            status=200,
            body=b"test",
            headers={"Content-Type": "image/png"},
        )
        asset, content_type = await client.get_document_asset(
            index_name="test_index", document_id="test_id", asset_id="test_asset_id"
        )

    assert isinstance(asset, bytes)
    assert asset == b"test"
    assert content_type == "image/png"


async def test_direct_search(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/_direct_search",
            status=200,
            payload={"hits": [], "scroll_id": "test_scroll_id"},
        )
        await client.direct_search(index_name="test_index", query={"match_all": {}})

        request = next(iter(m.requests.values()))[0]
        sent = request.kwargs["json"]
        assert "query" in sent
        assert "size" in sent


async def test_direct_search_scroll(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/_direct_search/scroll",
            status=200,
            payload={"hits": [], "scroll_id": "test_scroll_id"},
        )
        await client.direct_search_scroll(scroll_id="test_scroll_id", index_name="test_index", scroll="5m")

        request = next(iter(m.requests.values()))[0]
        sent = request.kwargs["json"]
        assert sent["scroll_id"] == "test_scroll_id"
        assert sent["scroll"] == "5m"


async def test_direct_search_with_sort_by(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/_direct_search",
            status=200,
            payload={"hits": [], "scroll_id": "test_scroll_id"},
        )
        sort_by = [SortBy(field="created_at", order="desc")]
        await client.direct_search(index_name="test_index", query={"match_all": {}}, sort_by=sort_by)

        request = next(iter(m.requests.values()))[0]
        payload = request.kwargs["json"]
        assert payload["sort_by"] == [{"field": "created_at", "order": "desc"}]


async def test_get_models(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/config/models",
            status=200,
            payload={
                "dense": ["embed-english-v3.0"],
                "sparse": ["sparse-v1.0"],
                "rerank": ["rerank-v3.5"],
            },
        )
        result = await client.get_models()

    assert result == {
        "dense": ["embed-english-v3.0"],
        "sparse": ["sparse-v1.0"],
        "rerank": ["rerank-v3.5"],
    }


async def test_get_index_details(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes/test_index",
            status=200,
            payload={
                "name": "test_index",
                "count": 10,
                "parent_doc_count": 20,
                "dense_model": "embed-english-v3.0",
                "sparse_model": "sparse-v1.0",
                "analyzer": "english",
                "store_size_bytes": 1048576,
                "primary_store_size_bytes": 524288,
                "primary_shard_count": 5,
                "replica_count": 1,
                "health": "green",
            },
        )
        result = await client.get_index_details(index_name="test_index")

    assert result == IndexDetails(
        name="test_index",
        count=10,
        parent_doc_count=20,
        dense_model="embed-english-v3.0",
        sparse_model="sparse-v1.0",
        analyzer="english",
        store_size_bytes=1048576,
        primary_store_size_bytes=524288,
        primary_shard_count=5,
        replica_count=1,
        health="green",
    )


async def test_upload_document(client: CompassAsyncClient):
    upload_id = uuid.uuid4()
    document_id = "test_document_id"

    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/upload",
            status=200,
            payload={"upload_id": str(upload_id), "document_ids": [document_id]},
        )
        result = await client.upload_document(
            index_name="test_index",
            filename="test.pdf",
            filebytes=b"test content",
            content_type=ContentTypeEnum.ApplicationPdf,
            document_id=document_id,
        )

    assert result == UploadDocumentsResult(upload_id=upload_id, document_ids=[document_id])


async def test_upload_document_with_authorized_groups(client: CompassAsyncClient):
    upload_id = uuid.uuid4()
    document_id = "test_document_id"

    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/upload",
            status=200,
            payload={"upload_id": str(upload_id), "document_ids": [document_id]},
        )
        await client.upload_document(
            index_name="test_index",
            filename="test.pdf",
            filebytes=b"test content",
            content_type=ContentTypeEnum.ApplicationPdf,
            document_id=document_id,
            authorized_groups=["group1", "group2"],
        )

        request = next(iter(m.requests.values()))[0]
        sent = request.kwargs["json"]
        assert sent["authorized_groups"] == ["group1", "group2"]


async def test_upload_document_status(client: CompassAsyncClient):
    upload_id = uuid.uuid4()

    with aioresponses() as m:
        m.get(
            f"http://test.com/v1/indexes/test_index/documents/upload/{upload_id}",
            status=200,
            payload=[
                {
                    "upload_id": str(upload_id),
                    "document_id": "doc123",
                    "destinations": ["destination1"],
                    "file_name": "test.pdf",
                    "state": "completed",
                    "last_error": None,
                    "parsed_presigned_url": "test-presigned-url",
                }
            ],
        )
        result = await client.upload_document_status(index_name="test_index", upload_id=upload_id)

    assert result is not None
    assert result[0].upload_id == upload_id
    assert result[0].document_id == "doc123"


async def test_bulk_upload_document_status(client: CompassAsyncClient):
    upload_id_1 = uuid.uuid4()
    upload_id_2 = uuid.uuid4()

    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/uploads",
            status=200,
            payload=[
                {
                    "upload_id": str(upload_id_1),
                    "statuses": [
                        {
                            "upload_id": str(upload_id_1),
                            "document_id": "doc-abc",
                            "destinations": ["test_index"],
                            "file_name": "report.pdf",
                            "state": "COMPLETED",
                            "last_error": None,
                            "parsed_presigned_url": "https://presigned-url",
                        }
                    ],
                },
                {"upload_id": str(upload_id_2), "statuses": []},
            ],
        )
        result = await client.bulk_upload_document_status(
            index_name="test_index", upload_ids=[upload_id_1, upload_id_2]
        )

        request = next(iter(m.requests.values()))[0]
        body = request.kwargs["json"]
        assert body == {"upload_ids": [str(upload_id_1), str(upload_id_2)]}

    assert len(result) == 2
    assert result[0].upload_id == upload_id_1
    assert result[1].upload_id == upload_id_2


async def test_search_documents(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/_search",
            status=200,
            payload={
                "hits": [
                    {
                        "document_id": "doc1",
                        "score": 0.9,
                        "content": {"text": "test"},
                        "path": "test1.pdf",
                        "parent_document_id": "parent1",
                        "chunks": [],
                    }
                ]
            },
        )
        result = await client.search_documents(index_name="test_index", query="test query", top_k=10)

    assert len(result.hits) == 1
    assert result.hits[0].score == 0.9


async def test_search_documents_with_filters(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/_search",
            status=200,
            payload={
                "hits": [
                    {
                        "document_id": "doc1",
                        "score": 0.9,
                        "content": {"department": "engineering"},
                        "path": "doc1.pdf",
                        "parent_document_id": "parent1",
                        "chunks": [],
                    }
                ]
            },
        )
        search_filter = SearchFilter(field="department", type=SearchFilter.FilterType.EQ, value="engineering")
        result = await client.search_documents(
            index_name="test_index", query="test query", top_k=10, filters=[search_filter]
        )

        request = next(iter(m.requests.values()))[0]
        payload = request.kwargs["json"]
        assert "filters" in payload

    assert result.hits[0].document_id == "doc1"


async def test_search_chunks(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/documents/_search_chunks",
            status=200,
            payload={
                "hits": [
                    {
                        "chunk_id": "chunk1",
                        "score": 0.95,
                        "text": "chunk text 1",
                        "content": {},
                        "parent_document_id": "doc1",
                        "document_id": "chunk1",
                        "sort_id": 0,
                        "path": "test.pdf",
                    }
                ]
            },
        )
        result = await client.search_chunks(index_name="test_index", query="test query", top_k=5)

    assert len(result.hits) == 1
    assert result.hits[0].score == 0.95


async def test_update_group_authorization(client: CompassAsyncClient):
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/group_authorization",
            status=200,
            payload={
                "results": [
                    {"document_id": "doc1", "error": None},
                    {"document_id": "doc2", "error": "test error"},
                ]
            },
        )
        group_auth = GroupAuthorizationInput(
            document_ids=["doc1", "doc2"],
            authorized_groups=["group1", "group2"],
            action=GroupAuthorizationActions.ADD,
        )
        result = await client.update_group_authorization(index_name="test_index", group_auth_input=group_auth)

    assert result.results[0].document_id == "doc1"
    assert result.results[0].error is None
    assert result.results[1].document_id == "doc2"
    assert result.results[1].error == "test error"


async def test_authentication_error_401(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes",
            status=401,
            body=json.dumps({"error": "Unauthorized"}),
        )
        with pytest.raises(CompassAuthError, match=r"Unauthorized. Please check your bearer token."):
            await client.list_indexes()


async def test_client_error_404(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes/nonexistent",
            status=404,
            body=json.dumps({"error": "Index not found"}),
        )
        with pytest.raises(CompassClientError, match=r'Client error 404: \{"error": "Index not found"\}'):
            await client.get_index_details(index_name="nonexistent")


async def test_server_error_500(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes",
            status=500,
            body=json.dumps({"error": "Internal server error"}),
            repeat=True,
        )
        with pytest.raises(CompassError, match=r'Server error 500: \{"error": "Internal server error"\}'):
            await client.list_indexes()


async def test_timeout_handling(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/indexes",
            exception=TimeoutError("Request timeout"),
            repeat=True,
        )
        with pytest.raises(CompassTimeoutError, match="Timeout error: Request timeout"):
            await client.list_indexes()


async def test_compass_client_initialization():
    c = CompassAsyncClient(index_url="http://test.com")
    try:
        assert c.index_url == "http://test.com/"
    finally:
        await c.aclose()

    c_with_auth = CompassAsyncClient(index_url="http://test.com", bearer_token="test_token")
    try:
        assert c_with_auth.index_url == "http://test.com/"
        assert c_with_auth.bearer_token == "test_token"
    finally:
        await c_with_auth.aclose()


async def test_compass_client_close(client: CompassAsyncClient):
    await client.aclose()


async def test_get_asset_presigned_urls_success(client: CompassAsyncClient):
    asset_a_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    asset_b_uuid = uuid.UUID("87654321-4321-8765-4321-876543218765")
    with aioresponses() as m:
        m.post(
            "http://test.com/v1/indexes/test_index/assets/_presigned_urls",
            status=200,
            payload={
                "asset_urls": [
                    {
                        "document_id": "doc_123",
                        "asset_id": str(asset_a_uuid),
                        "presigned_url": "https://example.com/asset_A?X-Amz-Signature=...",
                    },
                    {
                        "document_id": "doc_456",
                        "asset_id": str(asset_b_uuid),
                        "presigned_url": "https://example.com/asset_B?X-Amz-Signature=...",
                    },
                ]
            },
        )
        result = await client.get_asset_presigned_urls(
            index_name="test_index",
            assets=[
                AssetPresignedUrlRequest(document_id="doc_123", asset_id=asset_a_uuid),
                AssetPresignedUrlRequest(document_id="doc_456", asset_id=asset_b_uuid),
            ],
        )

    assert len(result) == 2
    assert result[0].asset_id == asset_a_uuid
    assert result[1].asset_id == asset_b_uuid


async def test_get_task_is_valid(client: CompassAsyncClient):
    with aioresponses() as m:
        m.get(
            "http://test.com/v1/tasks/test_task_id",
            status=200,
            payload={
                "task_id": "test_task_id",
                "status": "completed",
                "result": {"key": "value"},
            },
        )
        result = await client.get_task(task_id="test_task_id")

    assert result["task_id"] == "test_task_id"
    assert result["status"] == "completed"
