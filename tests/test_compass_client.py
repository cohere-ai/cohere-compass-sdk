import json
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any, Literal

import httpx
import pytest
import respx
from pydantic import ValidationError
from respx import MockRouter

from cohere_compass import GroupAuthorizationActions, GroupAuthorizationInput
from cohere_compass.clients import CompassClient
from cohere_compass.exceptions import CompassError
from cohere_compass.models import (
    CompassDocument,
    CreateDataSource,
    DataSource,
    SearchFilter,
)
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.datasources import AzureBlobStorageConfig
from cohere_compass.models.documents import (
    AssetType,
    CompassDocumentMetadata,
    ContentTypeEnum,
    DocumentAttributes,
    UploadDocumentsResult,
)
from cohere_compass.models.indexes import IndexInfo
from cohere_compass.models.search import (
    SortBy,
)

HTTPMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def mock_endpoint(
    method: HTTPMethod,
    url: str,
    status_code: int = 200,
    response_body: Any = None,
    expected_request_body: dict[Any, Any] | None = None,
):
    def decorator(test_func: Callable[..., Any]) -> Callable[..., Any]:
        @respx.mock(assert_all_mocked=True)
        def wrapper(respx_mock: MockRouter, *args: Any, **kwargs: Any):
            route = getattr(respx_mock, method.lower())(url).mock(
                return_value=httpx.Response(status_code, json=response_body)
            )

            test_func(*args, **kwargs)

            assert route.called, f"Expected {method} {url} to be called"
            assert route.call_count == 1, f"Expected {method} {url} to be called once"

            if expected_request_body is not None:
                request_body = json.loads(route.calls.last.request.content)
                assert request_body == expected_request_body, (
                    f"Expected JSON body {expected_request_body}, got {request_body}"
                )

        return wrapper  # type: ignore

    return decorator


@mock_endpoint(
    "DELETE",
    "http://test.com/v1/indexes/test_index/documents/test_id",
    201,
)
def test_delete_url_formatted_with_doc_and_index():
    # Running...
    compass = CompassClient(index_url="http://test.com")
    compass.delete_document(index_name="test_index", document_id="test_id")


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index",
    200,
)
def test_create_index_formatted_with_index():
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(index_name="test_index")


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index",
    200,
    {"number_of_shards": 5},
)
def test_create_index_with_index_config():
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(
        index_name="test_index", index_config=IndexConfig(number_of_shards=5)
    )


@respx.mock
def test_create_index_with_invalid_name(respx_mock: MockRouter):
    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(ValueError) as exc_info:
        compass.create_index(index_name="there/are/slashes/here")
    assert "Invalid index name" in str(exc_info.value)


@respx.mock
def test_create_index_400s_propagated_to_caller(respx_mock: MockRouter):
    respx_mock.put("http://test.com/v1/indexes/test-index").mock(
        return_value=httpx.Response(400, json={"error": "invalid request"})
    )
    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(CompassError, match="Failed to send request"):
        compass.create_index(index_name="test-index")


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index/documents",
    200,
    {
        "documents": [
            {
                "chunks": [],
                "content": {},
                "document_id": "",
                "index_fields": [],
                "parent_document_id": "",
                "path": "",
            }
        ],
        "merge_groups_on_conflict": False,
    },
)
def test_put_documents_payload_and_url_exist():
    compass = CompassClient(index_url="http://test.com")
    compass.insert_docs(index_name="test_index", docs=iter([CompassDocument()]))


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index/documents",
    200,
    {
        "documents": [
            {
                "chunks": [],
                "content": {},
                "document_id": "",
                "index_fields": [],
                "parent_document_id": "",
                "path": "",
            }
        ],
        "merge_groups_on_conflict": False,
    },
)
def test_put_document_payload_and_url_exist():
    compass = CompassClient(index_url="http://test.com")
    compass.insert_doc(index_name="test_index", doc=CompassDocument())


@respx.mock(assert_all_mocked=True)
def test_put_document_payload_with_invalid_document_id(respx_mock: MockRouter):
    doc = CompassDocument(
        filebytes=b"",
        metadata=CompassDocumentMetadata(
            document_id="bypass-validation", filename="tests/docs/sample.doc"
        ),
    )
    doc.metadata.document_id = "something/with/slashes"
    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(ValidationError) as exc_info:
        compass.insert_doc(index_name="test_index", doc=doc)
        assert "String should match pattern" in str(exc_info)


@mock_endpoint(
    "GET",
    "http://test.com/v1/indexes",
    200,
    response_body={
        "indexes": [
            {
                "name": "test_index",
                "count": 1,
                "parent_doc_count": 1,
            }
        ]
    },
)
def test_list_indices_is_valid():
    compass = CompassClient(index_url="http://test.com")
    response = compass.list_indexes()
    assert response.indexes == [
        IndexInfo(name="test_index", count=1, parent_doc_count=1)
    ]


@mock_endpoint(
    "GET",
    "http://test.com/v1/indexes/test_index/documents/test_id",
    200,
    response_body={
        "document": {
            "document_id": "test-document-id",
            "path": "test-path",
            "parent_document_id": "test-parent-document-id",
            "content": {
                "field-1": "value-1",
                "field-2": "value-2",
            },
            "index_fields": ["field-1", "field-2"],
            "authorized_groups": ["group-1", "group-2"],
            "chunks": [
                {
                    "chunk_id": "test-chunk-id",
                    "sort_id": 1,
                    "parent_document_id": "test-parent-document-id",
                    "content": {
                        "field-1": "value-1",
                        "field-2": "value-2",
                    },
                    "origin": {
                        "field-1": "value-1",
                        "field-2": "value-2",
                    },
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
    },
)
def test_get_document_is_valid():
    compass = CompassClient(index_url="http://test.com")
    document = compass.get_document(index_name="test_index", document_id="test_id")

    assert document.document_id == "test-document-id"
    assert document.path == "test-path"
    assert document.parent_document_id == "test-parent-document-id"
    assert document.content == {"field-1": "value-1", "field-2": "value-2"}
    assert document.index_fields == ["field-1", "field-2"]
    assert document.authorized_groups == ["group-1", "group-2"]

    assert document.chunks[0].chunk_id == "test-chunk-id"
    assert document.chunks[0].sort_id == 1
    assert document.chunks[0].parent_document_id == "test-parent-document-id"
    assert document.chunks[0].content == {"field-1": "value-1", "field-2": "value-2"}
    assert document.chunks[0].origin == {"field-1": "value-1", "field-2": "value-2"}

    assert document.chunks[0].assets_info is not None
    assert len(document.chunks[0].assets_info) == 2

    assert document.chunks[0].assets_info[0].asset_id == "test-asset-id"
    assert document.chunks[0].assets_info[0].asset_type == AssetType.PAGE_IMAGE
    assert document.chunks[0].assets_info[0].content_type == "test-content-type"
    assert document.chunks[0].assets_info[0].presigned_url == "test-presigned-url"

    assert document.chunks[0].assets_info[1].asset_id == "test-asset-id"

    assert document.chunks[0].assets_info[1].asset_type == AssetType.PAGE_MARKDOWN
    assert document.chunks[0].assets_info[1].content_type == "test-content-type"
    assert document.chunks[0].assets_info[1].presigned_url == "test-presigned-url"


@mock_endpoint(
    "POST",
    "http://test.com/v1/indexes/test_index/_refresh",
    200,
)
def test_refresh_is_valid():
    compass = CompassClient(index_url="http://test.com")
    compass.refresh_index(index_name="test_index")


@mock_endpoint(
    "POST",
    "http://test.com/v1/indexes/test_index/documents/test_id/_add_attributes",
    200,
    {"fake": "context"},
)
def test_add_attributes_is_valid():
    attrs = DocumentAttributes()
    attrs.fake = "context"
    compass = CompassClient(index_url="http://test.com")
    compass.add_attributes(
        index_name="test_index",
        document_id="test_id",
        attributes=attrs,
    )


def test_get_document_asset_with_json_asset(respx_mock: MockRouter):
    respx_mock.get(
        "http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"test": "test"},
            headers={"Content-Type": "application/json"},
        ),
    )
    compass = CompassClient(index_url="http://test.com")
    asset, content_type = compass.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )

    assert isinstance(asset, dict)
    assert asset == {"test": "test"}
    assert content_type == "application/json"


def test_get_document_asset_markdown(respx_mock: MockRouter):
    respx_mock.get(
        "http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id"
    ).mock(
        return_value=httpx.Response(
            200,
            text="# Test",
            headers={"Content-Type": "text/markdown"},
        ),
    )
    compass = CompassClient(index_url="http://test.com")
    asset, content_type = compass.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )
    assert isinstance(asset, str)
    assert asset == "# Test"
    assert content_type == "text/markdown"


def test_get_document_asset_image(respx_mock: MockRouter):
    respx_mock.get(
        "http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id"
    ).mock(
        return_value=httpx.Response(
            200,
            content=b"test",
            headers={"Content-Type": "image/png"},
        ),
    )
    compass = CompassClient(index_url="http://test.com")
    asset, content_type = compass.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )
    assert isinstance(asset, bytes)
    assert asset == b"test"
    assert content_type == "image/png"


def test_direct_search_is_valid(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/_direct_search"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"hits": [], "scroll_id": "test_scroll_id"},
        )
    )

    compass = CompassClient(index_url="http://test.com")
    compass.direct_search(index_name="test_index", query={"match_all": {}})

    assert route.called
    assert route.call_count == 1

    req_sent = json.loads(route.calls.last.request.content)
    assert "query" in req_sent
    assert "size" in req_sent


def test_direct_search_scroll_is_valid(respx_mock: MockRouter):
    index_name = "test_index"
    route = respx_mock.post(
        f"http://test.com/v1/indexes/{index_name}/_direct_search/scroll",
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": [],
                "scroll_id": "test_scroll_id",
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    compass.direct_search_scroll(
        scroll_id="test_scroll_id",
        index_name=index_name,
        scroll="5m",
    )

    assert route.called
    assert route.call_count == 1

    req_sent = json.loads(route.calls.last.request.content)
    assert req_sent["scroll_id"] == "test_scroll_id"
    assert req_sent["scroll"] == "5m"


@respx.mock
def test_direct_search_with_sort_by_single_field(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/_direct_search"
    ).mock(
        return_value=httpx.Response(
            200, json={"hits": [], "scroll_id": "test_scroll_id"}
        )
    )

    compass = CompassClient(index_url="http://test.com")
    sort_by = [SortBy(field="created_at", order="desc")]
    compass.direct_search(
        index_name="test_index", query={"match_all": {}}, sort_by=sort_by
    )

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "sort_by" in payload
    assert payload["sort_by"] == [{"field": "created_at", "order": "desc"}]


@respx.mock
def test_direct_search_with_sort_by_multiple_fields(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/_direct_search"
    ).mock(
        return_value=httpx.Response(
            200, json={"hits": [], "scroll_id": "test_scroll_id"}
        )
    )

    compass = CompassClient(index_url="http://test.com")
    sort_by = [
        SortBy(field="created_at", order="desc"),
        SortBy(field="score", order="asc"),
    ]
    compass.direct_search(
        index_name="test_index", query={"match_all": {}}, sort_by=sort_by
    )

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "sort_by" in payload
    assert payload["sort_by"] == [
        {"field": "created_at", "order": "desc"},
        {"field": "score", "order": "asc"},
    ]


@respx.mock
def test_direct_search_without_sort_by(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/_direct_search"
    ).mock(
        return_value=httpx.Response(
            200, json={"hits": [], "scroll_id": "test_scroll_id"}
        )
    )

    compass = CompassClient(index_url="http://test.com")
    compass.direct_search(index_name="test_index", query={"match_all": {}})

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "sort_by" not in payload or payload["sort_by"] is None


@respx.mock
def test_get_models(respx_mock: MockRouter):
    route = respx_mock.get("http://test.com/v1/config/models").mock(
        return_value=httpx.Response(
            200,
            json={
                "dense": [
                    "embed-english-v3.0",
                    "embed-multilingual-v3.0",
                    "embed-v4.0",
                ],
                "sparse": [
                    "sparse-v1.0",
                    "sparse-multilingual-v1.0",
                ],
                "rerank": [
                    "rerank-v3.5",
                ],
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.get_models()

    assert route.called
    assert result == {
        "dense": [
            "embed-english-v3.0",
            "embed-multilingual-v3.0",
            "embed-v4.0",
        ],
        "sparse": [
            "sparse-v1.0",
            "sparse-multilingual-v1.0",
        ],
        "rerank": [
            "rerank-v3.5",
        ],
    }


@respx.mock
def test_get_index_details(respx_mock: MockRouter):
    route = respx_mock.get("http://test.com/v1/indexes/test_index").mock(
        return_value=httpx.Response(
            200,
            json={
                "number_of_shards": 5,
                "number_of_replicas": 1,
                "knn_index_engine": "faiss",
                "analyzer": "english",
                "dense_model": "embed-english-v3.0",
                "sparse_model": "sparse-v1.0",
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.get_index_details(index_name="test_index")

    assert route.called
    assert result == IndexConfig(
        number_of_shards=5,
        number_of_replicas=1,
        knn_index_engine="faiss",
        analyzer="english",
        dense_model="embed-english-v3.0",
        sparse_model="sparse-v1.0",
    )


@respx.mock
def test_upload_document(respx_mock: MockRouter):
    upload_id = uuid.uuid4()
    document_id = "test_document_id"

    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/documents/_upload"
    ).mock(
        return_value=httpx.Response(
            200, json={"upload_id": str(upload_id), "document_ids": [document_id]}
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.upload_document(
        index_name="test_index",
        filename="test.pdf",
        filebytes=b"test content",
        content_type=ContentTypeEnum.ApplicationPdf,
        document_id=document_id,
    )

    assert route.called
    assert result == UploadDocumentsResult(
        upload_id=upload_id, document_ids=[document_id]
    )


@respx.mock
def test_upload_document_status(respx_mock: MockRouter):
    upload_id = uuid.uuid4()

    route = respx_mock.get(
        "http://test.com/v1/indexes/test_index/documents/_upload/upload_123/status"
    ).mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "upload_id": str(upload_id),
                    "document_id": "doc123",
                    "destinations": ["destination1", "destination2"],
                    "file_name": "test.pdf",
                    "state": "completed",
                    "last_error": None,
                    "parsed_presigned_url": "test-presigned-url",
                }
            ],
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.upload_document_status(
        index_name="test_index", upload_id="upload_123"
    )

    assert route.called

    assert result[0].upload_id == upload_id
    assert result[0].document_id == "doc123"
    assert result[0].destinations == ["destination1", "destination2"]
    assert result[0].file_name == "test.pdf"
    assert result[0].state == "completed"
    assert result[0].last_error is None
    assert result[0].parsed_presigned_url == "test-presigned-url"


@respx.mock
def test_download_parsed_document(respx_mock: MockRouter):
    upload_id = uuid.uuid4()

    route = respx_mock.get(
        "http://test.com/v1/indexes/test_index/documents/_upload/upload123/download"
    ).mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "upload_id": str(upload_id),
                    "document_id": "doc123",
                    "documents": [],
                    "state": "completed",
                }
            ],
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.download_parsed_document(
        index_name="test_index", upload_id="upload123"
    )

    assert route.called
    assert result[0].upload_id == upload_id
    assert result[0].document_id == "doc123"
    assert result[0].state == "completed"


@respx.mock
def test_search_documents(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/documents/_search"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": [
                    {
                        "document_id": "doc1",
                        "score": 0.9,
                        "content": {"text": "test"},
                        "path": "test1.pdf",
                        "parent_document_id": "parent1",
                        "chunks": [],
                    },
                    {
                        "document_id": "doc2",
                        "score": 0.8,
                        "content": {"text": "test2"},
                        "path": "test2.pdf",
                        "parent_document_id": "parent2",
                        "chunks": [],
                    },
                ]
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.search_documents(
        index_name="test_index", query="test query", top_k=10
    )

    assert route.called
    assert len(result.hits) == 2
    assert result.hits[0].score == 0.9


@respx.mock
def test_search_chunks(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/documents/_search_chunks"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
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
                    },
                    {
                        "chunk_id": "chunk2",
                        "score": 0.85,
                        "text": "chunk text 2",
                        "content": {},
                        "parent_document_id": "doc2",
                        "document_id": "chunk2",
                        "sort_id": 1,
                        "path": "test.pdf",
                    },
                ]
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.search_chunks(index_name="test_index", query="test query", top_k=5)

    assert route.called
    assert len(result.hits) == 2
    assert result.hits[0].score == 0.95


@respx.mock
def test_create_datasource(respx_mock: MockRouter):
    datasource_id = uuid.uuid4()
    created_at = datetime.now()
    updated_at = datetime.now()
    route = respx_mock.post("http://test.com/v1/datasources").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": str(datasource_id),
                "name": "Test Datasource",
                "description": "Test Datasource",
                "config": {
                    "type": "msft_azure_blob_storage",
                    "connection_string": "test_connection_string",
                    "container_name": "test_container_name",
                    "name_starts_with": "test_name_starts_with",
                },
                "destinations": [],
                "enabled": True,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")

    datasource_obj = DataSource(
        id=datasource_id,
        name="Test Datasource",
        description="Test Datasource",
        config=AzureBlobStorageConfig(
            type="msft_azure_blob_storage",
            connection_string="test_connection_string",
            container_name="test_container_name",
            name_starts_with="test_name_starts_with",
        ),
        destinations=[],
        enabled=True,
        created_at=created_at,
        updated_at=updated_at,
    )
    create_datasource = CreateDataSource(datasource=datasource_obj)
    result = compass.create_datasource(datasource=create_datasource)

    assert route.called
    assert result.id == datasource_id
    assert result.name == "Test Datasource"
    assert result.description == "Test Datasource"
    assert result.config == AzureBlobStorageConfig(
        type="msft_azure_blob_storage",
        connection_string="test_connection_string",
        container_name="test_container_name",
        name_starts_with="test_name_starts_with",
    )
    assert result.destinations == []
    assert result.enabled is True
    assert result.created_at == created_at
    assert result.updated_at == updated_at


@respx.mock
def test_list_datasources(respx_mock: MockRouter):
    ds1_id = uuid.uuid4()
    ds2_id = uuid.uuid4()
    route = respx_mock.get("http://test.com/v1/datasources").mock(
        return_value=httpx.Response(
            200,
            json={
                "value": [
                    {
                        "id": str(ds1_id),
                        "name": "Datasource 1",
                        "description": "Datasource 1",
                        "type": "msft_azure_blob_storage",
                        "config": {
                            "type": "msft_azure_blob_storage",
                            "connection_string": "test_connection_string",
                            "container_name": "test_container_name",
                            "name_starts_with": "test_name_starts_with",
                        },
                        "destinations": [],
                    },
                    {
                        "id": str(ds2_id),
                        "name": "Datasource 2",
                        "description": "Datasource 2",
                        "type": "msft_azure_blob_storage",
                        "config": {
                            "type": "msft_azure_blob_storage",
                            "connection_string": "test_connection_string",
                            "container_name": "test_container_name",
                            "name_starts_with": "test_name_starts_with",
                        },
                        "destinations": [],
                    },
                ],
                "skip": 0,
                "limit": 10,
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.list_datasources()

    assert route.called
    assert len(result.value) == 2


@respx.mock
def test_get_datasource(respx_mock: MockRouter):
    ds_id = uuid.uuid4()
    created_at = datetime.now()
    updated_at = datetime.now()
    route = respx_mock.get(f"http://test.com/v1/datasources/{ds_id}").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": str(ds_id),
                "name": "Test Datasource",
                "description": "Test Datasource",
                "type": "msft_azure_blob_storage",
                "config": {
                    "type": "msft_azure_blob_storage",
                    "connection_string": "test_connection_string",
                    "container_name": "test_container_name",
                    "name_starts_with": "test_name_starts_with",
                },
                "destinations": [],
                "enabled": True,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.get_datasource(datasource_id=str(ds_id))

    assert route.called
    assert result.name == "Test Datasource"
    assert result.description == "Test Datasource"
    assert result.config == AzureBlobStorageConfig(
        type="msft_azure_blob_storage",
        connection_string="test_connection_string",
        container_name="test_container_name",
        name_starts_with="test_name_starts_with",
    )
    assert result.destinations == []
    assert result.enabled is True
    assert result.created_at == created_at
    assert result.updated_at == updated_at


@respx.mock
def test_delete_datasource(respx_mock: MockRouter):
    route = respx_mock.delete("http://test.com/v1/datasources/ds123").mock(
        return_value=httpx.Response(200, json={"status": "deleted"})
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.delete_datasource(datasource_id="ds123")

    assert route.called
    assert result["status"] == "deleted"


@respx.mock
def test_sync_datasource(respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/datasources/ds123/_sync").mock(
        return_value=httpx.Response(
            200, json={"sync_id": "sync123", "status": "started"}
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.sync_datasource(datasource_id="ds123")

    assert route.called
    # Result may be a string or dict depending on implementation
    assert result is not None


@respx.mock
def test_update_group_authorization(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/group_authorization"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "document_id": "doc1",
                        "error": None,
                    },
                    {
                        "document_id": "doc2",
                        "error": "test error",
                    },
                ]
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    group_auth = GroupAuthorizationInput(
        document_ids=["doc1", "doc2"],
        authorized_groups=["group1", "group2"],
        action=GroupAuthorizationActions.ADD,
    )
    result = compass.update_group_authorization(
        index_name="test_index", group_auth_input=group_auth
    )

    assert route.called
    assert result.results[0].document_id == "doc1"
    assert result.results[0].error is None
    assert result.results[1].document_id == "doc2"
    assert result.results[1].error == "test error"


# Error handling tests
@respx.mock
def test_authentication_error_401(respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes").mock(
        return_value=httpx.Response(401, json={"error": "Unauthorized"})
    )

    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(CompassError, match="Failed to send request"):
        compass.list_indexes()


@respx.mock
def test_client_error_404(respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes/nonexistent").mock(
        return_value=httpx.Response(404, json={"error": "Index not found"})
    )

    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(CompassError, match="Failed to send request"):
        compass.get_index_details(index_name="nonexistent")


@respx.mock
def test_server_error_500(respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes").mock(
        return_value=httpx.Response(500, json={"error": "Internal server error"})
    )

    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(CompassError, match="Failed to send request"):
        compass.list_indexes()


# Test timeout and retry behavior
@respx.mock
def test_timeout_handling(respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes").mock(
        side_effect=httpx.TimeoutException("Request timeout")
    )

    compass = CompassClient(index_url="http://test.com")
    with pytest.raises(CompassError, match="Failed to send request"):
        compass.list_indexes()


# Test client initialization and configuration
def test_compass_client_initialization():
    client = CompassClient(index_url="http://test.com")
    assert client.index_url == "http://test.com/"

    client_with_auth = CompassClient(
        index_url="http://test.com", bearer_token="test_token"
    )
    assert client_with_auth.index_url == "http://test.com/"
    assert client_with_auth.bearer_token == "test_token"


def test_compass_client_close():
    client = CompassClient(index_url="http://test.com")
    client.close()  # Should not raise an error


# Test search with filters
@respx.mock
def test_search_documents_with_filters(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/documents/_search"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
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
    )

    compass = CompassClient(index_url="http://test.com")
    search_filter = SearchFilter(field="department", type="$eq", value="engineering")
    result = compass.search_documents(
        index_name="test_index", query="test query", top_k=10, filters=[search_filter]
    )

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "filters" in payload
    assert result.hits[0].document_id == "doc1"


# Test edge cases and validation
@respx.mock
def test_empty_query_search(respx_mock: MockRouter):
    route = respx_mock.post(
        "http://test.com/v1/indexes/test_index/documents/_search"
    ).mock(return_value=httpx.Response(200, json={"hits": []}))

    compass = CompassClient(index_url="http://test.com")
    result = compass.search_documents(index_name="test_index", query="", top_k=10)

    assert route.called
    assert len(result.hits) == 0


# Test list_datasources_objects_states if it exists
@respx.mock
def test_list_datasources_objects_states(respx_mock: MockRouter):
    source_id = "test-source-id"
    ds_id = uuid.uuid4()
    skip = 0
    limit = 10
    created_at = datetime.now()
    updated_at = datetime.now()
    route = respx_mock.get(
        f"http://test.com/v1/datasources/{ds_id}/documents?skip={skip}&limit={limit}"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "value": [
                    {
                        "document_id": "doc1",
                        "source_id": source_id,
                        "state": "processed",
                        "destinations": ["destination1", "destination2"],
                        "created_at": created_at.isoformat(),
                        "updated_at": updated_at.isoformat(),
                    },
                    {
                        "document_id": "doc2",
                        "state": "pending",
                        "destinations": ["destination3", "destination4"],
                        "source_id": source_id,
                        "created_at": created_at.isoformat(),
                        "updated_at": updated_at.isoformat(),
                    },
                ],
                "skip": skip,
                "limit": limit,
            },
        )
    )

    compass = CompassClient(index_url="http://test.com")
    result = compass.list_datasources_objects_states(
        datasource_id=str(ds_id),
        skip=skip,
        limit=limit,
    )

    assert route.called
    assert len(result.value) == 2
    assert result.value[0].document_id == "doc1"
    assert result.value[0].state == "processed"
    assert result.value[0].source_id == source_id
    assert result.value[0].created_at == created_at
    assert result.value[0].updated_at == updated_at
    assert result.value[1].document_id == "doc2"
    assert result.value[1].state == "pending"
    assert result.value[1].source_id == source_id
    assert result.value[1].created_at == created_at
    assert result.value[1].updated_at == updated_at
