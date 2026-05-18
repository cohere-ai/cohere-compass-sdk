import json
import uuid
from collections.abc import Callable
from typing import Any, Literal, cast

import httpx
import pytest
import respx
from pydantic import ValidationError
from respx import MockRouter

from cohere_compass import GroupAuthorizationActions, GroupAuthorizationInput
from cohere_compass.clients import CompassClient
from cohere_compass.exceptions import (
    CompassAuthError,
    CompassClientError,
    CompassError,
    CompassTimeoutError,
)
from cohere_compass.models import (
    CompassDocument,
    SearchFilter,
)
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.documents import (
    AssetPresignedUrlDetails,
    AssetPresignedUrlRequest,
    AssetType,
    CompassDocumentMetadata,
    ContentTypeEnum,
    DocumentAttributes,
    ParseableDocument,
    UploadDocumentsResult,
    UploadFilePresignedUrlRequest,
    UploadFilePresignedUrlResponse,
)
from cohere_compass.models.indexes import (
    IndexDetails,
    IndexInfo,
    RetentionPolicy,
    RetentionType,
)
from cohere_compass.models.search import (
    SortBy,
)
from tests.utils import SyncifiedCompassAsyncClient

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
        def wrapper(client: CompassClient, respx_mock: MockRouter, *args: Any, **kwargs: Any):
            route = getattr(respx_mock, method.lower())(url).mock(
                return_value=httpx.Response(status_code, json=response_body)
            )

            test_func(client, *args, **kwargs)

            assert route.called, f"Expected {method} {url} to be called"
            assert route.call_count == 1, f"Expected {method} {url} to be called once"

            if expected_request_body is not None:
                request_body = json.loads(route.calls.last.request.content)
                assert request_body == expected_request_body, (
                    f"Expected JSON body {expected_request_body}, got {request_body}"
                )

        return wrapper  # type: ignore

    return decorator


@pytest.fixture
def sync_client():
    return CompassClient(index_url="http://test.com")


@pytest.fixture
def async_client() -> CompassClient:
    return cast(CompassClient, SyncifiedCompassAsyncClient(index_url="http://test.com"))


# A single "client" fixture that selects which one to use
@pytest.fixture(params=["sync_client", "async_client"])
def client(request: pytest.FixtureRequest):
    return request.getfixturevalue(request.param)


@mock_endpoint(
    "DELETE",
    "http://test.com/v1/indexes/test_index/documents/test_id",
    201,
)
def test_delete_url_formatted_with_doc_and_index(client: CompassClient):
    client.delete_document(index_name="test_index", document_id="test_id")


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index",
    200,
)
def test_create_index_formatted_with_index(client: CompassClient):
    client.create_index(index_name="test_index")


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index",
    200,
    {"number_of_shards": 5},
)
def test_create_index_with_index_config(client: CompassClient):
    client.create_index(index_name="test_index", index_config=IndexConfig(number_of_shards=5))


@respx.mock
def test_create_index_with_invalid_name(client: CompassClient, respx_mock: MockRouter):
    client = CompassClient(index_url="http://test.com")
    with pytest.raises(ValueError) as exc_info:
        client.create_index(index_name="there/are/slashes/here")
    assert "Invalid index name" in str(exc_info.value)


@respx.mock
def test_create_index_400s_propagated_to_caller(client: CompassClient, respx_mock: MockRouter):
    respx_mock.put("http://test.com/v1/indexes/test-index").mock(
        return_value=httpx.Response(400, json={"error": "invalid request"})
    )
    with pytest.raises(CompassError, match=r'Client error 400: {"error":"invalid request"}'):
        client.create_index(index_name="test-index")


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
def test_put_documents_payload_and_url_exist(client: CompassClient):
    client.insert_docs(index_name="test_index", docs=[CompassDocument()])


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
def test_put_document_payload_and_url_exist(client: CompassClient):
    client.insert_doc(index_name="test_index", doc=CompassDocument())


@respx.mock(assert_all_mocked=True)
def test_put_document_payload_with_invalid_document_id(client: CompassClient, respx_mock: MockRouter):
    doc = CompassDocument(
        filebytes=b"",
        metadata=CompassDocumentMetadata(document_id="bypass-validation", filename="tests/docs/sample.doc"),
    )
    doc.metadata.document_id = "something/with/slashes"
    with pytest.raises(ValidationError) as exc_info:
        client.insert_doc(index_name="test_index", doc=doc)
        assert "String should match pattern" in str(exc_info)


@respx.mock(assert_all_mocked=True)
def test_insert_doc_with_tuple_does_not_throw_attribute_error(client: CompassClient, respx_mock: MockRouter):
    try:
        client.insert_docs(
            index_name="test_index",
            docs=[("filename", Exception("error in processing"))],
        )
    except AttributeError as e:
        pytest.fail(f"Unexpected exception: {e}")


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
def test_list_indices_is_valid(client: CompassClient):
    response = client.list_indexes()
    assert response.indexes == [IndexInfo(name="test_index", count=1, parent_doc_count=1)]


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
                    "path": "test-path",
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
def test_get_document_is_valid(client: CompassClient):
    document = client.get_document(index_name="test_index", document_id="test_id")

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
def test_refresh_is_valid(client: CompassClient):
    client.refresh_index(index_name="test_index")


@mock_endpoint(
    "POST",
    "http://test.com/v1/indexes/test_index/documents/test_id/_add_attributes",
    200,
    {"fake": "context"},
)
def test_add_attributes_is_valid(client: CompassClient):
    attrs = DocumentAttributes()
    attrs.fake = "context"
    client.add_attributes(
        index_name="test_index",
        document_id="test_id",
        attributes=attrs,
    )


def test_get_document_asset_with_json_asset(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id").mock(
        return_value=httpx.Response(
            200,
            json={"test": "test"},
            headers={"Content-Type": "application/json"},
        ),
    )
    asset, content_type = client.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )

    assert isinstance(asset, dict)
    assert asset == {"test": "test"}
    assert content_type == "application/json"


def test_get_document_asset_markdown(respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id").mock(
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


def test_get_document_asset_image(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes/test_index/documents/test_id/assets/test_asset_id").mock(
        return_value=httpx.Response(
            200,
            content=b"test",
            headers={"Content-Type": "image/png"},
        ),
    )
    asset, content_type = client.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )
    assert isinstance(asset, bytes)
    assert asset == b"test"
    assert content_type == "image/png"


def test_get_media_clip(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.get(
        "http://test.com/v1/indexes/test_index/documents/doc_audio/assets/audio_asset_id",
        params={"start_time": "1.5", "end_time": "3.0"},
    ).mock(
        return_value=httpx.Response(
            200,
            content=b"fake-wav-bytes",
            headers={"Content-Type": "audio/wav"},
        ),
    )
    clip, content_type = client.get_media_clip(
        index_name="test_index",
        document_id="doc_audio",
        asset_id="audio_asset_id",
        start_time=1.5,
        end_time=3.0,
    )
    assert route.called
    assert isinstance(clip, bytes)
    assert clip == b"fake-wav-bytes"
    assert content_type == "audio/wav"


def test_direct_search_is_valid(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/_direct_search").mock(
        return_value=httpx.Response(
            200,
            json={"hits": [], "scroll_id": "test_scroll_id"},
        )
    )

    client.direct_search(index_name="test_index", query={"match_all": {}})

    assert route.called
    assert route.call_count == 1

    req_sent = json.loads(route.calls.last.request.content)
    assert "query" in req_sent
    assert "size" in req_sent


def test_direct_search_scroll_is_valid(client: CompassClient, respx_mock: MockRouter):
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

    client.direct_search_scroll(
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
def test_direct_search_with_sort_by_single_field(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/_direct_search").mock(
        return_value=httpx.Response(200, json={"hits": [], "scroll_id": "test_scroll_id"})
    )

    sort_by = [SortBy(field="created_at", order="desc")]
    client.direct_search(index_name="test_index", query={"match_all": {}}, sort_by=sort_by)

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "sort_by" in payload
    assert payload["sort_by"] == [{"field": "created_at", "order": "desc"}]


@respx.mock
def test_direct_search_with_sort_by_multiple_fields(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/_direct_search").mock(
        return_value=httpx.Response(200, json={"hits": [], "scroll_id": "test_scroll_id"})
    )

    sort_by = [
        SortBy(field="created_at", order="desc"),
        SortBy(field="score", order="asc"),
    ]
    client.direct_search(index_name="test_index", query={"match_all": {}}, sort_by=sort_by)

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "sort_by" in payload
    assert payload["sort_by"] == [
        {"field": "created_at", "order": "desc"},
        {"field": "score", "order": "asc"},
    ]


@respx.mock
def test_direct_search_without_sort_by(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/_direct_search").mock(
        return_value=httpx.Response(200, json={"hits": [], "scroll_id": "test_scroll_id"})
    )

    client.direct_search(index_name="test_index", query={"match_all": {}})

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "sort_by" not in payload or payload["sort_by"] is None


@respx.mock
def test_get_models(client: CompassClient, respx_mock: MockRouter):
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

    result = client.get_models()

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
def test_get_index_details(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.get("http://test.com/v1/indexes/test_index").mock(
        return_value=httpx.Response(
            200,
            json={
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
    )

    result = client.get_index_details(index_name="test_index")

    assert route.called
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


@respx.mock
def test_upload_document(client: CompassClient, respx_mock: MockRouter):
    upload_id = uuid.uuid4()
    document_id = "test_document_id"

    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/upload").mock(
        return_value=httpx.Response(200, json={"upload_id": str(upload_id), "document_ids": [document_id]})
    )

    result = client.upload_document(
        index_name="test_index",
        filename="test.pdf",
        filebytes=b"test content",
        content_type=ContentTypeEnum.ApplicationPdf,
        document_id=document_id,
    )

    assert route.called
    assert result == UploadDocumentsResult(upload_id=upload_id, document_ids=[document_id])


@respx.mock
def test_upload_document_with_authorized_groups(client: CompassClient, respx_mock: MockRouter):
    upload_id = uuid.uuid4()
    document_id = "test_document_id"

    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/upload").mock(
        return_value=httpx.Response(200, json={"upload_id": str(upload_id), "document_ids": [document_id]})
    )

    result = client.upload_document(
        index_name="test_index",
        filename="test.pdf",
        filebytes=b"test content",
        content_type=ContentTypeEnum.ApplicationPdf,
        document_id=document_id,
        authorized_groups=["group1", "group2"],
    )

    assert route.called
    assert result == UploadDocumentsResult(upload_id=upload_id, document_ids=[document_id])

    # Verify the request payload includes authorized_groups
    request = route.calls.last.request
    request_body = json.loads(request.content)
    assert "authorized_groups" in request_body
    assert request_body["authorized_groups"] == ["group1", "group2"]


@respx.mock
def test_upload_document_status(client: CompassClient, respx_mock: MockRouter):
    upload_id = uuid.uuid4()

    route = respx_mock.get(f"http://test.com/v1/indexes/test_index/documents/upload/{upload_id}").mock(
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

    result = client.upload_document_status(index_name="test_index", upload_id=upload_id)

    assert route.called

    assert result[0].upload_id == upload_id
    assert result[0].document_id == "doc123"
    assert result[0].destinations == ["destination1", "destination2"]
    assert result[0].file_name == "test.pdf"
    assert result[0].state == "completed"
    assert result[0].last_error is None
    assert result[0].parsed_presigned_url == "test-presigned-url"


@respx.mock
def test_bulk_upload_document_status(client: CompassClient, respx_mock: MockRouter):
    upload_id_1 = uuid.uuid4()
    upload_id_2 = uuid.uuid4()

    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/uploads").mock(
        return_value=httpx.Response(
            200,
            json=[
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
                {
                    "upload_id": str(upload_id_2),
                    "statuses": [],
                },
            ],
        )
    )

    result = client.bulk_upload_document_status(
        index_name="test_index",
        upload_ids=[upload_id_1, upload_id_2],
    )

    assert route.called

    request_body = json.loads(route.calls.last.request.content)
    assert request_body == {"upload_ids": [str(upload_id_1), str(upload_id_2)]}

    assert len(result) == 2

    assert result[0].upload_id == upload_id_1
    assert len(result[0].statuses) == 1
    assert result[0].statuses[0].upload_id == upload_id_1
    assert result[0].statuses[0].document_id == "doc-abc"
    assert result[0].statuses[0].destinations == ["test_index"]
    assert result[0].statuses[0].file_name == "report.pdf"
    assert result[0].statuses[0].state == "COMPLETED"
    assert result[0].statuses[0].last_error is None
    assert result[0].statuses[0].parsed_presigned_url == "https://presigned-url"

    assert result[1].upload_id == upload_id_2
    assert result[1].statuses == []


@respx.mock
def test_download_parsed_document(client: CompassClient, respx_mock: MockRouter):
    upload_id = uuid.uuid4()

    route = respx_mock.get(f"http://test.com/v1/indexes/test_index/documents/upload/{upload_id}/_download").mock(
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

    result = client.download_parsed_document(index_name="test_index", upload_id=upload_id)

    assert route.called
    assert result[0].upload_id == upload_id
    assert result[0].document_id == "doc123"
    assert result[0].state == "completed"


@respx.mock
def test_search_documents(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/_search").mock(
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

    result = client.search_documents(index_name="test_index", query="test query", top_k=10)

    assert route.called
    assert len(result.hits) == 2
    assert result.hits[0].score == 0.9


@respx.mock
def test_search_chunks(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/_search_chunks").mock(
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

    result = client.search_chunks(index_name="test_index", query="test query", top_k=5)

    assert route.called
    assert len(result.hits) == 2
    assert result.hits[0].score == 0.95


@respx.mock
def test_update_group_authorization(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/group_authorization").mock(
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

    group_auth = GroupAuthorizationInput(
        document_ids=["doc1", "doc2"],
        authorized_groups=["group1", "group2"],
        action=GroupAuthorizationActions.ADD,
    )
    result = client.update_group_authorization(index_name="test_index", group_auth_input=group_auth)

    assert route.called
    assert result.results[0].document_id == "doc1"
    assert result.results[0].error is None
    assert result.results[1].document_id == "doc2"
    assert result.results[1].error == "test error"


# Error handling tests
@respx.mock
def test_authentication_error_401(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes").mock(return_value=httpx.Response(401, json={"error": "Unauthorized"}))

    with pytest.raises(CompassAuthError, match=r"Unauthorized. Please check your bearer token."):
        client.list_indexes()


@respx.mock
def test_client_error_404(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes/nonexistent").mock(
        return_value=httpx.Response(404, json={"error": "Index not found"})
    )

    with pytest.raises(CompassClientError, match=r'Client error 404: {"error":"Index not found"}'):
        client.get_index_details(index_name="nonexistent")


@respx.mock
def test_server_error_500(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes").mock(
        return_value=httpx.Response(500, json={"error": "Internal server error"})
    )

    with pytest.raises(CompassError, match=r'Server error 500: {"error":"Internal server error"}'):
        client.list_indexes()


# Test timeout and retry behavior
@respx.mock
def test_timeout_handling(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/indexes").mock(side_effect=httpx.TimeoutException("Request timeout"))

    with pytest.raises(CompassTimeoutError, match="Timeout error: Request timeout"):
        client.list_indexes()


# Test client initialization and configuration
def test_compass_client_initialization():
    client = CompassClient(index_url="http://test.com")
    assert client.index_url == "http://test.com/"

    # TODO ----- add test for async client -----
    client_with_auth = CompassClient(index_url="http://test.com", bearer_token="test_token")
    assert client_with_auth.index_url == "http://test.com/"
    assert client_with_auth.bearer_token == "test_token"


def test_compass_client_close(client: CompassClient):
    client.close()  # Should not raise an error


# Test search with filters
@respx.mock
def test_search_documents_with_filters(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/_search").mock(
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

    search_filter = SearchFilter(field="department", type=SearchFilter.FilterType.EQ, value="engineering")
    result = client.search_documents(index_name="test_index", query="test query", top_k=10, filters=[search_filter])

    assert route.called
    payload = json.loads(route.calls.last.request.content)
    assert "filters" in payload
    assert result.hits[0].document_id == "doc1"


# Test edge cases and validation
@respx.mock
def test_empty_query_search(client: CompassClient, respx_mock: MockRouter):
    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/_search").mock(
        return_value=httpx.Response(200, json={"hits": []})
    )

    result = client.search_documents(index_name="test_index", query="", top_k=10)

    assert route.called
    assert len(result.hits) == 0


@mock_endpoint(
    "GET",
    "http://test.com/v1/tasks/test_task_id",
    200,
    response_body={
        "task_id": "test_task_id",
        "status": "completed",
        "result": {"key": "value"},
    },
)
def test_get_task_is_valid(client: CompassClient):
    result = client.get_task(task_id="test_task_id")

    assert result["task_id"] == "test_task_id"
    assert result["status"] == "completed"
    assert result["result"] == {"key": "value"}


@respx.mock
def test_get_task_not_found(client: CompassClient, respx_mock: MockRouter):
    respx_mock.get("http://test.com/v1/tasks/nonexistent").mock(
        return_value=httpx.Response(404, json={"error": "Task not found"})
    )

    with pytest.raises(CompassClientError):
        client.get_task(task_id="nonexistent")


def test_get_asset_presigned_urls_success(client: CompassClient, respx_mock: MockRouter):
    from cohere_compass.models.documents import AssetPresignedUrlRequest

    asset_a_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    asset_b_uuid = uuid.UUID("87654321-4321-8765-4321-876543218765")
    respx_mock.post("http://test.com/v1/indexes/test_index/assets/_presigned_urls").mock(
        return_value=httpx.Response(
            200,
            json={
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
        ),
    )

    result = client.get_asset_presigned_urls(
        index_name="test_index",
        assets=[
            AssetPresignedUrlRequest(document_id="doc_123", asset_id=asset_a_uuid),
            AssetPresignedUrlRequest(document_id="doc_456", asset_id=asset_b_uuid),
        ],
    )

    assert len(result) == 2
    assert result[0].document_id == "doc_123"
    assert result[0].asset_id == asset_a_uuid
    assert result[0].presigned_url == "https://example.com/asset_A?X-Amz-Signature=..."
    assert result[1].document_id == "doc_456"
    assert result[1].asset_id == asset_b_uuid
    assert result[1].presigned_url == "https://example.com/asset_B?X-Amz-Signature=..."


# ── ParseableDocument model validator tests ──────────────────────────────


def test_parseable_document_with_encoded_bytes():
    doc = ParseableDocument(
        id="doc1",
        filename="test.pdf",
        content_length_bytes=100,
        content_encoded_bytes="dGVzdA==",
        attributes=DocumentAttributes(),
    )
    assert doc.content_encoded_bytes == "dGVzdA=="
    assert doc.file_data_uuid is None


def test_parseable_document_with_file_data_uuid():
    test_uuid = uuid.uuid4()
    doc = ParseableDocument(
        id="doc1",
        filename="test.pdf",
        content_length_bytes=100,
        file_data_uuid=test_uuid,
        attributes=DocumentAttributes(),
    )
    assert doc.file_data_uuid == test_uuid
    assert doc.content_encoded_bytes is None


def test_parseable_document_rejects_both_bytes_and_uuid():
    with pytest.raises(ValidationError, match="Exactly one of"):
        ParseableDocument(
            id="doc1",
            filename="test.pdf",
            content_length_bytes=100,
            content_encoded_bytes="dGVzdA==",
            file_data_uuid=uuid.uuid4(),
            attributes=DocumentAttributes(),
        )


def test_parseable_document_rejects_neither_bytes_nor_uuid():
    with pytest.raises(ValidationError, match="Exactly one of"):
        ParseableDocument(
            id="doc1",
            filename="test.pdf",
            content_length_bytes=100,
            attributes=DocumentAttributes(),
        )


# ── AssetPresignedUrlRequest new optional fields ─────────────────────────


def test_asset_presigned_url_request_with_crop_fields():
    asset_id = uuid.uuid4()
    req = AssetPresignedUrlRequest(
        document_id="doc1",
        asset_id=asset_id,
        x0=10,
        y0=20,
        x1=500,
        y1=800,
    )
    assert req.x0 == 10
    assert req.y0 == 20
    assert req.x1 == 500
    assert req.y1 == 800
    assert req.start_time is None
    assert req.end_time is None


def test_asset_presigned_url_request_with_time_fields():
    asset_id = uuid.uuid4()
    req = AssetPresignedUrlRequest(
        document_id="doc1",
        asset_id=asset_id,
        start_time=1.5,
        end_time=10.0,
    )
    assert req.start_time == 1.5
    assert req.end_time == 10.0
    assert req.x0 is None


def test_asset_presigned_url_request_defaults_none():
    asset_id = uuid.uuid4()
    req = AssetPresignedUrlRequest(document_id="doc1", asset_id=asset_id)
    assert req.x0 is None
    assert req.y0 is None
    assert req.x1 is None
    assert req.y1 is None
    assert req.start_time is None
    assert req.end_time is None


def test_asset_presigned_url_request_crop_bounds_validation():
    with pytest.raises(ValidationError):
        AssetPresignedUrlRequest(
            document_id="doc1",
            asset_id=uuid.uuid4(),
            x0=1001,
        )
    with pytest.raises(ValidationError):
        AssetPresignedUrlRequest(
            document_id="doc1",
            asset_id=uuid.uuid4(),
            start_time=-1.0,
        )


# ── AssetPresignedUrlDetails nullable presigned_url ──────────────────────


def test_asset_presigned_url_details_null_url():
    detail = AssetPresignedUrlDetails(
        document_id="doc1",
        asset_id=uuid.uuid4(),
        presigned_url=None,
    )
    assert detail.presigned_url is None


def test_asset_presigned_url_details_with_url():
    detail = AssetPresignedUrlDetails(
        document_id="doc1",
        asset_id=uuid.uuid4(),
        presigned_url="https://example.com/asset",
    )
    assert detail.presigned_url == "https://example.com/asset"


# ── UploadFilePresignedUrl models ────────────────────────────────────────


def test_upload_file_presigned_url_request():
    req = UploadFilePresignedUrlRequest(
        content_type=ContentTypeEnum.ApplicationPdf,
    )
    assert req.content_type == ContentTypeEnum.ApplicationPdf


def test_upload_file_presigned_url_response():
    test_uuid = uuid.uuid4()
    resp = UploadFilePresignedUrlResponse(
        file_data_uuid=test_uuid,
        presigned_url="https://storage.example.com/upload?sig=abc",
        expires_in_seconds=3600,
    )
    assert resp.file_data_uuid == test_uuid
    assert resp.presigned_url == "https://storage.example.com/upload?sig=abc"
    assert resp.expires_in_seconds == 3600


# ── Client: upload_document with file_data_uuid ──────────────────────────


@respx.mock
def test_upload_document_via_uuid(client: CompassClient, respx_mock: MockRouter):
    upload_id = uuid.uuid4()
    document_id = "test_document_id"
    file_uuid = uuid.uuid4()

    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/upload").mock(
        return_value=httpx.Response(200, json={"upload_id": str(upload_id), "document_ids": [document_id]})
    )

    result = client.upload_document_via_uuid(
        index_name="test_index",
        filename="test.pdf",
        file_data_uuid=file_uuid,
        content_length_bytes=4096,
        content_type=ContentTypeEnum.ApplicationPdf,
        document_id=document_id,
    )

    assert route.called
    assert result == UploadDocumentsResult(upload_id=upload_id, document_ids=[document_id])

    request_body = json.loads(route.calls.last.request.content)
    doc_payload = request_body["documents"][0]
    assert doc_payload["file_data_uuid"] == str(file_uuid)
    assert doc_payload.get("content_encoded_bytes") is None
    assert doc_payload["content_length_bytes"] == 4096


# ── Client: get_upload_presigned_url ─────────────────────────────────────


@respx.mock
def test_get_upload_presigned_url(client: CompassClient, respx_mock: MockRouter):
    file_uuid = uuid.uuid4()
    route = respx_mock.post("http://test.com/v1/indexes/test_index/documents/upload/presigned_url").mock(
        return_value=httpx.Response(
            200,
            json={
                "file_data_uuid": str(file_uuid),
                "presigned_url": "https://storage.example.com/upload?sig=xyz",
                "expires_in_seconds": 3600,
                "content_type": "application/pdf",
            },
        )
    )

    result = client.get_upload_presigned_url(
        index_name="test_index",
        content_type=ContentTypeEnum.ApplicationPdf,
    )

    assert route.called
    assert result.file_data_uuid == file_uuid
    assert result.presigned_url == "https://storage.example.com/upload?sig=xyz"
    assert result.expires_in_seconds == 3600

    request_body = json.loads(route.calls.last.request.content)
    assert request_body["content_type"] == "application/pdf"


# Retention policy tests


@mock_endpoint(
    "GET",
    "http://test.com/v1/indexes/test_index/retention",
    200,
    response_body={
        "retention_policy": {
            "retention_type": "sliding",
            "ttl_days": 365,
            "grace_period_days": 7,
            "enabled": True,
        }
    },
)
def test_get_retention_policy_unwraps_envelope(client: CompassClient):
    # Regression test for https://github.com/cohere-ai/cohere-compass-sdk/issues/204:
    # the server wraps the policy under a `retention_policy` key, which used to
    # raise a pydantic ValidationError.
    policy = client.get_retention_policy(index_name="test_index")

    assert policy == RetentionPolicy(
        retention_type=RetentionType.Sliding,
        ttl_days=365,
        grace_period_days=7,
        enabled=True,
    )


@mock_endpoint(
    "GET",
    "http://test.com/v1/indexes/test_index/retention",
    200,
    response_body={"retention_policy": None},
)
def test_get_retention_policy_returns_none_when_envelope_null(client: CompassClient):
    assert client.get_retention_policy(index_name="test_index") is None


@mock_endpoint(
    "GET",
    "http://test.com/v1/indexes/test_index/retention",
    200,
    response_body=None,
)
def test_get_retention_policy_returns_none_when_body_empty(client: CompassClient):
    assert client.get_retention_policy(index_name="test_index") is None


@mock_endpoint(
    "PUT",
    "http://test.com/v1/indexes/test_index/retention",
    200,
    expected_request_body={
        "retention_type": "fixed",
        "ttl_days": 30,
        "grace_period_days": 7,
        "enabled": True,
    },
)
def test_set_retention_policy_sends_correct_body(client: CompassClient):
    client.set_retention_policy(
        index_name="test_index",
        retention_policy=RetentionPolicy(
            retention_type=RetentionType.Fixed,
            ttl_days=30,
        ),
    )


@mock_endpoint(
    "DELETE",
    "http://test.com/v1/indexes/test_index/retention",
    200,
)
def test_delete_retention_policy_sends_request(client: CompassClient):
    client.delete_retention_policy(index_name="test_index")
