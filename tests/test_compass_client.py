import json
from collections.abc import Callable
from typing import Any, Literal

import httpx
import pytest
import respx
from pydantic import ValidationError
from respx import MockRouter

from cohere_compass.clients import CompassClient
from cohere_compass.models import CompassDocument
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.documents import (
    AssetType,
    CompassDocumentMetadata,
    DocumentAttributes,
)
from cohere_compass.models.indexes import IndexInfo

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


# TODO - Re-enable these.
# def test_create_index_with_invalid_name(requests_mock: Mocker):
#     compass = CompassClient(index_url="http://test.com")
#     with pytest.raises(ValueError) as exc_info:
#         compass.create_index(index_name="there/are/slashes/here")
#     assert "Invalid index name" in str(exc_info)
#     assert len(requests_mock.request_history) == 0
#
#
# def test_create_index_400s_propagated_to_caller(requests_mock: Mocker):
#     requests_mock.put(
#         "http://test.com/v1/indexes/test-index",
#         status_code=404,
#         json={"error": "invalid request"},
#     )
#     compass = CompassClient(index_url="http://test.com")
#     with pytest.raises(CompassClientError) as exc_info:
#         compass.create_index(index_name="test-index")
#     assert "invalid request" in str(exc_info)
#     assert len(requests_mock.request_history) == 1


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


# TODO - Enable these tests.
# def test_direct_search_with_sort_by_single_field(requests_mock_200s: Mocker):
#     # Register mock response for the direct_search endpoint
#     requests_mock_200s.post(
#         "http://test.com/v1/indexes/test_index/_direct_search",
#         json={"hits": [], "scroll_id": "test_scroll_id"},
#     )
#
#     compass = CompassClient(index_url="http://test.com")
#     sort_by = [SortBy(field="created_at", order="desc")]
#     compass.direct_search(
#         index_name="test_index", query={"match_all": {}}, sort_by=sort_by
#     )
#     assert requests_mock_200s.request_history[0].method == "POST"
#     assert (
#         requests_mock_200s.request_history[0].url
#         == "http://test.com/v1/indexes/test_index/_direct_search"
#     )
#     payload = requests_mock_200s.request_history[0].json()
#     assert "sort_by" in payload
#     assert payload["sort_by"] == [{"field": "created_at", "order": "desc"}]
#
#
# def test_direct_search_with_sort_by_multiple_fields(requests_mock_200s: Mocker):
#     # Register mock response for the direct_search endpoint
#     requests_mock_200s.post(
#         "http://test.com/v1/indexes/test_index/_direct_search",
#         json={"hits": [], "scroll_id": "test_scroll_id"},
#     )
#
#     compass = CompassClient(index_url="http://test.com")
#     sort_by = [
#         SortBy(field="created_at", order="desc"),
#         SortBy(field="score", order="asc"),
#     ]
#     compass.direct_search(
#         index_name="test_index", query={"match_all": {}}, sort_by=sort_by
#     )
#     assert requests_mock_200s.request_history[0].method == "POST"
#     assert (
#         requests_mock_200s.request_history[0].url
#         == "http://test.com/v1/indexes/test_index/_direct_search"
#     )
#     payload = requests_mock_200s.request_history[0].json()
#     assert "sort_by" in payload
#     assert payload["sort_by"] == [
#         {"field": "created_at", "order": "desc"},
#         {"field": "score", "order": "asc"},
#     ]
#
#
# def test_direct_search_without_sort_by(requests_mock_200s: Mocker):
#     # Register mock response for the direct_search endpoint
#     requests_mock_200s.post(
#         "http://test.com/v1/indexes/test_index/_direct_search",
#         json={"hits": [], "scroll_id": "test_scroll_id"},
#     )
#
#     compass = CompassClient(index_url="http://test.com")
#     compass.direct_search(index_name="test_index", query={"match_all": {}})
#     assert requests_mock_200s.request_history[0].method == "POST"
#     assert (
#         requests_mock_200s.request_history[0].url
#         == "http://test.com/v1/indexes/test_index/_direct_search"
#     )
#     payload = requests_mock_200s.request_history[0].json()
#     assert "sort_by" not in payload or payload["sort_by"] is None
#


# def test_proper_handling_of_returned_tuple_from_parser():
#     compass = CompassClient(index_url="http://test.com")
#     docs = compass.insert_docs(
#         index_name="test_index",
#         docs=iter([("test_file.pdf", "time out")]),  # type: ignore
#     )
#     assert docs == [{"test_file.pdf": "time out"}]
#
