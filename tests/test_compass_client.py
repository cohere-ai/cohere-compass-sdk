import json
from collections.abc import Callable
from typing import Any, Literal

import httpx
import respx
from respx import MockRouter

from cohere_compass.clients import CompassClient
from cohere_compass.models import CompassDocument
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.documents import DocumentAttributes

HTTPMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def simple_compass_client_test(
    method: HTTPMethod,
    url: str,
    status_code: int = 200,
    expected_request_body: dict[Any, Any] | None = None,
):
    def decorator(test_func: Callable[..., Any]) -> Callable[..., Any]:
        @respx.mock(assert_all_mocked=True)
        # @functools.wraps(test_func)
        def wrapper(respx_mock: MockRouter, *args: Any, **kwargs: Any):
            route = getattr(respx_mock, method.lower())(url).mock(
                return_value=httpx.Response(status_code)
            )

            test_func(*args, **kwargs)

            assert route.called, f"Expected {method} {url} to be called"
            assert route.call_count == 1, f"Expected {method} {url} to be called once"

            if expected_request_body is not None:
                request_body = json.loads(route.calls.last.request.content)
                assert (
                    request_body == expected_request_body
                ), f"Expected JSON body {expected_request_body}, got {request_body}"

        return wrapper  # type: ignore

    return decorator


@simple_compass_client_test(
    "DELETE",
    "http://test.com/api/v1/indexes/test_index/documents/test_id",
    201,
)
def test_delete_url_formatted_with_doc_and_index():
    # Running...
    compass = CompassClient(index_url="http://test.com")
    compass.delete_document(index_name="test_index", document_id="test_id")


@simple_compass_client_test(
    "PUT",
    "http://test.com/api/v1/indexes/test_index",
    200,
)
def test_create_index_formatted_with_index():
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(index_name="test_index")


@simple_compass_client_test(
    "PUT",
    "http://test.com/api/v1/indexes/test_index",
    200,
    {"number_of_shards": 5},
)
def test_create_index_with_index_config():
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(
        index_name="test_index", index_config=IndexConfig(number_of_shards=5)
    )


@simple_compass_client_test(
    "PUT",
    "http://test.com/api/v1/indexes/test_index/documents",
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


@simple_compass_client_test(
    "PUT",
    "http://test.com/api/v1/indexes/test_index/documents",
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


@simple_compass_client_test(
    "GET",
    "http://test.com/api/v1/indexes",
    200,
)
def test_list_indices_is_valid():
    # Running...
    compass = CompassClient(index_url="http://test.com")
    compass.list_indexes()


@simple_compass_client_test(
    "GET",
    "http://test.com/api/v1/indexes/test_index/documents/test_id",
    200,
)
def test_get_documents_is_valid():
    compass = CompassClient(index_url="http://test.com")
    compass.get_document(index_name="test_index", document_id="test_id")


@simple_compass_client_test(
    "POST",
    "http://test.com/api/v1/indexes/test_index/_refresh",
    200,
)
def test_refresh_is_valid():
    compass = CompassClient(index_url="http://test.com")
    compass.refresh_index(index_name="test_index")


@simple_compass_client_test(
    "POST",
    "http://test.com/api/v1/indexes/test_index/documents/test_id/_add_attributes",
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
        "http://test.com/api/v1/indexes/test_index/documents/test_id/assets/test_asset_id"
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
        "http://test.com/api/v1/indexes/test_index/documents/test_id/assets/test_asset_id"
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
        "http://test.com/api/v1/indexes/test_index/documents/test_id/assets/test_asset_id"
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
        "http://test.com/api/v1/indexes/test_index/_direct_search"
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
        f"http://test.com/api/v1/indexes/{index_name}/_direct_search/scroll",
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


def test_proper_handling_of_returned_tuple_from_parser():
    compass = CompassClient(index_url="http://test.com")
    docs = compass.insert_docs(
        index_name="test_index",
        docs=iter([("test_file.pdf", "time out")]),  # type: ignore
    )
    assert docs == [{"test_file.pdf": "time out"}]
