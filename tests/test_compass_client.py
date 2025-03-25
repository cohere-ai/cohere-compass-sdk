import pytest
from requests_mock import Mocker

from cohere_compass.clients import CompassClient
from cohere_compass.exceptions import CompassClientError
from cohere_compass.models import CompassDocument
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.documents import DocumentAttributes


def test_delete_url_formatted_with_doc_and_index(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.delete_document(index_name="test_index", document_id="test_id")
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/documents/test_id"
    )
    assert requests_mock.request_history[0].method == "DELETE"


def test_create_index_formatted_with_index(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(index_name="test_index")
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index"
    )
    assert requests_mock.request_history[0].method == "PUT"


def test_create_index_with_index_config(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(
        index_name="test_index", index_config=IndexConfig(number_of_shards=5)
    )
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index"
    )
    assert requests_mock.request_history[0].method == "PUT"
    assert requests_mock.request_history[0].json() == {"number_of_shards": 5}


def test_put_documents_payload_and_url_exist(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.insert_docs(index_name="test_index", docs=iter([CompassDocument()]))
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/documents"
    )
    assert requests_mock.request_history[0].method == "PUT"
    assert "documents" in requests_mock.request_history[0].json()


def test_put_document_payload_and_url_exist(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.insert_doc(index_name="test_index", doc=CompassDocument())
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/documents"
    )
    assert requests_mock.request_history[0].method == "PUT"
    assert "documents" in requests_mock.request_history[0].json()


def test_list_indices_is_valid(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.list_indexes()
    assert requests_mock.request_history[0].method == "GET"
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes"


def test_get_documents_is_valid(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.get_document(index_name="test_index", document_id="test_id")
    assert requests_mock.request_history[0].method == "GET"
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/documents/test_id"
    )


def test_refresh_is_valid(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    compass.refresh_index(index_name="test_index")
    assert requests_mock.request_history[0].method == "POST"
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/_refresh"
    )


def test_add_attributes_is_valid(requests_mock: Mocker):
    attrs = DocumentAttributes()
    attrs.fake = "context"
    compass = CompassClient(index_url="http://test.com")
    compass.add_attributes(
        index_name="test_index",
        document_id="test_id",
        attributes=attrs,
    )
    assert requests_mock.request_history[0].method == "POST"
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/documents/test_id/_add_attributes"
    )
    assert requests_mock.request_history[0].body == b'{"fake": "context"}'


def test_search_doc_handles_connection_aborted_error_correctly(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    url = "http://test.com/api/v1/indexes/test_index/documents/_search"
    requests_mock.post(url, exc=ConnectionAbortedError)
    with pytest.raises(CompassClientError):
        compass.search_documents(index_name="test_index", query="test")


def test_search_chunk_handles_connection_aborted_error_correctly(requests_mock: Mocker):
    compass = CompassClient(index_url="http://test.com")
    url = "http://test.com/api/v1/indexes/test_index/documents/_search_chunks"
    requests_mock.post(url, exc=ConnectionAbortedError)
    with pytest.raises(CompassClientError):
        compass.search_chunks(index_name="test_index", query="test")


def test_get_document_asset_with_json_asset(requests_mock: Mocker):
    requests_mock.get(
        "http://test.com/api/v1/indexes/test_index/documents/test_id/assets/test_asset_id",
        json={"test": "test"},
        headers={"Content-Type": "application/json"},
    )
    compass = CompassClient(index_url="http://test.com")
    asset, content_type = compass.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )
    assert isinstance(asset, dict)
    assert asset == {"test": "test"}
    assert content_type == "application/json"


def test_get_document_asset_markdown(requests_mock: Mocker):
    requests_mock.get(
        "http://test.com/api/v1/indexes/test_index/documents/test_id/assets/test_asset_id",
        text="# Test",
        headers={"Content-Type": "text/markdown"},
    )
    compass = CompassClient(index_url="http://test.com")
    asset, content_type = compass.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )
    assert isinstance(asset, str)
    assert asset == "# Test"
    assert content_type == "text/markdown"


def test_get_document_asset_image(requests_mock: Mocker):
    requests_mock.get(
        "http://test.com/api/v1/indexes/test_index/documents/test_id/assets/test_asset_id",
        content=b"test",
        headers={"Content-Type": "image/png"},
    )
    compass = CompassClient(index_url="http://test.com")
    asset, content_type = compass.get_document_asset(
        index_name="test_index", document_id="test_id", asset_id="test_asset_id"
    )
    assert isinstance(asset, bytes)
    assert asset == b"test"
    assert content_type == "image/png"


def test_direct_search_is_valid(requests_mock: Mocker):
    # Register mock response for the direct_search endpoint
    requests_mock.post(
        "http://test.com/api/v1/indexes/test_index/_direct_search",
        json={"hits": [], "scroll_id": "test_scroll_id"},
    )

    compass = CompassClient(index_url="http://test.com")
    compass.direct_search(index_name="test_index", query={"match_all": {}})
    assert requests_mock.request_history[0].method == "POST"
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/_direct_search"
    )
    assert "query" in requests_mock.request_history[0].json()
    assert "size" in requests_mock.request_history[0].json()


def test_direct_search_scroll_is_valid(requests_mock: Mocker):
    # Register mock response for the direct_search_scroll endpoint
    requests_mock.post(
        "http://test.com/api/v1/indexes/_direct_search/scroll",
        json={"hits": [], "scroll_id": "test_scroll_id"},
    )

    compass = CompassClient(index_url="http://test.com")
    compass.direct_search_scroll(scroll_id="test_scroll_id")
    assert requests_mock.request_history[0].method == "POST"
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/_direct_search/scroll"
    )
    request_body = requests_mock.request_history[0].json()
    assert request_body["scroll_id"] == "test_scroll_id"
    assert request_body["scroll"] == "1m"
