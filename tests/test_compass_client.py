from compass_sdk.compass import CompassClient, CompassDocument


def test_delete_url_formatted_with_doc_and_index(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.delete_document(index_name="test_index", doc_id="test_id")
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes/test_index/documents/test_id"
    assert requests_mock.request_history[0].method == "DELETE"


def test_create_index_formatted_with_index(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.create_index(index_name="test_index")
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes/test_index"
    assert requests_mock.request_history[0].method == "PUT"


def test_put_documents_payload_and_url_exist(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.insert_docs(index_name="test_index", docs=iter([CompassDocument()]))
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes/test_index/documents"
    assert requests_mock.request_history[0].method == "PUT"
    assert "docs" in requests_mock.request_history[0].json()


def test_put_document_payload_and_url_exist(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.insert_doc(index_name="test_index", doc=CompassDocument())
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes/test_index/documents"
    assert requests_mock.request_history[0].method == "PUT"
    assert "docs" in requests_mock.request_history[0].json()


def test_list_indices_is_valid(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.list_indexes()
    assert requests_mock.request_history[0].method == "GET"
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes"


def test_get_documents_is_valid(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.get_document(index_name="test_index", doc_id="test_id")
    assert requests_mock.request_history[0].method == "GET"
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes/test_index/documents/test_id"


def test_refresh_is_valid(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.refresh(index_name="test_index")
    assert requests_mock.request_history[0].method == "POST"
    assert requests_mock.request_history[0].url == "http://test.com/api/v1/indexes/test_index/refresh"


def test_add_context_is_valid(requests_mock):
    compass = CompassClient(index_url="http://test.com")
    compass.add_context(index_name="test_index", doc_id="test_id", context={"fake": "context"})
    assert requests_mock.request_history[0].method == "POST"
    assert (
        requests_mock.request_history[0].url
        == "http://test.com/api/v1/indexes/test_index/documents/add_context/test_id"
    )
    assert requests_mock.request_history[0].body == b'{"fake": "context"}'
