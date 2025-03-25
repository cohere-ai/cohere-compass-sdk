from requests_mock import Mocker

from cohere.compass.clients import CompassParserClient


def test_process_file_bytes(requests_mock: Mocker) -> None:
    requests_mock.post("mock://test.com/v1/process_file", json={"docs": []})
    client = CompassParserClient(parser_url="mock://test.com")
    client.process_file_bytes(filename="test.pdf", file_bytes=b"0")
    assert requests_mock.request_history[0].method == "POST"
    assert requests_mock.request_history[0].url == "mock://test.com/v1/process_file"
    headers = requests_mock.request_history[0].headers
    assert "multipart/form-data" in headers["Content-Type"]
    assert "Authorization" not in headers


def test_process_file_bytes_with_auth(requests_mock: Mocker) -> None:
    requests_mock.post("mock://test.com/v1/process_file", json={"docs": []})
    client = CompassParserClient(parser_url="mock://test.com", bearer_token="secret")
    client.process_file_bytes(filename="test.pdf", file_bytes=b"0")
    assert requests_mock.request_history[0].method == "POST"
    assert requests_mock.request_history[0].url == "mock://test.com/v1/process_file"
    headers = requests_mock.request_history[0].headers
    assert "multipart/form-data" in headers["Content-Type"]
    assert "Authorization" in headers
