import httpx
from respx import MockRouter

from cohere_compass.clients import CompassParserClient


def test_process_file_bytes(respx_mock: MockRouter) -> None:
    # Mock the response for the file processing endpoint
    route = respx_mock.post("http://test.com/v1/process_file").mock(
        return_value=httpx.Response(
            200,
            json={"docs": []},
            headers={"Content-Type": "application/json"},
        )
    )

    client = CompassParserClient(parser_url="http://test.com")
    client.process_file_bytes(filename="test.pdf", file_bytes=b"0")

    # Verify call is made.
    assert route.called
    assert route.call_count

    # Validate headers.
    headers = route.calls.last.request.headers
    assert "multipart/form-data" in headers["Content-Type"]
    assert "Authorization" not in headers


def test_process_file_bytes_with_auth(respx_mock: MockRouter) -> None:
    # Mock the response for the file processing endpoint
    route = respx_mock.post("http://test.com/v1/process_file").mock(
        return_value=httpx.Response(
            200,
            json={"docs": []},
            headers={"Content-Type": "application/json"},
        )
    )

    client = CompassParserClient(parser_url="http://test.com", bearer_token="secret")
    client.process_file_bytes(filename="test.pdf", file_bytes=b"0")

    # Verify call is made.
    assert route.called
    assert route.call_count

    # Validate headers.
    headers = route.calls.last.request.headers
    assert "multipart/form-data" in headers["Content-Type"]
    assert "Authorization" in headers
