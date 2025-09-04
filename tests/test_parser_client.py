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


# TODO - Re-enable this.
# def test_process_file_with_invalid_id(requests_mock: Mocker) -> None:
#     client = CompassParserClient(parser_url="mock://test.com", bearer_token="secret")
#     with pytest.raises(pydantic.ValidationError) as exc_info:
#         client.process_file_bytes(
#             filename="test.pdf",
#             file_bytes=b"0",
#             file_id="filesystem://some/file/path/to/test.pdf",
#         )
#     assert "String should match pattern" in str(exc_info)
#     assert len(requests_mock.request_history) == 0
#
