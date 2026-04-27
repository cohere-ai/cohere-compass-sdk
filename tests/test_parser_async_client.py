import pytest
from aioresponses import aioresponses

from cohere_compass.clients.parser_async import CompassParserAsyncClient

pytestmark = pytest.mark.asyncio


async def test_process_file_bytes() -> None:
    client = CompassParserAsyncClient(parser_url="http://test.com")
    try:
        with aioresponses() as m:
            m.post(
                "http://test.com/v1/process_file",
                status=200,
                payload={"docs": []},
                headers={"Content-Type": "application/json"},
            )
            await client.process_file_bytes(filename="test.pdf", file_bytes=b"0")

            request = next(iter(m.requests.values()))[0]
            headers = request.kwargs["headers"]
            # aiohttp sets Content-Type when sending FormData
            assert "Authorization" not in (headers or {})
    finally:
        await client.aclose()


async def test_process_file_bytes_with_auth() -> None:
    client = CompassParserAsyncClient(parser_url="http://test.com", bearer_token="secret")
    try:
        with aioresponses() as m:
            m.post(
                "http://test.com/v1/process_file",
                status=200,
                payload={"docs": []},
                headers={"Content-Type": "application/json"},
            )
            await client.process_file_bytes(filename="test.pdf", file_bytes=b"0")

            request = next(iter(m.requests.values()))[0]
            headers = request.kwargs["headers"]
            assert headers is not None
            assert "Authorization" in headers
            assert headers["Authorization"] == "Bearer secret"
    finally:
        await client.aclose()
