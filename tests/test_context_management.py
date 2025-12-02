"""Tests for context management behavior in all client classes."""

# pyright: reportPrivateUsage=false

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from cohere_compass.clients import CompassClient, CompassParserClient
from cohere_compass.clients.compass_async import CompassAsyncClient
from cohere_compass.clients.parser_async import CompassParserAsyncClient

# =============================================================================
# Fixtures
# =============================================================================

ALL_CLIENTS: list[tuple[type, dict[str, Any], bool]] = [
    (CompassClient, {"index_url": "http://test.com"}, False),
    (CompassParserClient, {"parser_url": "http://test.com"}, False),
    (CompassAsyncClient, {"index_url": "http://test.com"}, True),
    (CompassParserAsyncClient, {"parser_url": "http://test.com"}, True),
]


@pytest.fixture(
    params=ALL_CLIENTS,
    ids=[
        "CompassClient",
        "CompassParserClient",
        "CompassAsyncClient",
        "CompassParserAsyncClient",
    ],
)
def client_factory(request: pytest.FixtureRequest) -> tuple[Callable[..., Any], bool]:
    """Factory fixture for all clients. Returns (factory, is_async)."""
    client_cls, default_kwargs, is_async = request.param

    def factory(**kwargs: Any) -> Any:
        return client_cls(**(default_kwargs | kwargs))

    return factory, is_async


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
async def test_context_manager_closes_own_httpx_client(
    client_factory: tuple[Callable[..., Any], bool],
):
    """Context manager should close httpx client it created."""
    factory, is_async = client_factory

    if is_async:
        async with factory() as client:
            assert client._own_httpx_client is True
            assert client._closed is False
        assert client._closed is True
    else:
        with factory() as client:
            assert client._own_httpx_client is True
            assert client._closed is False
        assert client._closed is True


@pytest.mark.asyncio
async def test_context_manager_does_not_close_external_httpx_client(
    client_factory: tuple[Callable[..., Any], bool],
):
    """Context manager should not close externally provided httpx client."""
    factory, is_async = client_factory

    if is_async:
        external_httpx = httpx.AsyncClient()
        try:
            async with factory(httpx_client=external_httpx) as client:
                assert client._own_httpx_client is False
                assert client._closed is False

            assert client._closed is False
            assert not external_httpx.is_closed
        finally:
            await external_httpx.aclose()
    else:
        external_httpx = httpx.Client()
        try:
            with factory(httpx_client=external_httpx) as client:
                assert client._own_httpx_client is False
                assert client._closed is False

            assert client._closed is False
            assert not external_httpx.is_closed
        finally:
            external_httpx.close()


@pytest.mark.asyncio
async def test_close_is_idempotent(client_factory: tuple[Callable[..., Any], bool]):
    """Calling close() multiple times should not cause errors."""
    factory, is_async = client_factory
    client = factory()
    assert client._closed is False

    if is_async:
        await client.close()
        assert client._closed is True
        await client.close()
    else:
        client.close()
        assert client._closed is True
        client.close()

    assert client._closed is True


@pytest.mark.asyncio
async def test_close_after_context_manager_is_safe(
    client_factory: tuple[Callable[..., Any], bool],
):
    """Calling close() after context manager exits should be safe."""
    factory, is_async = client_factory

    if is_async:
        async with factory() as client:
            pass
        await client.close()
    else:
        with factory() as client:
            pass
        client.close()

    assert client._closed is True


@pytest.mark.asyncio
async def test_manual_close_then_context_exit_is_safe(
    client_factory: tuple[Callable[..., Any], bool],
):
    """Manual close inside context manager should not cause double-close."""
    factory, is_async = client_factory

    if is_async:
        async with factory() as client:
            await client.close()
            assert client._closed is True
    else:
        with factory() as client:
            client.close()
            assert client._closed is True

    assert client._closed is True


@pytest.mark.asyncio
async def test_context_manager_returns_self(
    client_factory: tuple[Callable[..., Any], bool],
):
    """Context manager __enter__/__aenter__ should return the client instance."""
    factory, is_async = client_factory
    client = factory()

    if is_async:
        async with client as ctx:
            assert ctx is client
    else:
        with client as ctx:
            assert ctx is client
        client.close()
