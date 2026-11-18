"""Opt-in contract acceptance against an isolated running Compass API.

Supply SEARCH_SETTINGS_API_URL, SEARCH_SETTINGS_TEST_INDEX, SEARCH_SETTINGS_WRITER_TOKEN and
SEARCH_SETTINGS_READER_TOKEN. HTTP and authorization are not mocked.
"""

import asyncio
import os
from typing import Any

import pytest

from cohere_compass.clients import CompassAsyncClient, CompassClient
from cohere_compass.exceptions import CompassAuthError, CompassClientError
from cohere_compass.models.indexes import Glossary, GlossaryEntry, PrefixSearchSettings

REQUIRED = (
    "SEARCH_SETTINGS_API_URL",
    "SEARCH_SETTINGS_TEST_INDEX",
    "SEARCH_SETTINGS_WRITER_TOKEN",
    "SEARCH_SETTINGS_READER_TOKEN",
)
pytestmark = pytest.mark.skipif(
    not all(os.environ.get(key) for key in REQUIRED), reason="isolated live API/token inputs missing"
)


async def _call(client: CompassClient | CompassAsyncClient, method: str, **kwargs: Any) -> Any:
    function = getattr(client, method)
    if isinstance(client, CompassAsyncClient):
        return await function(**kwargs)
    return await asyncio.to_thread(function, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_search_settings_crud_preserves_real_caller_permissions(asynchronous: bool) -> None:
    index = os.environ["SEARCH_SETTINGS_TEST_INDEX"]
    assert index.startswith("deleteme-"), "Use an isolated disposable acceptance index"
    client_type = CompassAsyncClient if asynchronous else CompassClient
    writer = client_type(
        index_url=os.environ["SEARCH_SETTINGS_API_URL"], bearer_token=os.environ["SEARCH_SETTINGS_WRITER_TOKEN"]
    )
    reader = client_type(
        index_url=os.environ["SEARCH_SETTINGS_API_URL"], bearer_token=os.environ["SEARCH_SETTINGS_READER_TOKEN"]
    )
    glossary = Glossary(entries=[GlossaryEntry(terms=["SPB"], expansions=["Star Pubs & Bars Limited"])])
    prefix = PrefixSearchSettings(fields=["text"], min_prefix_length=3)
    previous_glossary = await _call(writer, "get_glossary", index_name=index)
    previous_prefix = await _call(writer, "get_prefix_search", index_name=index)
    try:
        await _call(writer, "delete_glossary", index_name=index)
        await _call(writer, "delete_prefix_search", index_name=index)
        assert await _call(writer, "get_glossary", index_name=index) is None
        assert await _call(reader, "get_prefix_search", index_name=index) is None
        await _call(writer, "set_glossary", index_name=index, glossary=glossary)
        await _call(writer, "set_prefix_search", index_name=index, prefix_search=prefix)
        assert await _call(reader, "get_glossary", index_name=index) == glossary
        assert await _call(reader, "get_prefix_search", index_name=index) == prefix
        with pytest.raises(CompassAuthError):
            await _call(reader, "set_glossary", index_name=index, glossary=glossary)
        with pytest.raises(CompassAuthError):
            await _call(reader, "delete_prefix_search", index_name=index)
        with pytest.raises(CompassClientError):
            await _call(writer, "get_glossary", index_name=index + "-missing")
        await _call(writer, "delete_glossary", index_name=index)
        assert await _call(reader, "get_glossary", index_name=index) is None
        assert await _call(reader, "get_prefix_search", index_name=index) == prefix
    finally:
        await _call(writer, "delete_glossary", index_name=index)
        await _call(writer, "delete_prefix_search", index_name=index)
        if previous_glossary is not None:
            await _call(writer, "set_glossary", index_name=index, glossary=previous_glossary)
        if previous_prefix is not None:
            await _call(writer, "set_prefix_search", index_name=index, prefix_search=previous_prefix)
        await _call(writer, "aclose" if asynchronous else "close")
        await _call(reader, "aclose" if asynchronous else "close")
