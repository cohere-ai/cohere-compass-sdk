"""Opt-in contract acceptance against an isolated, running Compass API.

Supply CMPS1285_API_URL, CMPS1285_TEST_INDEX, CMPS1285_WRITER_TOKEN and
CMPS1285_READER_TOKEN. The index must be disposable and accessible to both callers.
HTTP and authorization are not mocked.
"""

import os
from typing import cast

import pytest

from cohere_compass.clients import CompassClient
from cohere_compass.exceptions import CompassAuthError, CompassClientError
from cohere_compass.models.indexes import Glossary, GlossaryEntry, PrefixSearchSettings
from tests.utils import SyncifiedCompassAsyncClient

REQUIRED = ("CMPS1285_API_URL", "CMPS1285_TEST_INDEX", "CMPS1285_WRITER_TOKEN", "CMPS1285_READER_TOKEN")
pytestmark = pytest.mark.skipif(
    not all(os.environ.get(key) for key in REQUIRED), reason="isolated live API/token inputs missing"
)


@pytest.mark.parametrize("client_type", [CompassClient, SyncifiedCompassAsyncClient], ids=["sync", "async"])
def test_search_settings_crud_preserves_real_caller_permissions(
    client_type: type[CompassClient] | type[SyncifiedCompassAsyncClient],
) -> None:
    index = os.environ["CMPS1285_TEST_INDEX"]
    writer = cast(
        CompassClient,
        client_type(index_url=os.environ["CMPS1285_API_URL"], bearer_token=os.environ["CMPS1285_WRITER_TOKEN"]),
    )
    reader = cast(
        CompassClient,
        client_type(index_url=os.environ["CMPS1285_API_URL"], bearer_token=os.environ["CMPS1285_READER_TOKEN"]),
    )
    glossary = Glossary(entries=[GlossaryEntry(terms=["SPB"], expansions=["Star Pubs & Bars Limited"])])
    prefix = PrefixSearchSettings(fields=["text"], min_prefix_length=3)
    try:
        writer.delete_glossary(index_name=index)
        writer.delete_prefix_search(index_name=index)
        assert writer.get_glossary(index_name=index) is None
        assert reader.get_prefix_search(index_name=index) is None
        writer.set_glossary(index_name=index, glossary=glossary)
        writer.set_prefix_search(index_name=index, prefix_search=prefix)
        assert reader.get_glossary(index_name=index) == glossary
        assert reader.get_prefix_search(index_name=index) == prefix
        with pytest.raises(CompassAuthError):
            reader.set_glossary(index_name=index, glossary=glossary)
        with pytest.raises(CompassAuthError):
            reader.delete_prefix_search(index_name=index)
        with pytest.raises(CompassClientError):
            writer.get_glossary(index_name=index + "-missing")
        writer.delete_glossary(index_name=index)
        assert reader.get_glossary(index_name=index) is None
        assert reader.get_prefix_search(index_name=index) == prefix
    finally:
        writer.delete_glossary(index_name=index)
        writer.delete_prefix_search(index_name=index)
        writer.close()
        reader.close()
