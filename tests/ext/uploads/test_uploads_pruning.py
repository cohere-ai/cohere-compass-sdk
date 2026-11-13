import asyncio
from typing import cast
from unittest.mock import AsyncMock, Mock

import pytest

from cohere_compass.clients import CompassAsyncClient
from cohere_compass.exceptions import CompassError
from cohere_compass.ext.uploads import pruning
from cohere_compass.ext.uploads.pruning import (
    PruneResult,
    find_stale_documents,
    prune_index,
)
from cohere_compass.models.search import (
    DirectSearchResponse,
    RetrievedChunkExtended,
    SortBy,
)

FIRST_CHUNKS = {"range": {"sort_id": {"lte": 1}}}


def _chunk(document_id: str, number: int) -> RetrievedChunkExtended:
    return RetrievedChunkExtended(
        chunk_id=f"{document_id}-{number}",
        sort_id=number,
        parent_document_id=document_id,
        path="file.pdf",
        content={},
        score=0,
        document_id=document_id,
    )


def _page(scroll_id: str | None, *document_ids: str) -> DirectSearchResponse:
    return DirectSearchResponse(
        hits=[_chunk(document_id, 0) for document_id in document_ids],
        scroll_id=scroll_id,
    )


def _client(*pages: DirectSearchResponse) -> Mock:
    client = Mock(spec=CompassAsyncClient)
    client.direct_search = AsyncMock(return_value=pages[0])
    client.direct_search_scroll = AsyncMock(side_effect=list(pages[1:]))
    client.delete_documents = AsyncMock(return_value=None)
    return client


@pytest.mark.asyncio
async def test_finds_stale_documents_across_scroll_pages() -> None:
    client = _client(
        _page("scroll-1", "a", "a", "keep"),
        _page("scroll-2", "keep", "c"),
        _page("scroll-3"),
    )

    stale = await find_stale_documents(cast(CompassAsyncClient, client), "docs", ["keep"])

    assert stale == ("a", "c")
    client.direct_search.assert_awaited_once_with(
        index_name="docs",
        query=FIRST_CHUNKS,
        sort_by=[SortBy(field="_doc", order="asc")],
        size=1000,
        scroll="1m",
    )
    assert client.direct_search_scroll.await_args_list[1].kwargs == {
        "index_name": "docs",
        "scroll_id": "scroll-2",
        "scroll": "1m",
    }


@pytest.mark.asyncio
async def test_stops_when_a_page_has_no_scroll_id() -> None:
    client = _client(_page(None, "a"))

    stale = await find_stale_documents(cast(CompassAsyncClient, client), "docs", [])

    assert stale == ("a",)
    client.direct_search_scroll.assert_not_awaited()


@pytest.mark.asyncio
async def test_deletes_stale_documents_in_bulk_and_records_failed_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pruning, "MAX_BULK_DELETE_DOCUMENT_IDS", 2)
    client = _client(_page(None, "keep", "a", "b", "c", "d", "e"))
    batches: list[list[str]] = []

    async def delete_documents(*, index_name: str, document_ids: list[str]) -> object:
        batches.append(document_ids)
        if "c" in document_ids:
            raise CompassError("unavailable")
        return object()

    client.delete_documents = AsyncMock(side_effect=delete_documents)

    result = await prune_index(cast(CompassAsyncClient, client), "docs", {"keep"}, workers=2)

    assert sorted(batches) == [["a", "b"], ["c", "d"], ["e"]]
    assert result.stale_document_ids == ("a", "b", "c", "d", "e")
    assert result.deleted_document_ids == ("a", "b", "e")
    assert result.errors == {"c": "unavailable", "d": "unavailable"}
    assert not result.succeeded and not result.dry_run


@pytest.mark.asyncio
async def test_deletions_use_at_most_the_given_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pruning, "MAX_BULK_DELETE_DOCUMENT_IDS", 1)
    client = _client(_page(None, *(f"doc-{number}" for number in range(10))))
    active = maximum = 0

    async def delete_documents(*, index_name: str, document_ids: list[str]) -> object:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0)
        active -= 1
        return object()

    client.delete_documents = AsyncMock(side_effect=delete_documents)

    result = await prune_index(cast(CompassAsyncClient, client), "docs", (), workers=3)

    assert len(result.deleted_document_ids) == 10
    assert maximum == 3


@pytest.mark.asyncio
async def test_nothing_stale_sends_no_deletions() -> None:
    client = _client(_page(None, "keep"))

    result = await prune_index(cast(CompassAsyncClient, client), "docs", {"keep"}, workers=2)

    assert result == PruneResult(stale_document_ids=())
    client.delete_documents.assert_not_called()


@pytest.mark.asyncio
async def test_dry_run_finds_stale_documents_without_deleting() -> None:
    client = _client(_page(None, "keep", "stale"))

    result = await prune_index(cast(CompassAsyncClient, client), "docs", {"keep"}, workers=2, dry_run=True)

    assert result == PruneResult(stale_document_ids=("stale",), dry_run=True)
    client.delete_documents.assert_not_called()


def test_prune_without_errors_succeeds() -> None:
    assert PruneResult(stale_document_ids=("a",), deleted_document_ids=("a",)).succeeded
