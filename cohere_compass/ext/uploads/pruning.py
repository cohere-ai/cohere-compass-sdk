"""
Make an index mirror an upload source by deleting documents the source lacks.

Compass has no endpoint that lists an index's documents, so the document IDs
are collected from a scrolled direct search over each document's first chunk.
That listing misses any document with no chunk whose sort_id is 0 or 1, for
example one still being processed, or a PDF whose first page failed to parse.
A missed document is never deleted and never reported as stale, so the error
leaves stale documents in the index but never deletes one wrongly.

Pruning is meant for indexes of up to tens of thousands of documents and does
not scale to millions of deletions. It holds every expected and every stale
document ID in memory and lists the index one page of chunks at a time.
Indexes that large need a different tool, such as a server-side operation.

TODO: Once the Compass API can list an index's documents, replace the search
in `find_stale_documents` with that listing.
"""

import asyncio
from collections.abc import Collection
from typing import Any

from pydantic import BaseModel, ConfigDict

from cohere_compass.clients import CompassAsyncClient
from cohere_compass.constants import MAX_BULK_DELETE_DOCUMENT_IDS
from cohere_compass.exceptions import CompassError
from cohere_compass.models.search import SortBy

_PAGE_SIZE = 1000
_SCROLL_KEEPALIVE = "1m"
# Each document's first chunk has sort_id 0, or 1 for a PDF, whose chunks are
# numbered by page. Searching only those chunks lists the documents while
# reading one or two chunks per document instead of all of them.
_FIRST_CHUNKS: dict[str, Any] = {"range": {"sort_id": {"lte": 1}}}


class PruneResult(BaseModel):
    """
    Documents found in the index but not in the source, and what became of them.

    A dry run deletes nothing, so its `deleted_document_ids` is always empty.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stale_document_ids: tuple[str, ...]
    deleted_document_ids: tuple[str, ...] = ()
    errors: dict[str, str] = {}
    dry_run: bool = False

    @property
    def succeeded(self) -> bool:
        """Whether every stale document was deleted, or would be in a dry run."""
        return not self.errors


async def find_stale_documents(
    client: CompassAsyncClient, index_name: str, expected: Collection[str]
) -> tuple[str, ...]:
    """
    Return the IDs of documents in *index_name* that are not in *expected*.

    Each page of the listing is compared as it arrives, so only the stale IDs
    are kept rather than every ID in the index. See the module docstring for
    the documents the listing misses.
    """
    expected_ids = set(expected)
    response = await client.direct_search(
        index_name=index_name,
        query=_FIRST_CHUNKS,
        sort_by=[SortBy(field="_doc", order="asc")],
        size=_PAGE_SIZE,
        scroll=_SCROLL_KEEPALIVE,
    )
    stale: set[str] = set()
    while response.hits:
        stale.update(hit.document_id for hit in response.hits if hit.document_id not in expected_ids)
        if response.scroll_id is None:
            break
        response = await client.direct_search_scroll(
            index_name=index_name,
            scroll_id=response.scroll_id,
            scroll=_SCROLL_KEEPALIVE,
        )
    return tuple(sorted(stale))


async def prune_index(
    client: CompassAsyncClient,
    index_name: str,
    expected: Collection[str],
    *,
    workers: int,
    dry_run: bool = False,
) -> PruneResult:
    """
    Delete every document of *index_name* that is not in *expected*.

    A dry run only finds the stale documents. Documents are deleted in bulk
    requests, at most *workers* at a time. A document that is already gone
    counts as deleted. A failed request is recorded against each document ID
    of its batch rather than raised, so one failed batch does not stop the
    others.
    """
    stale = await find_stale_documents(client, index_name, expected)
    if dry_run:
        return PruneResult(stale_document_ids=stale, dry_run=True)
    # A fixed set of workers pulls batches from one queue, so the number of
    # requests in flight stays bounded by `workers` however many are stale.
    queue: asyncio.Queue[list[str]] = asyncio.Queue()
    for start in range(0, len(stale), MAX_BULK_DELETE_DOCUMENT_IDS):
        queue.put_nowait(list(stale[start : start + MAX_BULK_DELETE_DOCUMENT_IDS]))
    deleted: list[str] = []
    errors: dict[str, str] = {}

    async def worker() -> None:
        while not queue.empty():
            batch = queue.get_nowait()
            try:
                await client.delete_documents(index_name=index_name, document_ids=batch)
            except CompassError as error:
                # A failed request deleted none of its batch.
                errors.update(dict.fromkeys(batch, str(error)))
                continue
            # Both outcomes, deleted and not found, leave the document gone.
            deleted.extend(batch)

    await asyncio.gather(*(worker() for _ in range(min(workers, queue.qsize()))))
    return PruneResult(
        stale_document_ids=stale,
        deleted_document_ids=tuple(sorted(deleted)),
        errors=errors,
    )
