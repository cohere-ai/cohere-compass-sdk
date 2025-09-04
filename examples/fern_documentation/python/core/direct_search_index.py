from typing import Optional

from cohere_compass.clients import CompassClient
from cohere_compass.models import RetrievedChunkExtended

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"
DOCUMENT_ID = "<DOCUMENT_ID>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

query = {"query": {"match_all": {}}}


def call_direct_search_scroll(
    hits: list[RetrievedChunkExtended],
    scroll_id: Optional[str] = None,
) -> list[RetrievedChunkExtended]:
    if hits is None:
        hits = []

    if scroll_id is None:
        return hits

    results_scroll = compass_client.direct_search_scroll(
        index_name=INDEX_NAME,
        scroll_id=scroll_id,
        scroll="1m",
    )

    hits.extend(results_scroll.hits)
    return call_direct_search_scroll(hits=hits, scroll_id=results.scroll_id)


results = compass_client.direct_search(
    index_name=INDEX_NAME,
    query=query,
    scroll="1m",
    size=100,
)

hits = call_direct_search_scroll(hits=results.hits, scroll_id=results.scroll_id)
