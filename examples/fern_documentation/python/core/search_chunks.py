from cohere.compass.clients import CompassClient

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...
DOCUMENT_ID = ...
SEARCH_QUERY = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

r = compass_client.search_chunks(index_name=INDEX_NAME, query=SEARCH_QUERY)

if r.error:
    raise Exception(f"Failed to search: {r.error}")

print(r.hits)
