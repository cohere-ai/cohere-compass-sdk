from cohere.compass.clients import CompassClient

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...
DOCUMENT_ID = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

r = compass_client.get_document(index_name=INDEX_NAME, document_id=DOCUMENT_ID)

if r.error:
    raise Exception(f"Failed to get doc: {r.error}")

print(r.result.hits)
