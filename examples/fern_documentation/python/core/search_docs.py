from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"
DOCUMENT_ID = "<DOCUMENT_ID>"
SEARCH_QUERY = "<SEARCH_QUERY>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    r = compass_client.search_documents(index_name=INDEX_NAME, query=SEARCH_QUERY)
except Exception as e:
    raise Exception(f"Failed to search documents: {e}")

print(r.hits)
