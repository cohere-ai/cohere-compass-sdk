from cohere_compass.clients import CompassClient

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

r = compass_client.list_indexes()
if r.error:
    raise Exception(f"Failed to list indexes: {r.error}")

indexes = r.result["indexes"]
for index in indexes:
    print(f"Index name: {index['name']}")
    print(f"Chunk count: {index['count']}")
    print(f"Document count: {index['parent_doc_count']}")
