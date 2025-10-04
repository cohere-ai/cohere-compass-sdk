from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    r = compass_client.list_indexes()

    indexes = r.indexes
    for index in indexes:
        print(f"Index name: {index.name}")
        print(f"Chunk count: {index.count}")
        print(f"Document count: {index.parent_doc_count}")

except Exception as e:
    raise Exception(f"Failed to list indexes: {e}")
