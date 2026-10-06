from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"
DOCUMENT_IDS = ["<DOCUMENT_ID_1>", "<DOCUMENT_ID_2>"]

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

result = compass_client.delete_documents(
    index_name=INDEX_NAME, document_ids=DOCUMENT_IDS
)
for r in result.results:
    print(f"{r.document_id}: {r.status.value}")
