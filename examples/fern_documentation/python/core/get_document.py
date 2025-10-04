from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"
DOCUMENT_ID = "<DOCUMENT_ID>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    document = compass_client.get_document(
        index_name=INDEX_NAME, document_id=DOCUMENT_ID
    )
    # Do something with the document
except Exception as e:
    raise Exception(f"Failed to get doc: {e}")
