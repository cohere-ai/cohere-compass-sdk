from cohere_compass.clients import CompassClient
from cohere_compass.models import DocumentAttributes

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...
DOCUMENT_ID = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

doc_attr = DocumentAttributes()
doc_attr.key = "value"

error = compass_client.add_attributes(
    index_name=INDEX_NAME,
    document_id=DOCUMENT_ID,
    attributes=doc_attr,
)

if error:
    raise Exception(f"Failed to add attributes: {error}")
