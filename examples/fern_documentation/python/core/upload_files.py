import uuid

from cohere.compass.clients import CompassClient
from cohere.compass.models import DocumentAttributes

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...
DOCUMENT_ID = ...
FILE_PATH = ...
FILE_NAME = ...
CONTENT_TYPE = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

doc_attr = DocumentAttributes()
doc_attr.key = "value"

file_path = FILE_PATH
if not file_path:
    raise Exception("FILE_PATH is required")

with open(file_path, "rb") as f:
    file_bytes = f.read()

result = compass_client.upload_document(
    index_name=INDEX_NAME,
    filename=FILE_NAME,
    filebytes=file_bytes,
    content_type=CONTENT_TYPE,
    document_id=uuid.uuid4(),
)

print(result)
