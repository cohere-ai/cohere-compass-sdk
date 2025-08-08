from cohere_compass.client_exceptions import CompassClientError
from cohere_compass.clients import CompassClient

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    compass_client.create_index(index_name=INDEX_NAME)
except CompassClientError as e:
    raise Exception("Failed to create index") from e
