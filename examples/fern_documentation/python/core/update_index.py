from cohere_compass.clients import CompassClient
from cohere_compass.exceptions import CompassClientError

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"
INDEX_DESCRIPTION = "<WHAT_THE_INDEX_CONTAINS>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    compass_client.update_index(index_name=INDEX_NAME, description=INDEX_DESCRIPTION)
except CompassClientError as e:
    raise Exception("Failed to update index") from e
