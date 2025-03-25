from cohere_compass.clients import CompassClient

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

r = compass_client.refresh_index(index_name=INDEX_NAME)
if r.error:
    raise Exception(f"Failed to refresh indexes: {r.error}")
