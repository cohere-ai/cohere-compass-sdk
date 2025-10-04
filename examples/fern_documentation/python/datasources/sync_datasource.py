from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"

DATASOURCE_ID = "<DATASOURCE_ID>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)
r = compass_client.sync_datasource(
    datasource_id=DATASOURCE_ID,
)

print(r)
