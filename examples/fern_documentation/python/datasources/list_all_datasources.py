from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    r = compass_client.list_datasources()
except Exception as e:
    raise Exception(f"Failed to list datasources: {e}")

datasources = r.value
for datasource in datasources:
    print(f"Datasource id: {datasource.id}")
    print(f"Datasource name: {datasource.name}")
    print(f"Datasource description: {datasource.description}")
    print(f"Datasource enabled: {datasource.enabled}")
