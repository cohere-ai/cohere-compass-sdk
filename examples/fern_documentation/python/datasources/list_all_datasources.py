from cohere_compass.clients import CompassClient
from cohere_compass.models import PaginatedList

COMPASS_API_URL = ...
BEARER_TOKEN = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)
r = compass_client.list_datasources()

if isinstance(r, str):
    raise Exception(f"Failed to list datasources: {r}")

if isinstance(r, PaginatedList):
    datasources = r.value
    for datasource in datasources:
        print(f"Datasource id: {datasource.id}")
        print(f"Datasource name: {datasource.name}")
        print(f"Datasource description: {datasource.description}")
        print(f"Datasource enabled: {datasource.enabled}")
