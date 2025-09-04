from cohere_compass.clients import CompassClient

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"

DATASOURCE_ID = "<DATASOURCE_ID>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

try:
    r = compass_client.list_datasources_objects_states(
        datasource_id=DATASOURCE_ID,
    )
except Exception as e:
    raise Exception(f"Failed to list datasources: {e}")


documents = r.value
for document in documents:
    print(f"Document id: {document.document_id}")
    print(f"Document state: {document.state}")
    print(f"Document source id: {document.source_id}")
    print(f"Document destination: {document.destinations}")
