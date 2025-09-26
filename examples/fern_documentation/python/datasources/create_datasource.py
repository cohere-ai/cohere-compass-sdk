from cohere_compass.clients import CompassClient
from cohere_compass.models import AzureBlobStorageConfig, CreateDataSource, DataSource

COMPASS_API_URL = "<COMPASS_API_URL>"
BEARER_TOKEN = "<BEARER_TOKEN>"
INDEX_NAME = "<INDEX_NAME>"

DATASOURCE_ENABLED: bool = True  # True or False
DATASOURCE_NAME = "<DATASOURCE_NAME>"
DATASOURCE_DESCRIPTION = "<DATASOURCE_DESCRIPTION>"

AZURE_BLOB_STORAGE_CONNECTION_STRING = "<AZURE_BLOB_STORAGE_CONNECTION_STRING>"
AZURE_BLOB_STORAGE_CONTAINER_NAME = "<AZURE_BLOB_STORAGE_CONTAINER_NAME>"
AZURE_BLOB_STORAGE_NAME_STARTS_WITH = "<AZURE_BLOB_STORAGE_NAME_STARTS_WITH>"

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

compass_client.create_index(index_name=INDEX_NAME)

try:
    datasource = compass_client.create_datasource(
        datasource=CreateDataSource(
            datasource=DataSource(
                name=DATASOURCE_NAME,
                description=DATASOURCE_DESCRIPTION,
                config=AzureBlobStorageConfig(
                    type="msft_azure_blob_storage",
                    connection_string=AZURE_BLOB_STORAGE_CONNECTION_STRING,
                    container_name=AZURE_BLOB_STORAGE_CONTAINER_NAME,
                    name_starts_with=AZURE_BLOB_STORAGE_NAME_STARTS_WITH,
                ),
                destinations=[INDEX_NAME],
                enabled=DATASOURCE_ENABLED,
            )
        )
    )
except Exception as e:
    raise Exception(f"Failed to create datasource: {e}")

print(datasource)
