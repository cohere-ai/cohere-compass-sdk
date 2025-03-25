from cohere_compass.clients import CompassClient
from cohere_compass.models import AzureBlobStorageConfig, CreateDataSource, DataSource

COMPASS_API_URL = ...
BEARER_TOKEN = ...
INDEX_NAME = ...

DATASOURCE_ENABLED: bool = ...
DATASOURCE_NAME = ...
DATASOURCE_DESCRIPTION = ...

AZURE_BLOB_STORAGE_CONNECTION_STRING = ...
AZURE_BLOB_STORAGE_CONTAINER_NAME = ...
AZURE_BLOB_STORAGE_NAME_STARTS_WITH = ...

compass_client = CompassClient(
    index_url=COMPASS_API_URL,
    bearer_token=BEARER_TOKEN,
)

compass_client.create_index(index_name=INDEX_NAME)

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

if isinstance(datasource, str):
    raise Exception(f"Failed to create datasources: {datasource}")

if isinstance(datasource, DataSource):
    print(datasource)
