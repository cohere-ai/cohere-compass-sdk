"""Models for datasources functionality in the Cohere Compass SDK."""

# Python imports
import datetime
import typing

# 3rd party imports
import pydantic

# NOTE: The models below are directly copied from the API.

T = typing.TypeVar("T")


class PaginatedList(pydantic.BaseModel, typing.Generic[T]):
    """Model class for a paginated list of items."""

    value: list[T]
    skip: int | None
    limit: int | None


class OneDriveConfig(pydantic.BaseModel):
    """Model class for OneDrive configuration."""

    type: typing.Literal["msft_onedrive"]


class AzureBlobStorageConfig(pydantic.BaseModel):
    """Model class for Azure Blob Storage configuration."""

    type: typing.Literal["msft_azure_blob_storage"]
    connection_string: str
    container_name: str
    name_starts_with: str | None = None


DatasourceConfig = typing.Annotated[
    AzureBlobStorageConfig | OneDriveConfig,
    pydantic.Field(discriminator="type"),
]


class DataSource(pydantic.BaseModel):
    """Model class for a data source."""

    id: pydantic.UUID4 | None = None
    name: str
    description: str | None = None
    config: DatasourceConfig
    destinations: list[str]
    enabled: bool = True
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None


class CreateDataSource(pydantic.BaseModel):
    """Model class for the create_datasource API."""

    datasource: DataSource
    state_key: str | None = None


class DocumentStatus(pydantic.BaseModel):
    """Model class for the response of the list_datasources_objects_states API."""

    document_id: str
    source_id: str | None
    state: str
    destinations: list[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime | None = None
