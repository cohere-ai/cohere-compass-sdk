# Python imports
import datetime
import typing

# 3rd party imports
import pydantic

# NOTE: The models below are directly copied from the API.

T = typing.TypeVar("T")

Content: typing.TypeAlias = typing.Dict[str, typing.Any]


class PaginatedList(pydantic.BaseModel, typing.Generic[T]):
    value: typing.List[T]
    nextPageToken: typing.Optional[str] = None


class OneDriveConfig(pydantic.BaseModel):
    type: typing.Literal["msft_onedrive"]


class AzureBlobStorageConfig(pydantic.BaseModel):
    type: typing.Literal["msft_azure_blob_storage"]
    connection_string: str
    container_name: str
    name_starts_with: typing.Optional[str] = None


DatasourceConfig = typing.Annotated[
    typing.Union[AzureBlobStorageConfig, OneDriveConfig],
    pydantic.Field(discriminator="type"),
]


class DataSource(pydantic.BaseModel):
    id: typing.Optional[pydantic.UUID4] = None
    name: str
    description: typing.Optional[str] = None
    config: DatasourceConfig
    destinations: typing.List[str]
    enabled: bool = True
    created_at: typing.Optional[datetime.datetime] = None
    updated_at: typing.Optional[datetime.datetime] = None


class CreateDataSource(pydantic.BaseModel):
    datasource: DataSource
    state_key: typing.Optional[str] = None
