from pydantic import BaseModel, Field


class IndexInfo(BaseModel):
    """Information about an index."""

    name: str = Field(description="The name of the index - this has to be unique")
    count: int = Field(description="The total number of chunks in the index")
    parent_doc_count: int | None = Field(
        description="The total number of files provided for the index - files "
        "can be broken into 1..* chunks"
    )


class ListIndexesResponse(BaseModel):
    """Response object for list_indexes API."""

    indexes: list[IndexInfo]
