"""Models for indexes functionality in the Cohere Compass SDK."""

from pydantic import BaseModel, Field


class IndexInfo(BaseModel):
    """Information about an index."""

    name: str = Field(description="The name of the index - this has to be unique")
    count: int = Field(description="The total number of chunks in the index")
    parent_doc_count: int | None = Field(
        description="The total number of files provided for the index - files "
        "can be broken into 1..* chunks"
    )


class IndexDetails(IndexInfo):
    """Model representing the details of an index."""

    dense_model: str = Field(
        description="The name of the dense model used for embedding documents "
        "in the index.",
    )
    sparse_model: str = Field(
        description="The name of the sparse model used for embedding documents "
        "in the index.",
    )
    analyzer: str = Field(
        description="The name of the analyzer used for the index.",
    )


class ListIndexesResponse(BaseModel):
    """Response object for list_indexes API."""

    indexes: list[IndexInfo]
