"""Models for search functionality in the Cohere Compass SDK."""

# Python imports
from enum import Enum
from typing import Any, Literal, cast

# 3rd party imports
from pydantic import BaseModel, Field, model_validator

from cohere_compass.models.documents import AssetType


class VisualElement(BaseModel):
    """Visual element of an asset."""

    id: int
    x0: int
    y0: int
    x1: int
    y1: int
    asset_id: str | None = None


class AssetInfo(BaseModel):
    """Information about an asset."""

    asset_id: str | None = None
    asset_type: AssetType
    content_type: str
    presigned_url: str
    visual_elements: list[VisualElement] | None = None

    @model_validator(mode="before")
    @classmethod
    def _default_presigned_url(cls, data: Any) -> Any:
        """
        Ensure that the presigned_url is always present.

        This is done to keep it backward compatible.
        """
        if isinstance(data, dict):
            values = cast(dict[str, Any], data)
            if values.get("presigned_url") is None:
                values = {**values, "presigned_url": ""}
            return values
        return data


class RetrievedChunk(BaseModel):
    """Chunk of a document retrieved from get_document API."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
    path: str
    content: dict[str, Any]
    origin: dict[str, Any] | None = None
    assets_info: list[AssetInfo] | None = None


class RetrievedScoredChunk(RetrievedChunk):
    """Chunk of a document retrieved from search API."""

    score: float


class RetrievedDocument(BaseModel):
    """Document retrieved from get_document API."""

    document_id: str
    path: str
    parent_document_id: str
    content: dict[str, Any]
    index_fields: list[str] | None = None
    authorized_groups: list[str] | None = None
    chunks: list[RetrievedChunk]


class RetrievedScoredDocument(RetrievedDocument):
    """Document retrieved from search API."""

    chunks: list[RetrievedScoredChunk]  # pyright: ignore[reportIncompatibleVariableOverride]
    score: float


class RetrievedChunkExtended(RetrievedScoredChunk):
    """Additional information about a chunk retrieved from search."""

    document_id: str
    index_fields: list[str] | None = None


class GetDocumentResponse(BaseModel):
    """Response object for get_document API."""

    document: RetrievedDocument


class SearchDocumentsResponse(BaseModel):
    """Response object for search_documents API."""

    hits: list[RetrievedScoredDocument]


class SearchChunksResponse(BaseModel):
    """Response object for search_chunks API."""

    hits: list[RetrievedChunkExtended]


class SearchFilter(BaseModel):
    """Filter to apply on search results."""

    class FilterType(str, Enum):
        """Types of filters supported."""

        EQ = "$eq"
        NEQ = "$neq"
        LT_EQ = "$lte"
        GT_EQ = "$gte"
        WORD_MATCH = "$wordMatch"

    field: str
    type: FilterType
    value: Any


class SearchInput(BaseModel):
    """Input to search APIs."""

    query: str
    top_k: int
    filters: list[SearchFilter] | None = None
    rerank_model: str | None = None
    group_scopes: list[str] | None = None


class SortBy(BaseModel):
    """Specifies sorting options for search results."""

    field: str
    order: Literal["asc", "desc"]


class DirectSearchInput(BaseModel):
    """Input to direct search APIs."""

    query: dict[str, Any]
    size: int
    sort_by: list[SortBy] | None = None
    scroll: str | None = None
    group_scopes: list[str] | None = None


class DirectSearchScrollInput(BaseModel):
    """Input to direct search scroll API."""

    scroll_id: str
    scroll: str


class DirectSearchResponse(BaseModel):
    """Response object for direct search APIs."""

    hits: list[RetrievedChunkExtended]
    scroll_id: str | None = None


class CountInput(BaseModel):
    """Input to the count API."""

    query: dict[str, Any] | None = None
    group_by: str | None = None
    group_size: int = 100
    group_scopes: list[str] | None = None


class CountBucket(BaseModel):
    """Chunks and the documents they belong to."""

    chunk_count: int = Field(description="Number of chunks.")
    document_count: int = Field(description="Estimated number of documents those chunks belong to.")


class CountValue(CountBucket):
    """Counts for one value of the grouped field."""

    value: str | int | float = Field(
        description="A value of the grouped field. Numbers come back as numbers; dates and true/false values as "
        'text, such as "2026-11-23T00:00:00.000Z" or "true".'
    )


class CountGroup(BaseModel):
    """Counts per value of the grouped field."""

    field: str = Field(description="The grouped field.")
    cardinality: int = Field(description="Estimated number of distinct values of the field.")
    values: list[CountValue] = Field(description="The values with the most chunks, most first.")
    other_chunk_count: int = Field(
        description="Chunks whose value is not listed; 0 means every value is listed. For a field that holds a "
        "list, a chunk is counted once per value it holds."
    )
    chunk_count_error_upper_bound: int = Field(
        description="Most a listed chunk_count can be off by, when the field has more values than are listed."
    )
    missing: CountBucket = Field(
        description="Chunks with no indexed value for the field, including values too long for its keyword index."
    )


class CountResponse(BaseModel):
    """Response object for the count API."""

    chunk_count: int = Field(description="Number of matching chunks. Exact.")
    document_count: int = Field(description="Estimated number of documents the matching chunks belong to.")
    group_by: CountGroup | None = Field(default=None, description="Counts per value, when group_by is set.")
