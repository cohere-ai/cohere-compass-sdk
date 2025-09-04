"""Models for search functionality in the Cohere Compass SDK."""

# Python imports
from enum import Enum
from typing import Any, Literal

# 3rd party imports
from pydantic import BaseModel

from cohere_compass.models.documents import AssetType


class AssetInfo(BaseModel):
    """Information about an asset."""

    asset_id: str | None = None
    asset_type: AssetType
    content_type: str
    presigned_url: str


class RetrievedChunk(BaseModel):
    """Chunk of a document retrieved from get_document API."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
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
    path: str
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


class DirectSearchScrollInput(BaseModel):
    """Input to direct search scroll API."""

    scroll_id: str
    scroll: str


class DirectSearchResponse(BaseModel):
    """Response object for direct search APIs."""

    hits: list[RetrievedChunkExtended]
    scroll_id: str | None = None
