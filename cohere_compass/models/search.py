"""Models for search functionality in the Cohere Compass SDK."""

# Python imports
from enum import Enum
from typing import Any, Literal

# 3rd party imports
from pydantic import Field

from cohere_compass.models.documents import APIModel, AssetType, VisualElement


class AssetInfo(APIModel):
    """Read-side asset metadata on retrieved chunks."""

    asset_type: AssetType
    content_type: str
    asset_id: str | None = None
    presigned_url: str | None = None
    visual_elements: list[VisualElement] | None = None


class RetrievedChunk(APIModel):
    """A document chunk returned by get-document, search, or direct-search."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
    path: str
    content: dict[str, Any]
    origin: dict[str, Any] | None = None
    assets_info: list[AssetInfo] | None = None
    document_id: str | None = None
    index_fields: list[str] | None = None
    score: float | None = None
    created_at: int | None = None
    updated_at: int | None = None
    accessed_at: int | None = None
    source: str | None = None


class RetrievedDocument(APIModel):
    """A document returned by get-document or search_documents."""

    document_id: str
    path: str
    parent_document_id: str
    content: dict[str, Any]
    chunks: list[RetrievedChunk]
    index_fields: list[str] | None = None
    authorized_groups: list[str] | None = None
    score: float | None = None
    source: str | None = None


class GetDocumentResponse(APIModel):
    """Response object for get_document API."""

    document: RetrievedDocument


class SearchDocumentsResponse(APIModel):
    """Response object for search_documents API."""

    hits: list[RetrievedDocument]


class SearchChunksResponse(APIModel):
    """Response object for search_chunks API."""

    hits: list[RetrievedChunk]


class SearchFilter(APIModel):
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


class SearchInput(APIModel):
    """Input to search APIs."""

    query: str
    top_k: int
    filters: list[SearchFilter] | None = None
    rerank_model: str | None = None
    enable_profiling: bool = False


class SortBy(APIModel):
    """Specifies sorting options for search results."""

    field: str
    order: Literal["asc", "desc"]


class DirectSearchInput(APIModel):
    """Input to direct search APIs."""

    query: dict[str, Any]
    size: int
    sort_by: list[SortBy] | None = None
    scroll: str | None = None


class DirectSearchScrollInput(APIModel):
    """Input to direct search scroll API."""

    scroll_id: str
    scroll: str = Field(default="1m")


class DirectSearchResponse(APIModel):
    """Response object for direct search APIs."""

    hits: list[RetrievedChunk]
    scroll_id: str | None = None
