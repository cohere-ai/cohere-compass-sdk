"""Models for search functionality in the Cohere Compass SDK."""

# Python imports
from enum import Enum
from typing import Any, Literal

# 3rd party imports
from pydantic import BaseModel, ConfigDict, Field

from cohere_compass.models.documents import AssetType, VisualElement


class AssetInfo(BaseModel):
    """Read-side asset metadata on retrieved chunks."""

    model_config = ConfigDict(extra="ignore")

    asset_type: AssetType
    content_type: str
    asset_id: str | None = None
    presigned_url: str | None = None
    visual_elements: list[VisualElement] | None = None


class RetrievedChunk(BaseModel):
    """A document chunk returned by get-document, search, or direct-search."""

    model_config = ConfigDict(extra="ignore")

    sort_id: int
    path: str
    content: dict[str, Any]
    document_id: str | None = None
    origin: dict[str, Any] | None = None
    assets_info: list[AssetInfo] | None = None
    score: float | None = None
    created_at: int | None = None
    updated_at: int | None = None
    accessed_at: int | None = None
    source: str | None = None


class RetrievedDocument(BaseModel):
    """A document returned by get-document or search_documents."""

    model_config = ConfigDict(extra="ignore")

    document_id: str
    path: str
    content: dict[str, Any]
    chunks: list[RetrievedChunk]
    index_fields: list[str] | None = None
    authorized_groups: list[str] | None = None
    score: float | None = None
    source: str | None = None


class GetDocumentResponse(BaseModel):
    """Response object for get_document API."""

    model_config = ConfigDict(extra="ignore")

    document: RetrievedDocument


class SearchDocumentsResponse(BaseModel):
    """Response object for search_documents API."""

    model_config = ConfigDict(extra="ignore")

    hits: list[RetrievedDocument]


class SearchChunksResponse(BaseModel):
    """Response object for search_chunks API."""

    model_config = ConfigDict(extra="ignore")

    hits: list[RetrievedChunk]


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
    scroll: str = Field(default="1m")


class DirectSearchResponse(BaseModel):
    """Response object for direct search APIs."""

    model_config = ConfigDict(extra="ignore")

    hits: list[RetrievedChunk]
    scroll_id: str | None = None
