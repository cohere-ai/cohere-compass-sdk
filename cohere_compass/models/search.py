# Python imports
import enum
from enum import Enum
from typing import Any, Optional

# 3rd party imports
from pydantic import BaseModel

from cohere_compass.models.documents import AssetType


class AssetInfo(BaseModel):
    """Information about an asset."""

    asset_id: Optional[str] = None
    asset_type: AssetType
    content_type: str
    presigned_url: str


class RetrievedChunk(BaseModel):
    """Chunk of a document retrieved from search."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
    content: dict[str, Any]
    origin: Optional[dict[str, Any]] = None
    assets_info: Optional[list[AssetInfo]] = None
    score: float


class RetrievedDocument(BaseModel):
    """Document retrieved from search."""

    document_id: str
    path: str
    parent_document_id: str
    content: dict[str, Any]
    index_fields: Optional[list[str]] = None
    authorized_groups: Optional[list[str]] = None
    chunks: list[RetrievedChunk]
    score: float


class RetrievedChunkExtended(RetrievedChunk):
    """Additional information about a chunk retrieved from search."""

    document_id: str
    path: str
    index_fields: Optional[list[str]] = None


class SearchDocumentsResponse(BaseModel):
    """Response object for search_documents API."""

    hits: list[RetrievedDocument]


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
    filters: Optional[list[SearchFilter]] = None


class SearchConfig(BaseModel):
    """Search configuration which is used in expert search APIs."""

    class RerankStrategy(str, enum.Enum):
        """Specifies the reranking strategy to use."""

        NONE = "none"
        CHUNKS = "chunks"

    class ScoringStrategy(str, enum.Enum):
        """Specifies the scoring strategy to use."""

        RERANK_ONLY = "rerank_only"
        MIX = "mix"

    use_lexical: bool = True
    use_dense: bool = True
    use_sparse: bool = True
    first_stage_top_k: int = 1000
    crop_first_stage_to_top_k: bool = True
    lexical_boost: float = 1.0
    rerank_boost: float = 3.0
    sparse_boost: float = 1.0
    dense_boost: float = 1.0
    rerank_limit: int = -1
    rerank_input_max_chars_per_chunk: Optional[int] = None
    semantic_first_stage_top_k_factor: int = 5
    rerank_strategy: RerankStrategy = RerankStrategy.CHUNKS
    scoring_strategy: ScoringStrategy = ScoringStrategy.MIX


class ExpertSearchInput(BaseModel):
    """Input to expert search APIs."""

    search_input: SearchInput
    search_config: SearchConfig


class DirectSearchInput(BaseModel):
    """Input to direct search APIs."""

    query: dict[str, Any]
    size: int
    scroll: Optional[str] = None


class DirectSearchScrollInput(BaseModel):
    """Input to direct search scroll API."""

    scroll_id: str
    scroll: str


class DirectSearchResponse(BaseModel):
    """Response object for direct search APIs."""

    hits: list[RetrievedChunkExtended]
    scroll_id: Optional[str] = None
