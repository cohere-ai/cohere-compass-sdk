# Python imports
from enum import Enum
from typing import Any, Dict, List, Optional, TypeAlias

# 3rd party imports
from pydantic import BaseModel

Content: TypeAlias = Dict[str, Any]


class AssetInfo(BaseModel):
    content_type: str
    presigned_url: str


class RetrievedChunk(BaseModel):
    chunk_id: str
    sort_id: int
    parent_doc_id: str
    content: Content
    origin: Optional[Dict[str, Any]] = None
    assets_info: Optional[list[AssetInfo]] = None
    score: float


class RetrievedDocument(BaseModel):
    doc_id: str
    path: str
    parent_doc_id: str
    content: Content
    index_fields: Optional[List[str]] = None
    authorized_groups: Optional[List[str]] = None
    chunks: List[RetrievedChunk]
    score: float


class RetrieveChunkExtended(RetrievedChunk):
    doc_id: str
    path: str
    index_fields: Optional[List[str]] = None


class SearchDocumentsResponse(BaseModel):
    hits: List[RetrievedDocument]


class SearchChunksResponse(BaseModel):
    hits: List[RetrieveChunkExtended]


class SearchFilter(BaseModel):
    class FilterType(str, Enum):
        EQ = "$eq"
        LT_EQ = "$lte"
        GT_EQ = "$gte"
        WORD_MATCH = "$wordMatch"

    field: str
    type: FilterType
    value: Any


class SearchInput(BaseModel):
    """
    Search query input
    """

    query: str
    top_k: int
    filters: Optional[List[SearchFilter]] = None
