from typing import Any, Dict, List, Optional, TypeAlias

from pydantic import BaseModel

Content: TypeAlias = Dict[str, Any]


class AssetInfo(BaseModel):
    content_type: str
    presigned_url: str


class Chunk(BaseModel):
    chunk_id: str
    sort_id: int
    parent_doc_id: str
    content: Content
    origin: Optional[Dict[str, Any]] = None
    assets_info: Optional[list[AssetInfo]] = None
    score: float


class Document(BaseModel):
    doc_id: str
    path: str
    parent_doc_id: str
    content: Content
    index_fields: Optional[List[str]] = None
    authorized_groups: Optional[List[str]] = None
    chunks: List[Chunk]
    score: float


class ChunkExtended(Chunk):
    doc_id: str
    path: str
    index_fields: Optional[List[str]] = None


class SearchDocumentsResponse(BaseModel):
    hits: List[Document]


class SearchChunksResponse(BaseModel):
    hits: List[ChunkExtended]
