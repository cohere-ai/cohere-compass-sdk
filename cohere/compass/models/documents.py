# Python imports
from dataclasses import field
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional
import uuid

# 3rd party imports
from pydantic import BaseModel, Field, PositiveInt, StringConstraints

# Local imports
from cohere.compass.models import ValidatedModel


class CompassDocumentMetadata(ValidatedModel):
    """
    Compass document metadata
    """

    document_id: str = ""
    filename: str = ""
    meta: list[Any] = field(default_factory=list)
    parent_document_id: str = ""


class CompassDocumentChunkAsset(BaseModel):
    content_type: str
    asset_data: str


class CompassDocumentChunk(BaseModel):
    chunk_id: str
    sort_id: str
    document_id: str
    parent_document_id: str
    content: Dict[str, Any]
    origin: Optional[Dict[str, Any]] = None
    assets: Optional[list[CompassDocumentChunkAsset]] = None
    path: Optional[str] = ""

    def parent_doc_is_split(self):
        return self.document_id != self.parent_document_id


class CompassDocumentStatus(str, Enum):
    """
    Compass document status
    """

    Success = "success"
    ParsingErrors = "parsing-errors"
    MetadataErrors = "metadata-errors"
    IndexingErrors = "indexing-errors"


class CompassSdkStage(str, Enum):
    """
    Compass SDK stages
    """

    Parsing = "parsing"
    Metadata = "metadata"
    Chunking = "chunking"
    Indexing = "indexing"


class CompassDocument(ValidatedModel):
    """
    A Compass document contains all the information required to process a document and
    insert it into the index. It includes:
    - metadata: the document metadata (e.g., filename, title, authors, date)
    - content: the document content in string format
    - elements: the document's Unstructured elements (e.g., tables, images, text). Used
      for chunking
    - chunks: the document's chunks (e.g., paragraphs, tables, images). Used for indexing
    - index_fields: the fields to be indexed. Used by the indexer
    """

    filebytes: bytes = b""
    metadata: CompassDocumentMetadata = CompassDocumentMetadata()
    content: Dict[str, str] = field(default_factory=dict)
    content_type: Optional[str] = None
    elements: List[Any] = field(default_factory=list)
    chunks: List[CompassDocumentChunk] = field(default_factory=list)
    index_fields: List[str] = field(default_factory=list)
    errors: List[Dict[CompassSdkStage, str]] = field(default_factory=list)
    ignore_metadata_errors: bool = True
    markdown: Optional[str] = None

    def has_data(self) -> bool:
        return len(self.filebytes) > 0

    def has_markdown(self) -> bool:
        return self.markdown is not None

    def has_filename(self) -> bool:
        return len(self.metadata.filename) > 0

    def has_metadata(self) -> bool:
        return len(self.metadata.meta) > 0

    def has_parsing_errors(self) -> bool:
        return any(
            stage == CompassSdkStage.Parsing
            for error in self.errors
            for stage, _ in error.items()
        )

    def has_metadata_errors(self) -> bool:
        return any(
            stage == CompassSdkStage.Metadata
            for error in self.errors
            for stage, _ in error.items()
        )

    def has_indexing_errors(self) -> bool:
        return any(
            stage == CompassSdkStage.Indexing
            for error in self.errors
            for stage, _ in error.items()
        )

    @property
    def status(self) -> CompassDocumentStatus:
        if self.has_parsing_errors():
            return CompassDocumentStatus.ParsingErrors

        if not self.ignore_metadata_errors and self.has_metadata_errors():
            return CompassDocumentStatus.MetadataErrors

        if self.has_indexing_errors():
            return CompassDocumentStatus.IndexingErrors

        return CompassDocumentStatus.Success


class DocumentChunkAsset(BaseModel):
    content_type: str
    asset_data: str


class Chunk(BaseModel):
    chunk_id: str
    sort_id: int
    parent_document_id: str
    path: str = ""
    content: Dict[str, Any]
    origin: Optional[Dict[str, Any]] = None
    assets: Optional[List[DocumentChunkAsset]] = None
    asset_ids: Optional[List[str]] = None


class Document(BaseModel):
    """
    A document that can be indexed in Compass (i.e., a list of indexable chunks)
    """

    document_id: str
    path: str
    parent_document_id: str
    content: Dict[str, Any]
    chunks: List[Chunk]
    index_fields: Optional[List[str]] = None
    authorized_groups: Optional[List[str]] = None


class ParseableDocument(BaseModel):
    """
    A document to be sent to Compass in bytes format for parsing on the Compass side
    """

    id: uuid.UUID
    filename: Annotated[
        str, StringConstraints(min_length=1)
    ]  # Ensures the filename is a non-empty string
    content_type: str
    content_length_bytes: PositiveInt  # File size must be a non-negative integer
    content_encoded_bytes: str  # Base64-encoded file contents
    context: Dict[str, Any] = Field(default_factory=dict)


class PushDocumentsInput(BaseModel):
    documents: List[ParseableDocument]


class PutDocumentsInput(BaseModel):
    """
    A Compass request to put a list of Document
    """

    documents: List[Document]
    authorized_groups: Optional[List[str]] = None
    merge_groups_on_conflict: bool = False
