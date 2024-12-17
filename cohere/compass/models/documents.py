# Python imports
import uuid
from dataclasses import field
from enum import Enum
from typing import Annotated, Any, Optional

# 3rd party imports
from pydantic import BaseModel, Field, PositiveInt, StringConstraints

# Local imports
from cohere.compass.models import ValidatedModel


class CompassDocumentMetadata(ValidatedModel):
    """Compass document metadata."""

    document_id: str = ""
    filename: str = ""
    meta: list[Any] = field(default_factory=list)
    parent_document_id: str = ""


class AssetType(str, Enum):
    """Enum specifying the different types of assets."""

    def __str__(self) -> str:  # noqa: D105
        return self.value

    # A page that has been rendered as an image
    PAGE_IMAGE = "page_image"
    # A Markdown representation of a page's content
    PAGE_MARKDOWN = "page_markdown"
    # A dump of the text extracted from a document
    DOCUMENT_TEXT = "document_text"


class CompassDocumentChunkAsset(BaseModel):
    """An asset associated with a Compass document chunk."""

    asset_type: AssetType
    content_type: str
    asset_data: str


class CompassDocumentChunk(BaseModel):
    """A chunk of a Compass document."""

    chunk_id: str
    sort_id: str
    document_id: str
    parent_document_id: str
    content: dict[str, Any]
    origin: Optional[dict[str, Any]] = None
    assets: Optional[list[CompassDocumentChunkAsset]] = None
    path: Optional[str] = ""

    def parent_doc_is_split(self):
        """
        Check if the parent document is split.

        :returns: True if the document ID is different from the parent document ID,
        indicating that the parent document is split; False otherwise.
        """
        return self.document_id != self.parent_document_id


class CompassDocumentStatus(str, Enum):
    """Compass document status."""

    Success = "success"
    ParsingErrors = "parsing-errors"
    MetadataErrors = "metadata-errors"
    IndexingErrors = "indexing-errors"


class CompassSdkStage(str, Enum):
    """Compass SDK stages."""

    Parsing = "parsing"
    Metadata = "metadata"
    Chunking = "chunking"
    Indexing = "indexing"


class CompassDocument(ValidatedModel):
    """
    A model class for a Compass document.

    The model contains all the information required to process a document and insert it
    into the index. It includes:

    - metadata: the document metadata (e.g., filename, title, authors, date)
    - content: the document content in string format
    - elements: the document's Unstructured elements (e.g., tables, images, text).
    - chunks: the document's chunks (e.g., paragraphs, tables, images).
    - index_fields: the fields to be indexed. Used by the indexer
    """

    filebytes: bytes = b""
    metadata: CompassDocumentMetadata = CompassDocumentMetadata()
    content: dict[str, str] = field(default_factory=dict)
    content_type: Optional[str] = None
    elements: list[Any] = field(default_factory=list)
    chunks: list[CompassDocumentChunk] = field(default_factory=list)
    index_fields: list[str] = field(default_factory=list)
    errors: list[dict[CompassSdkStage, str]] = field(default_factory=list)
    ignore_metadata_errors: bool = True
    markdown: Optional[str] = None

    def has_data(self) -> bool:
        """Check if the document has any data."""
        return len(self.filebytes) > 0

    def has_markdown(self) -> bool:
        """Check if the document has a markdown representation."""
        return self.markdown is not None

    def has_filename(self) -> bool:
        """Check if the document has a filename."""
        return len(self.metadata.filename) > 0

    def has_metadata(self) -> bool:
        """Check if the document has metadata."""
        return len(self.metadata.meta) > 0

    def has_parsing_errors(self) -> bool:
        """Check if the document has parsing errors."""
        return any(
            stage == CompassSdkStage.Parsing
            for error in self.errors
            for stage, _ in error.items()
        )

    def has_metadata_errors(self) -> bool:
        """Check if the document has metadata errors."""
        return any(
            stage == CompassSdkStage.Metadata
            for error in self.errors
            for stage, _ in error.items()
        )

    def has_indexing_errors(self) -> bool:
        """Check if the document has indexing errors."""
        return any(
            stage == CompassSdkStage.Indexing
            for error in self.errors
            for stage, _ in error.items()
        )

    @property
    def status(self) -> CompassDocumentStatus:
        """Get the document status."""
        if self.has_parsing_errors():
            return CompassDocumentStatus.ParsingErrors

        if not self.ignore_metadata_errors and self.has_metadata_errors():
            return CompassDocumentStatus.MetadataErrors

        if self.has_indexing_errors():
            return CompassDocumentStatus.IndexingErrors

        return CompassDocumentStatus.Success


class DocumentChunkAsset(BaseModel):
    """Model class for an asset associated with a document chunk."""

    asset_type: AssetType
    content_type: str
    asset_data: str


class Chunk(BaseModel):
    """Model class for a chunk of a document."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
    path: str = ""
    content: dict[str, Any]
    origin: Optional[dict[str, Any]] = None
    assets: Optional[list[DocumentChunkAsset]] = None
    asset_ids: Optional[list[str]] = None


class Document(BaseModel):
    """Model class for a document."""

    document_id: str
    path: str
    parent_document_id: str
    content: dict[str, Any]
    chunks: list[Chunk]
    index_fields: Optional[list[str]] = None
    authorized_groups: Optional[list[str]] = None


class ParseableDocument(BaseModel):
    """A document to be sent to Compass for parsing."""

    id: uuid.UUID
    filename: Annotated[
        str, StringConstraints(min_length=1)
    ]  # Ensures the filename is a non-empty string
    content_type: str
    content_length_bytes: PositiveInt  # File size must be a non-negative integer
    content_encoded_bytes: str  # Base64-encoded file contents
    attributes: dict[str, Any] = Field(default_factory=dict)


class UploadDocumentsInput(BaseModel):
    """A model for the input of a call to upload_documents API."""

    documents: list[ParseableDocument]


class PutDocumentsInput(BaseModel):
    """A model for the input of a call to put_documents API."""

    documents: list[Document]
    authorized_groups: Optional[list[str]] = None
    merge_groups_on_conflict: bool = False
