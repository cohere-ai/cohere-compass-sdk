"""Models for documents functionality in the Cohere Compass SDK."""

# Python imports
import uuid
from dataclasses import field
from enum import Enum
from typing import Annotated, Any, TypeAlias

# 3rd party imports
from pydantic import (
    UUID4,
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    StringConstraints,
    model_validator,
)

# Local imports
from cohere_compass.constants import URL_SAFE_STRING_PATTERN
from cohere_compass.models import ValidatedModel
from cohere_compass.models.config import ParserConfig

DocumentId: TypeAlias = Annotated[str, Field(pattern=URL_SAFE_STRING_PATTERN)]


class CompassDocumentMetadata(ValidatedModel):
    """Compass document metadata."""

    document_id: DocumentId = ""
    filename: str = ""
    meta: list[Any] = field(default_factory=list[Any])
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
    asset_data: str | None = None
    asset_id: str | None = None


class CompassDocumentChunk(ValidatedModel):
    """A chunk of a Compass document."""

    chunk_id: str
    sort_id: str
    document_id: str
    parent_document_id: str
    content: dict[str, Any]
    origin: dict[str, Any] | None = None
    assets: list[CompassDocumentChunkAsset] | None = None
    path: str | None = ""

    def parent_doc_is_split(self):
        """
        Check if the parent document is split.

        :return: True if the document ID is different from the parent document ID,
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
    content: dict[str, str] = field(default_factory=dict[str, str])
    content_type: str | None = None
    elements: list[Any] = field(default_factory=list[Any])
    chunks: list[CompassDocumentChunk] = field(
        default_factory=list[CompassDocumentChunk]
    )
    index_fields: list[str] = field(default_factory=list[str])
    errors: list[dict[CompassSdkStage, str]] = field(
        default_factory=list[dict[CompassSdkStage, str]]
    )
    ignore_metadata_errors: bool = True
    markdown: str | None = None

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

    @model_validator(mode="after")
    def validate_index_fields_exists(self):
        """Validate that index_fields exist in chunks.content."""
        chunk_without_index_fields = next(
            (
                chunk
                for chunk in self.chunks
                if not set(self.index_fields).issubset(chunk.content.keys())
            ),
            None,
        )
        if chunk_without_index_fields:
            missing_fields = set(self.index_fields) - set(
                chunk_without_index_fields.content.keys()
            )
            raise ValueError(
                f"All index_fields must exist as keys in chunk content. "
                f"Missing in chunk {chunk_without_index_fields.chunk_id}: "
                f"{missing_fields}"
            )
        return self

    @staticmethod
    def adapt_doc_id_compass_doc(doc: dict[Any, Any]) -> "CompassDocument":
        """
        Adapt a document dictionary to a CompassDocument instance.

        This dict is returned from Parser client.
        """
        metadata = doc["metadata"]
        if "document_id" not in metadata:
            metadata["document_id"] = metadata.pop("doc_id")
            metadata["parent_document_id"] = metadata.pop("parent_doc_id")

        chunks = doc["chunks"]
        for chunk in chunks:
            if "parent_document_id" not in chunk:
                chunk["parent_document_id"] = chunk.pop("parent_doc_id")
            if "document_id" not in chunk:
                chunk["document_id"] = chunk.pop("doc_id")
            if "path" not in chunk:
                chunk["path"] = doc["metadata"]["filename"]

        res = CompassDocument(
            filebytes=doc["filebytes"],
            metadata=metadata,
            content=doc["content"],
            content_type=doc["content_type"],
            elements=doc["elements"],
            chunks=chunks,
            index_fields=doc["index_fields"],
            errors=doc["errors"],
            ignore_metadata_errors=doc["ignore_metadata_errors"],
            markdown=doc["markdown"],
        )

        return res


class DocumentChunkAsset(BaseModel):
    """Model class for an asset associated with a document chunk."""

    asset_type: AssetType
    content_type: str
    asset_data: str | None = None
    asset_id: str | None = None


class Chunk(BaseModel):
    """Model class for a chunk of a document."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
    path: str = ""
    content: dict[str, Any]
    origin: dict[str, Any] | None = None
    assets: list[DocumentChunkAsset] | None = None
    asset_ids: list[str] | None = None


class Document(BaseModel):
    """Model class for a document."""

    document_id: DocumentId
    path: str
    parent_document_id: DocumentId
    content: dict[str, Any]
    chunks: list[Chunk]
    index_fields: list[str] | None = None
    authorized_groups: list[str] | None = None


class DocumentAttributes(BaseModel):
    """Model class for document attributes."""

    model_config = ConfigDict(extra="allow")

    # Had to add this to please the linter, because BaseModel only defines __setattr__
    # if TYPE_CHECKING is not set, i.e. at runtime, resulting in the type checking pass
    # done by the linter failing to find the __setattr__ method. See:
    # https://github.com/pydantic/pydantic/blob/main/pydantic/main.py#L878-L920
    def __setattr__(self, name: str, value: Any):  # noqa: D105
        return super().__setattr__(name, value)


class ParseableDocumentConfig(BaseModel):
    """Configuration for a parseable document."""

    parser_config: ParserConfig = ParserConfig()
    only_parse_doc: bool = False


class ParseableDocument(BaseModel):
    """A document to be sent to Compass for parsing."""

    id: str
    filename: Annotated[
        str, StringConstraints(min_length=1)
    ]  # Ensures the filename is a non-empty string
    content_type: str
    content_length_bytes: PositiveInt  # File size must be a non-negative integer
    content_encoded_bytes: str  # Base64-encoded file contents
    attributes: DocumentAttributes
    config: ParseableDocumentConfig = ParseableDocumentConfig()


class UploadDocumentsInput(BaseModel):
    """A model for the input of a call to upload_documents API."""

    documents: list[ParseableDocument]


class UploadDocumentsResult(BaseModel):
    """A model for the result of a call to upload_documents API."""

    upload_id: UUID4
    document_ids: list[str]


class PutDocumentsInput(BaseModel):
    """A model for the input of a call to put_documents API."""

    documents: list[Document]
    authorized_groups: list[str] | None = None
    merge_groups_on_conflict: bool = False


class PutDocumentResult(BaseModel):
    """
    A model for the response of put_document.

    This model is also used by the put_documents and edit_group_authorization APIs.
    """

    document_id: str
    error: str | None


class PutDocumentsResponse(BaseModel):
    """A model for the response of put_documents and edit_group_authorization APIs."""

    results: list[PutDocumentResult]


class UploadDocumentsStatus(BaseModel):
    """A model for the response of status for documents when uploaded via async API."""

    upload_id: uuid.UUID
    document_id: str
    destinations: list[str]
    file_name: str
    state: str | None
    last_error: str | None
    parsed_presigned_url: str | None


class ParsedDocumentResponse(BaseModel):
    """A model response for downloading saved document during the async API call."""

    upload_id: uuid.UUID
    document_id: str
    documents: list[CompassDocument] | None
    state: str

    @staticmethod
    def convert(data: dict[str, Any]) -> "ParsedDocumentResponse":
        """
        Convert a dictionary to a ParsedDocumentResponse instance.

        :param data: Dictionary containing the document data.
        :return: ParsedDocumentResponse instance.
        """
        return ParsedDocumentResponse(
            upload_id=uuid.UUID(data.get("upload_id", "")),
            document_id=data.get("document_id", ""),
            documents=[
                CompassDocument.adapt_doc_id_compass_doc(doc)
                for doc in data.get("documents", [])
            ],
            state=data.get("state", ""),
        )


class AssetPresignedUrlRequest(BaseModel):
    """A single asset presigned URL request item."""

    document_id: str
    asset_id: uuid.UUID


class GetAssetPresignedUrlsRequest(BaseModel):
    """A model for the input of a call to get_asset_presigned_urls API."""

    assets: list[AssetPresignedUrlRequest]


class AssetPresignedUrlDetails(BaseModel):
    """A single asset presigned URL response item."""

    document_id: str
    asset_id: uuid.UUID
    presigned_url: str


class GetAssetPresignedUrlsResponse(BaseModel):
    """A model for the response of get_asset_presigned_urls API."""

    asset_urls: list[AssetPresignedUrlDetails]


class ContentTypeEnum(str, Enum):
    """Enum for content types used in upload API."""

    # Text types
    TextPlain = "text/plain"
    TextHtml = "text/html"
    TextCsv = "text/csv"
    TextTsv = "text/tsv"
    TextMarkdown = "text/x-markdown"
    TextOrg = "text/org"
    TextRtf = "text/rtf"
    TextRst = "text/x-rst"

    # Application types
    ApplicationJson = "application/json"
    ApplicationJsonl = "application/jsonl"
    ApplicationJsonLines = "application/json-lines"
    ApplicationPdf = "application/pdf"
    ApplicationXml = "application/xml"
    ApplicationMsword = "application/msword"
    ApplicationVndOpenXMLDocument = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    ApplicationVndMsExcel = "application/vnd.ms-excel"
    ApplicationVndOpenXMLSpreadsheet = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    ApplicationVndMsPowerpoint = "application/vnd.ms-powerpoint"
    ApplicationVndOpenXMLPresentation = (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    ApplicationEpubZip = "application/epub+zip"
    ApplicationVndOasisOpenDocumentText = "application/vnd.oasis.opendocument.text"
    ApplicationMsOutlook = "application/vnd.ms-outlook"
    ApplicationOctetStream = "application/octet-stream"
    Parquet = "application/vnd.apache.parquet"
    # Image types
    ImageJpeg = "image/jpeg"
    ImagePng = "image/png"
    ImageHeic = "image/heic"
    ImageTiff = "image/tiff"
    ImageBmp = "image/bmp"
    ImageGif = "image/gif"
    ImageSvgXml = "image/svg+xml"
    ImageWebp = "image/webp"

    # Audio types
    AudioMpeg = "audio/mpeg"
    AudioWav = "audio/x-wav"

    # Video types
    VideoMp4 = "video/mp4"
    VideoXMsVideo = "video/x-msvideo"

    # Message types
    MessageRfc822 = "message/rfc822"  # eml files
