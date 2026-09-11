"""Models for documents functionality in the Cohere Compass SDK."""

# Python imports
import uuid
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, TypeAlias, cast

# 3rd party imports
from pydantic import (
    UUID4,
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    GetJsonSchemaHandler,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

# Local imports
from cohere_compass.constants import URL_SAFE_STRING_PATTERN
from cohere_compass.models.config import EnrichmentConfig, ParserConfig

DocumentId: TypeAlias = Annotated[str, Field(pattern=URL_SAFE_STRING_PATTERN)]


class APIModel(BaseModel):
    """Base for Compass API models: ignore unknown response fields."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class CompassDocumentMetadata(APIModel):
    """Compass document metadata."""

    document_id: DocumentId = Field(
        default="",
        validation_alias=AliasChoices("document_id", "doc_id"),
    )
    filename: str = ""
    meta: dict[str, Any] = Field(default_factory=dict)
    parent_document_id: str = Field(
        default="",
        validation_alias=AliasChoices("parent_document_id", "parent_doc_id"),
    )

    @field_validator("meta", mode="before")
    @classmethod
    def _coerce_meta(cls, value: Any) -> Any:
        # Older parser payloads serialized meta as a list of singleton dicts.
        if isinstance(value, list):
            merged: dict[str, Any] = {}
            items = cast(list[Any], value)
            for item in items:
                if isinstance(item, dict):
                    merged.update(cast(dict[str, Any], item))
            return merged
        return value


class AssetType(str, Enum):
    """Enum specifying the different types of assets."""

    def __str__(self) -> str:  # noqa: D105
        return self.value

    PAGE_IMAGE = "page_image"
    PAGE_MARKDOWN = "page_markdown"
    DOCUMENT_TEXT = "document_text"
    VIDEO = "video"
    AUDIO = "audio"
    RAW = "raw"

    @classmethod
    def __get_pydantic_json_schema__(cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        """Make AssetType an extensible enum for better OpenAPI schema generation."""
        json_schema = handler(core_schema)
        values = json_schema.pop("enum", None)
        if values is not None:
            json_schema["x-extensible-enum"] = values
        return json_schema


class VisualElement(APIModel):
    """Visual element of an asset."""

    id: int
    x0: int
    y0: int
    x1: int
    y1: int
    asset_id: str | None = None


class DocumentChunkAsset(APIModel):
    """An asset associated with a document chunk (write / parser path)."""

    asset_type: AssetType
    content_type: str
    asset_data: str | None = None
    asset_id: str | None = None
    visual_elements: list[VisualElement] | None = None


class CompassDocumentChunk(APIModel):
    """A chunk of a Compass document from the parser."""

    sort_id: int
    content: dict[str, Any]
    chunk_id: str | None = None
    document_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("document_id", "doc_id"),
    )
    parent_document_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("parent_document_id", "parent_doc_id"),
    )
    origin: dict[str, Any] | None = None
    assets: list[DocumentChunkAsset] | None = None
    path: str | None = None

    @field_validator("sort_id", mode="before")
    @classmethod
    def _coerce_sort_id(cls, value: Any) -> Any:
        if isinstance(value, str) and value.lstrip("-").isdigit():
            return int(value)
        return value

    def to_index_chunk(self, *, document_id: str, parent_document_id: str, path: str) -> "Chunk":
        """Build the put-documents write model for this chunk."""
        resolved_document_id = self.document_id or document_id
        return Chunk(
            chunk_id=self.chunk_id or f"{resolved_document_id}_{self.sort_id}",
            sort_id=self.sort_id,
            parent_document_id=self.parent_document_id or parent_document_id,
            path=self.path or path,
            content=self.content,
            origin=self.origin,
            assets=self.assets,
        )


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


class CompassDocument(APIModel):
    """
    A parsed Compass document.

    This is the parser / local working model. Search and get-document responses use
    :class:`RetrievedDocument` instead.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True, arbitrary_types_allowed=True)

    filebytes: bytes = b""
    metadata: CompassDocumentMetadata = Field(default_factory=CompassDocumentMetadata)
    content: dict[str, Any] = Field(default_factory=dict)
    content_type: str | None = None
    chunks: list[CompassDocumentChunk] = Field(default_factory=list[CompassDocumentChunk])
    assets: list[DocumentChunkAsset] = Field(default_factory=list[DocumentChunkAsset])
    index_fields: list[str] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list[dict[str, Any]])

    @field_validator("filebytes", mode="before")
    @classmethod
    def _coerce_filebytes(cls, value: Any) -> Any:
        if value is None or value == "":
            return b""
        return value

    def has_data(self) -> bool:
        """Check if the document has any data."""
        return len(self.filebytes) > 0

    def has_filename(self) -> bool:
        """Check if the document has a filename."""
        return len(self.metadata.filename) > 0

    def has_metadata(self) -> bool:
        """Check if the document has metadata."""
        return len(self.metadata.meta) > 0

    def has_parsing_errors(self) -> bool:
        """Check if the document has parsing errors."""
        return any(stage == CompassSdkStage.Parsing for error in self.errors for stage in error)

    def has_metadata_errors(self) -> bool:
        """Check if the document has metadata errors."""
        return any(stage == CompassSdkStage.Metadata for error in self.errors for stage in error)

    def has_indexing_errors(self) -> bool:
        """Check if the document has indexing errors."""
        return any(stage == CompassSdkStage.Indexing for error in self.errors for stage in error)

    @property
    def status(self) -> CompassDocumentStatus:
        """Get the document status."""
        if self.has_parsing_errors():
            return CompassDocumentStatus.ParsingErrors
        if self.has_indexing_errors():
            return CompassDocumentStatus.IndexingErrors
        return CompassDocumentStatus.Success

    @model_validator(mode="after")
    def validate_index_fields_exists(self):
        """Validate that index_fields exist in chunks.content."""
        chunk_without_index_fields = next(
            (chunk for chunk in self.chunks if not set(self.index_fields).issubset(chunk.content.keys())),
            None,
        )
        if chunk_without_index_fields:
            missing_fields = set(self.index_fields) - set(chunk_without_index_fields.content.keys())
            raise ValueError(
                f"All index_fields must exist as keys in chunk content. "
                f"Missing in chunk {chunk_without_index_fields.chunk_id}: "
                f"{missing_fields}"
            )
        return self

    def to_index_document(self) -> "Document":
        """Build the put-documents write model for this parsed document."""
        document_id = self.metadata.document_id
        parent_document_id = self.metadata.parent_document_id or document_id
        path = self.metadata.filename
        return Document(
            document_id=document_id,
            parent_document_id=parent_document_id,
            path=path,
            content=self.content,
            chunks=[
                chunk.to_index_chunk(
                    document_id=document_id,
                    parent_document_id=parent_document_id,
                    path=path,
                )
                for chunk in self.chunks
            ],
            index_fields=self.index_fields,
        )


class Chunk(APIModel):
    """Write model for a chunk sent to put_documents."""

    chunk_id: str
    sort_id: int
    parent_document_id: str
    path: str
    content: dict[str, Any]
    origin: dict[str, Any] | None = None
    assets: list[DocumentChunkAsset] | None = None


class Document(APIModel):
    """Write model for a document sent to put_documents."""

    document_id: DocumentId
    path: str
    parent_document_id: DocumentId
    content: dict[str, Any]
    chunks: list[Chunk]
    index_fields: list[str] | None = None
    authorized_groups: list[str] | None = None


class DocumentAttributes(APIModel):
    """Model class for document attributes."""

    model_config = ConfigDict(extra="allow")

    def __setattr__(self, name: str, value: Any):  # noqa: D105
        return super().__setattr__(name, value)


class ParseableDocumentConfig(APIModel):
    """Configuration for a parseable document."""

    parser_config: ParserConfig = Field(default_factory=ParserConfig)
    enrichment_config: EnrichmentConfig | None = None
    only_parse_doc: bool = False


class ParseableDocument(APIModel):
    """A document to be sent to Compass for parsing."""

    id: str
    filename: Annotated[str, StringConstraints(min_length=1)]
    content_type: str | None = None
    content_encoded_bytes: str | None = None
    file_data_uuid: UUID4 | None = None
    attributes: DocumentAttributes
    config: ParseableDocumentConfig = Field(default_factory=ParseableDocumentConfig)

    @model_validator(mode="after")
    def _validate_content_source(self) -> "ParseableDocument":
        has_bytes = self.content_encoded_bytes is not None
        has_uuid = self.file_data_uuid is not None
        if has_bytes == has_uuid:
            raise ValueError("Exactly one of `content_encoded_bytes` or `file_data_uuid` must be provided.")
        return self


class UploadDocumentsInput(APIModel):
    """A model for the input of a call to upload_documents API."""

    documents: list[ParseableDocument]
    authorized_groups: list[str] | None = None
    merge_groups_on_conflict: bool = False


class UploadDocumentsResult(APIModel):
    """A model for the result of a call to upload_documents API."""

    upload_id: UUID4
    document_ids: list[str]


class PutDocumentsInput(APIModel):
    """A model for the input of a call to put_documents API."""

    documents: list[Document]
    authorized_groups: list[str] | None = None
    merge_groups_on_conflict: bool = False


class PutDocumentResult(APIModel):
    """
    A model for the response of put_document.

    This model is also used by the put_documents and edit_group_authorization APIs.
    """

    document_id: str
    error: str | None
    task_ids: list[str] | None = None


class PutDocumentsResponse(APIModel):
    """A model for the response of put_documents and edit_group_authorization APIs."""

    results: list[PutDocumentResult]


class UploadTimeline(APIModel):
    """Lifecycle timestamps for an upload, from API receipt to terminal state."""

    created_at: datetime | None = None
    last_enqueued_at: datetime | None = None
    last_started_at: datetime | None = None
    completed_at: datetime | None = None


class UploadDocumentsStatus(APIModel):
    """Status of a document uploaded via the async upload API."""

    upload_id: uuid.UUID
    document_id: str
    index_name: str
    file_name: str
    state: str | None = None
    last_error: str | None = None
    parsed_presigned_url: str | None = None
    timeline: UploadTimeline = Field(default_factory=UploadTimeline)


class BulkUploadStatusRequest(APIModel):
    """A model for the request body of the bulk upload status API."""

    upload_ids: list[UUID4]


class BulkUploadDocumentsStatus(APIModel):
    """A model for a single entry in the bulk upload status response."""

    upload_id: UUID4
    statuses: list[UploadDocumentsStatus]


class ParsedDocumentResponse(APIModel):
    """A model response for downloading a parsed document from an async upload."""

    upload_id: uuid.UUID
    document_id: str
    documents: list[CompassDocument] | None = None
    state: str | None = None


class AssetPresignedUrlRequest(APIModel):
    """
    A single asset presigned URL request item.

    The document_id is the ID of the document.
    The asset_id is the ID of the asset in the asset_info.
    The x0, y0, x1, y1 are the coordinates of the asset if you want cropped images.
    The start_time and end_time are the start and end times of media assets like audio or video.
    """

    document_id: str
    asset_id: uuid.UUID
    x0: int | None = Field(default=None, ge=0, le=1000)
    y0: int | None = Field(default=None, ge=0, le=1000)
    x1: int | None = Field(default=None, ge=0, le=1000)
    y1: int | None = Field(default=None, ge=0, le=1000)
    start_time: float | None = Field(default=None, ge=0)
    end_time: float | None = Field(default=None, ge=0)


class GetAssetPresignedUrlsRequest(APIModel):
    """A model for the input of a call to get_asset_presigned_urls API."""

    assets: list[AssetPresignedUrlRequest]


class AssetPresignedUrlDetails(APIModel):
    """A single asset presigned URL response item."""

    document_id: str
    asset_id: uuid.UUID
    presigned_url: str


class GetAssetPresignedUrlsResponse(APIModel):
    """A model for the response of get_asset_presigned_urls API."""

    asset_urls: list[AssetPresignedUrlDetails]


class ContentTypeEnum(str, Enum):
    """Enum for content types used in upload API."""

    # Text types
    TextPlain = "text/plain"
    TextHtml = "text/html"
    TextCsv = "text/csv"
    TextTsv = "text/tab-separated-values"
    TextMarkdown = "text/markdown"
    TextOrg = "text/org"
    TextRst = "text/prs.fallenstein.rst"

    # Application types
    ApplicationJson = "application/json"
    ApplicationJsonl = "application/jsonl"
    ApplicationJsonLines = "application/json-lines"
    ApplicationPdf = "application/pdf"
    ApplicationXml = "application/xml"
    ApplicationMsword = "application/msword"
    ApplicationVndOpenXMLDocument = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ApplicationVndMsExcel = "application/vnd.ms-excel"
    ApplicationVndOpenXMLSpreadsheet = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ApplicationVndMsPowerpoint = "application/vnd.ms-powerpoint"
    ApplicationVndOpenXMLPresentation = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ApplicationEpubZip = "application/epub+zip"
    ApplicationVndOasisOpenDocumentText = "application/vnd.oasis.opendocument.text"
    ApplicationVndOasisOpenDocumentSpreadsheet = "application/vnd.oasis.opendocument.spreadsheet"
    ApplicationVndOasisOpenDocumentPresentation = "application/vnd.oasis.opendocument.presentation"

    ApplicationMsOutlook = "application/vnd.ms-outlook"
    ApplicationOctetStream = "application/octet-stream"
    ApplicationRtf = "application/rtf"
    # HWP types
    ApplicationXHwp = "application/x-hwp"
    ApplicationXHwpx = "application/x-hwpx"

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
    AudioWav = "audio/wav"
    AudioMp3 = "audio/mp3"

    # Video types
    VideoMp4 = "video/mp4"
    VideoXMsVideo = "video/x-msvideo"

    # Message types
    MessageRfc822 = "message/rfc822"  # eml files


class UploadFilePresignedUrlRequest(APIModel):
    """Request body for getting a presigned URL to upload a file directly to storage."""

    content_type: ContentTypeEnum
    filename: str


class UploadFilePresignedUrlResponse(APIModel):
    """Response from the presigned URL upload endpoint."""

    file_data_uuid: UUID4
    presigned_url: str
    expires_in_seconds: int
