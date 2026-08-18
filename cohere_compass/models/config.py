"""Models for config functionality in the Cohere Compass SDK."""

# Python imports
import math
from enum import Enum
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal

# 3rd party imports
from pydantic import BaseModel, ConfigDict, Field

# Local imports
from cohere_compass.constants import (
    DEFAULT_MIN_CHARS_PER_ELEMENT,
    DEFAULT_MIN_NUM_CHUNKS_IN_TITLE,
    DEFAULT_MIN_NUM_TOKENS_CHUNK,
    DEFAULT_NUM_TOKENS_CHUNK_OVERLAP,
    DEFAULT_NUM_TOKENS_PER_CHUNK,
    SKIP_INFER_TABLE_TYPES,
)


class DocumentFormat(str, Enum):
    """Enum for specifying the output format of the parsed document."""

    Markdown = "markdown"
    Text = "text"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Markdown


class TabularParsingStrategy(str, Enum):
    """
    Enum defining strategies for parsing tabular files (CSV, Excel, ODS).

    Granular: Convert each row of the table to a document chunk.
    Digest: Creates one chunk for the table, containing metadata about the table.
    SubTables: Splits the table into semantic sub-table chunks optimized for retrieval.
    """

    Granular = "granular"
    Digest = "digest"
    SubTables = "subtables"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Granular


class PDFParsingStrategy(str, Enum):
    """Enum for specifying the parsing strategy for PDF files."""

    QuickText = "QuickText"
    ImageToMarkdown = "ImageToMarkdown"
    Smart = "Smart"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.QuickText


class PDFParsingConfig(BaseModel):
    """Contains extra parsing configuration specific to PDF files."""

    model_config = ConfigDict(
        extra="ignore",
    )

    # enable_classification - controls whether the system uses the visual classification
    # model to detect pages containing images, tables, or other visual elements. When
    # True, pages with classification scores exceeding visual_parsing_threshold will be
    # processed using visual parsing.
    enable_classification: bool = True

    # visual_parsing_threshold - sets the minimum confidence threshold for the
    # classification model to trigger visual parsing. Higher values (closer to 1.0) make
    # the system more conservative, using visual parsing only when the model is very
    # confident about the presence of visual elements. Lower values increase the
    # likelihood of visual parsing.
    # Range: 0.0 to 1.0
    visual_parsing_threshold: float = 0.5

    # enable_symbol_detection - controls whether to detect special symbols (mathematical
    # notation, currency symbols, etc.) that often extract poorly with standard text
    # extraction.  When enabled, pages with high symbol density are processed using
    # visual parsing, which is critical for academic papers, financial documents, and
    # scientific literature. This provides a complementary heuristic to visual
    # classification that's faster to compute and catches cases where visual complexity
    # is in the text content itself rather than the page layout.
    enable_symbol_detection: bool = True

    # symbol_density_threshold - the minimum density of special symbols required to
    # trigger visual parsing. Represents the ratio of special symbols to total
    # characters. Higher values make the system less sensitive to special symbols.
    # Range: 0.0 to 1.0 (practically, values above 0.5 are rare)
    symbol_density_threshold: float = 0.2


class PresentationParsingStrategy(str, Enum):
    """Enum for specifying the parsing strategy for presentation files."""

    Unstructured = "Unstructured"
    ImageToMarkdown = "ImageToMarkdown"
    ConvertToPDF = "ConvertToPDF"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Unstructured


class DocxParsingStrategy(str, Enum):
    """Enum for specifying the parsing strategy for DOCX files."""

    # Uses https://github.com/microsoft/markitdown
    MarkItDown = "MarkItDown"
    # Converts the DOCX to PDF and uses the PDF parsing strategy
    ConvertToPDF = "ConvertToPDF"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.MarkItDown


class ParserConfig(BaseModel):
    """A model class for specifying parsing configuration."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    # CompassParser configuration
    parsed_images_output_dir: str | None = None
    allowed_image_types: list[str] | None = None
    min_chars_per_element: int = DEFAULT_MIN_CHARS_PER_ELEMENT
    skip_infer_table_types: list[str] = SKIP_INFER_TABLE_TYPES

    # CompassChunker configuration
    num_tokens_per_chunk: int = DEFAULT_NUM_TOKENS_PER_CHUNK
    num_tokens_overlap: int = DEFAULT_NUM_TOKENS_CHUNK_OVERLAP
    min_chunk_tokens: int = DEFAULT_MIN_NUM_TOKENS_CHUNK
    num_chunks_in_title: int = DEFAULT_MIN_NUM_CHUNKS_IN_TITLE
    max_tokens_metadata: int = math.floor(num_tokens_per_chunk * 0.1)

    # Formatting configuration
    output_format: DocumentFormat = DocumentFormat.Markdown

    # Visual elements extraction configuration

    pdf_parsing_config: PDFParsingConfig = PDFParsingConfig()
    pdf_parsing_strategy: PDFParsingStrategy = PDFParsingStrategy.QuickText
    tabular_parsing_strategy: TabularParsingStrategy = TabularParsingStrategy.Granular
    presentation_parsing_strategy: PresentationParsingStrategy | None = None
    docx_parsing_strategy: DocxParsingStrategy | None = None

    # ASR configuration
    min_asr_chunk_duration_seconds: int | None = None
    max_asr_chunk_duration_seconds: int | None = None


class WebhookEnricherConfig(BaseModel):
    """
    Config for webhook enrichers.

    See cohere_compass.models.enrichments for the request/response contract.
    """

    model_config = ConfigDict(frozen=True)

    type: Literal["webhook"] = "webhook"
    webhook_url: str
    timeout: float | None = None
    params: dict[str, Any] = Field(default_factory=dict)


# Discriminated union for enricher configs. Add new types here.
EnricherConfigTypes = Annotated[
    WebhookEnricherConfig,
    Field(discriminator="type"),
]


class EnrichmentConfig(BaseModel):
    """
    A model class for specifying configuration related to document enrichment.

    :param enricher_configs: enricher configurations to apply to parsed documents
    """

    enrichers: list[EnricherConfigTypes] = Field(default_factory=list)  # type: ignore[reportUnknownVariableType]
    timeout_seconds: float | None = None


class IndexConfig(BaseModel):
    """
    A model class for specifying configuration related to a search index.

    :param number_of_shards: the total number of shards to split the index into
    :param number_of_replicas: the number of replicas for each shard. Number of shards
        will be multiplied by this number to determine the total number of shards used.
    :param knn_index_engine: the KNN index engine to use. Leave unset unless advised
        by cohere.
    :param analyzer: Analyzer is a parameter set for multilinguality. If None
        it will use the default from compass.
    :param dense_model: the dense model to use for the index. Leave unset unless advised
        by cohere.
    :param sparse_model: the sparse model to use for the index. Leave unset unless
        advised by cohere.
    """

    number_of_shards: int | None = None
    number_of_replicas: int | None = None
    knn_index_engine: str | None = None
    analyzer: str | None = None
    dense_model: str | None = None
    sparse_model: str | None = None


class SupportedFileType(BaseModel):
    """
    One file format Compass accepts, as a set of MIME types and matching extensions.

    MIME types are modelled as plain strings rather than ContentTypeEnum so that a
    Compass deployment newer than the SDK can advertise types the SDK does not know
    about yet without failing validation.

    :param mime_types: MIME types accepted for this format, canonical type first
        followed by any aliases that resolve to the same extensions.
    :param extensions: File extensions for this format, lowercase and dot-prefixed.
        Empty for formats uploaded by MIME type only, such as application/octet-stream.
    """

    mime_types: list[str] = Field(
        default_factory=list,
        description="MIME types accepted for this format: canonical type first, then aliases.",
    )
    extensions: list[str] = Field(
        default_factory=list,
        description="File extensions (with leading dot) for this format. Empty for MIME-only formats.",
    )


class SupportedFileTypesResponse(BaseModel):
    """
    The file formats a Compass deployment can currently parse and index.

    The set is gated by the deployment's runtime configuration rather than being a
    static capability list: audio formats are advertised only when ASR is enabled, and
    video formats additionally require the video LLM. Query it per deployment instead
    of caching it across environments.

    :param file_types: The supported formats, each pairing MIME types with extensions.
    """

    file_types: list[SupportedFileType] = Field(
        default_factory=list[SupportedFileType],
        description="Supported formats, each pairing accepted MIME types with their file extensions.",
    )

    @property
    def mime_types(self) -> set[str]:
        """All accepted MIME types, flattened across formats."""
        return {mime_type for file_type in self.file_types for mime_type in file_type.mime_types}

    @property
    def extensions(self) -> set[str]:
        """All accepted file extensions, flattened across formats."""
        return {extension for file_type in self.file_types for extension in file_type.extensions}

    def supports(self, *, filename: str | None = None, mime_type: str | None = None) -> bool:
        """
        Check whether Compass accepts a file identified by its name and/or MIME type.

        When both arguments are given the file counts as supported if either one
        matches, since Compass can accept an upload on the strength of either signal.
        Prefer passing the MIME type when you have a trustworthy one, as that is what
        Compass validates at upload time. Callers needing to know which signal matched
        should test :attr:`mime_types` and :attr:`extensions` directly.

        :param filename: File name to check. Only its extension is used, matched
            case-insensitively.
        :param mime_type: MIME type to check. Matched case-insensitively, ignoring any
            parameters such as "; charset=utf-8".

        :return: True if Compass accepts the file, False otherwise.

        :raises ValueError: If neither filename nor mime_type is provided.
        """
        if filename is None and mime_type is None:
            raise ValueError("At least one of filename or mime_type must be provided.")

        if mime_type is not None:
            queried = mime_type.split(";")[0].strip().lower()
            if any(queried == advertised.lower() for advertised in self.mime_types):
                return True

        return filename is not None and PurePosixPath(filename).suffix.lower() in self.extensions
