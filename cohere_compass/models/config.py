"""Models for config functionality in the Cohere Compass SDK."""

# Python imports
import math
from enum import Enum
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
    """

    Granular = "granular"
    Digest = "digest"

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


class ParsingStrategy(str, Enum):
    """Enum for specifying the parsing strategy to use."""

    Fast = "fast"
    Hi_Res = "hi_res"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Fast


class ParsingModel(str, Enum):
    """Enum for specifying the parsing model to use."""

    # Default model, which is actually a combination of models used by the "Marker" PDF
    # parser
    Marker = "marker"
    # Only PDF parsing working option from Unstructured
    YoloX_Quantized = "yolox_quantized"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Marker


class ParserConfig(BaseModel):
    """
    A model class for specifying parsing configuration.

    Important parameters:

    :param parsing_strategy: the parsing strategy to use:
        - 'auto' (default): automatically determine the best strategy
        - 'fast': leverage traditional NLP extraction techniques to quickly pull all the
          text elements. “Fast” strategy is not good for image based file types.
        - 'hi_res': identifies the layout of the document using detectron2. The
          advantage of “hi_res” is that it uses the document layout to gain additional
          information about document elements.  We recommend using this strategy if your
          use case is highly sensitive to correct classifications for document elements.
        - 'ocr_only': leverage Optical Character Recognition to extract text from the
          image based files.
    :param parsing_model: the parsing model to use. One of:
        - yolox_quantized (default): single-stage object detection model, quantized.
          Runs faster than YoloX. See
          https://unstructured-io.github.io/unstructured/best_practices/models.html for
          more details. We have temporarily removed the option to use other models
          because of ongoing stability issues.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    # CompassParser configuration
    parsed_images_output_dir: str | None = None
    allowed_image_types: list[str] | None = None
    min_chars_per_element: int = DEFAULT_MIN_CHARS_PER_ELEMENT
    skip_infer_table_types: list[str] = SKIP_INFER_TABLE_TYPES
    parsing_strategy: ParsingStrategy = ParsingStrategy.Fast
    parsing_model: ParsingModel = ParsingModel.YoloX_Quantized

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
