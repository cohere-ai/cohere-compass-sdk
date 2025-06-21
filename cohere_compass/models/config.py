# Python imports
import math
from enum import Enum
from os import getenv
from typing import Any, Optional

# 3rd party imports
from pydantic import BaseModel, ConfigDict

# Local imports
from cohere_compass.constants import (
    COHERE_API_ENV_VAR,
    DEFAULT_COMMANDR_EXTRACTABLE_ATTRIBUTES,
    DEFAULT_COMMANDR_PROMPT,
    DEFAULT_MIN_CHARS_PER_ELEMENT,
    DEFAULT_MIN_NUM_CHUNKS_IN_TITLE,
    DEFAULT_MIN_NUM_TOKENS_CHUNK,
    DEFAULT_NUM_TOKENS_CHUNK_OVERLAP,
    DEFAULT_NUM_TOKENS_PER_CHUNK,
    METADATA_HEURISTICS_ATTRIBUTES,
    SKIP_INFER_TABLE_TYPES,
)
from cohere_compass.models import ValidatedModel


class DocumentFormat(str, Enum):
    """Enum for specifying the output format of the parsed document."""

    Markdown = "markdown"
    Text = "text"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Markdown


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

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Unstructured


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
    parse_tables: bool = True
    parse_images: bool = True
    parsed_images_output_dir: Optional[str] = None
    allowed_image_types: Optional[list[str]] = None
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
    include_tables: bool = True

    # Formatting configuration
    output_format: DocumentFormat = DocumentFormat.Markdown

    # Visual elements extraction configuration
    extract_visual_elements: bool = False
    vertical_table_crop_margin: int = 100
    horizontal_table_crop_margin: int = 100

    pdf_parsing_strategy: PDFParsingStrategy = PDFParsingStrategy.QuickText

    pdf_parsing_config: PDFParsingConfig = PDFParsingConfig()

    presentation_parsing_strategy: PresentationParsingStrategy = (
        PresentationParsingStrategy.Unstructured
    )

    enable_assets_returned_as_base64: bool = True


class MetadataStrategy(str, Enum):
    """Enum for specifying the strategy for metadata detection."""

    No_Metadata = "no_metadata"
    Naive_Title = "naive_title"
    KeywordSearch = "keyword_search"
    Bart = "bart"
    Command_R = "command_r"
    Custom = "custom"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.No_Metadata


class MetadataConfig(ValidatedModel):
    """
    A model class for specifying configuration related to document metadata detection.

    :param metadata_strategy: the metadata detection strategy to use. One of:
        - No_Metadata: no metadata is inferred
        - Heuristics: metadata is inferred using heuristics
        - Bart: metadata is inferred using the BART summarization model
        - Command_R: metadata is inferred using the Command-R summarization model
    :param cohere_api_key: the Cohere API key to use for metadata detection
    :param commandr_model_name: the name of the Command-R model to use for metadata
    detection
    :param commandr_prompt: the prompt to use for the Command-R model
    :param commandr_extractable_attributes: the extractable attributes for the Command-R
        model
    :param commandr_max_tokens: the maximum number of tokens to use for the Command-R
        model
    :param keyword_search_attributes: the attributes to search for in the document when
        using keyword search
    :param keyword_search_separator: the separator to use for nested attributes when
        using keyword search
    :param ignore_errors: if set to True, metadata detection errors will not be raised
        or stop the parsing process

    """

    metadata_strategy: MetadataStrategy = MetadataStrategy.No_Metadata
    cohere_api_key: Optional[str] = getenv(COHERE_API_ENV_VAR, None)
    commandr_model_name: str = "command-r"
    commandr_prompt: str = DEFAULT_COMMANDR_PROMPT
    commandr_max_tokens: int = 500
    commandr_extractable_attributes: list[str] = DEFAULT_COMMANDR_EXTRACTABLE_ATTRIBUTES
    keyword_search_attributes: list[str] = METADATA_HEURISTICS_ATTRIBUTES
    keyword_search_separator: str = "."
    ignore_errors: bool = True


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

    number_of_shards: Optional[int] = None
    number_of_replicas: Optional[int] = None
    knn_index_engine: Optional[str] = None
    analyzer: Optional[str] = None
    dense_model: Optional[str] = None
    sparse_model: Optional[str] = None
