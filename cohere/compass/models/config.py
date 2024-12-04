# Python imports
from enum import Enum
from os import getenv
from typing import Any, List, Optional
import math

# 3rd party imports
from pydantic import BaseModel, ConfigDict

# Local imports
from cohere.compass.constants import (
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
from cohere.compass.models import ValidatedModel


class DocumentFormat(str, Enum):
    Markdown = "markdown"
    Text = "text"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Markdown


class PDFParsingStrategy(str, Enum):
    QuickText = "QuickText"
    ImageToMarkdown = "ImageToMarkdown"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.QuickText


class PresentationParsingStrategy(str, Enum):
    Unstructured = "Unstructured"
    ImageToMarkdown = "ImageToMarkdown"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Unstructured


class ParsingStrategy(str, Enum):
    Fast = "fast"
    Hi_Res = "hi_res"

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Fast


class ParsingModel(str, Enum):
    Marker = "marker"  # Default model, it is actually a combination of models used by the Marker PDF parser
    YoloX_Quantized = (
        "yolox_quantized"  # Only PDF parsing working option from Unstructured
    )

    @classmethod
    def _missing_(cls, value: Any):
        return cls.Marker


class ParserConfig(BaseModel):
    """
    CompassParser configuration. Important parameters:
    :param parsing_strategy: the parsing strategy to use:
        - 'auto' (default): automatically determine the best strategy
        - 'fast': leverage traditional NLP extraction techniques to quickly pull all the
         text elements. “Fast” strategy is not good for image based file types.
        - 'hi_res': identifies the layout of the document using detectron2. The advantage of “hi_res”
         is that it uses the document layout to gain additional information about document elements.
         We recommend using this strategy if your use case is highly sensitive to correct
         classifications for document elements.
        - 'ocr_only': leverage Optical Character Recognition to extract text from the image based files.
    :param parsing_model: the parsing model to use. One of:
        - yolox_quantized (default): single-stage object detection model, quantized. Runs faster than YoloX
        See https://unstructured-io.github.io/unstructured/best_practices/models.html for more details.
        We have temporarily removed the option to use other models because
        of ongoing stability issues.

    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
    )

    # CompassParser configuration
    parse_tables: bool = True
    parse_images: bool = True
    parsed_images_output_dir: Optional[str] = None
    allowed_image_types: Optional[List[str]] = None
    min_chars_per_element: int = DEFAULT_MIN_CHARS_PER_ELEMENT
    skip_infer_table_types: List[str] = SKIP_INFER_TABLE_TYPES
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
    presentation_parsing_strategy: PresentationParsingStrategy = (
        PresentationParsingStrategy.Unstructured
    )


class MetadataStrategy(str, Enum):
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
    Configuration class for metadata detection.
    :param metadata_strategy: the metadata detection strategy to use. One of:
        - No_Metadata: no metadata is inferred
        - Heuristics: metadata is inferred using heuristics
        - Bart: metadata is inferred using the BART summarization model
        - Command_R: metadata is inferred using the Command-R summarization model
    :param cohere_api_key: the Cohere API key to use for metadata detection
    :param commandr_model_name: the name of the Command-R model to use for metadata detection
    :param commandr_prompt: the prompt to use for the Command-R model
    :param commandr_extractable_attributes: the extractable attributes for the Command-R model
    :param commandr_max_tokens: the maximum number of tokens to use for the Command-R model
    :param keyword_search_attributes: the attributes to search for in the document when using keyword search
    :param keyword_search_separator: the separator to use for nested attributes when using keyword search
    :param ignore_errors: if set to True, metadata detection errors will not be raised or stop the parsing process

    """

    metadata_strategy: MetadataStrategy = MetadataStrategy.No_Metadata
    cohere_api_key: Optional[str] = getenv(COHERE_API_ENV_VAR, None)
    commandr_model_name: str = "command-r"
    commandr_prompt: str = DEFAULT_COMMANDR_PROMPT
    commandr_max_tokens: int = 500
    commandr_extractable_attributes: List[str] = DEFAULT_COMMANDR_EXTRACTABLE_ATTRIBUTES
    keyword_search_attributes: List[str] = METADATA_HEURISTICS_ATTRIBUTES
    keyword_search_separator: str = "."
    ignore_errors: bool = True
