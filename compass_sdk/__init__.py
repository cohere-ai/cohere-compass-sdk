import logging
import math
import uuid
from enum import Enum, StrEnum
from os import getenv
from typing import Annotated, Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, StringConstraints

from compass_sdk.constants import (
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


class Logger:
    def __init__(self, name: str, log_level: int = logging.INFO):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(log_level)

        formatter = logging.Formatter(f"%(asctime)s-{name}-PID:%(process)d: %(message)s", "%d-%m-%y:%H:%M:%S")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self._logger.addHandler(stream_handler)

    def info(self, msg: str):
        self._logger.info(msg)

    def debug(self, msg: str):
        self._logger.debug(msg)

    def error(self, msg: str):
        self._logger.error(msg)

    def critical(self, msg: str):
        self._logger.critical(msg)

    def warning(self, msg: str):
        self._logger.warning(msg)

    def flush(self):
        for handler in self._logger.handlers:
            handler.flush()

    def setLevel(self, level: Union[int, str]):
        self._logger.setLevel(level)


logger = Logger(name="compass-sdk", log_level=logging.INFO)


class ValidatedModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True

    @classmethod
    def attribute_in_model(cls, attr_name):
        return attr_name in cls.__fields__

    def __init__(self, **data):
        for name, value in data.items():
            if not self.attribute_in_model(name):
                raise ValueError(f"{name} is not a valid attribute for {self.__class__.__name__}")
        super().__init__(**data)


class CompassDocumentMetadata(ValidatedModel):
    """
    Compass document metadata
    """

    doc_id: str = ""
    filename: str = ""
    meta: List = []
    parent_doc_id: str = ""


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


class CompassDocumentChunkAsset(BaseModel):
    content_type: str
    asset_data: str


class CompassDocumentChunk(BaseModel):
    chunk_id: str
    sort_id: str
    doc_id: str
    parent_doc_id: str
    content: Dict[str, Any]
    origin: Optional[Dict[str, Any]] = None
    assets: Optional[list[CompassDocumentChunkAsset]] = None

    def parent_doc_is_split(self):
        return self.doc_id != self.parent_doc_id


class CompassDocument(ValidatedModel):
    """
    A Compass document contains all the information required to process a document and insert it into the index
    It includes:
    - metadata: the document metadata (e.g., filename, title, authors, date)
    - content: the document content in string format
    - elements: the document's Unstructured elements (e.g., tables, images, text). Used for chunking
    - chunks: the document's chunks (e.g., paragraphs, tables, images). Used for indexing
    - index_fields: the fields to be indexed. Used by the indexer
    """

    filebytes: bytes = b""
    metadata: CompassDocumentMetadata = CompassDocumentMetadata()
    content: Dict[str, str] = {}
    content_type: Optional[str] = None
    elements: List[Any] = []
    chunks: List[CompassDocumentChunk] = []
    index_fields: List[str] = []
    errors: List[Dict[CompassSdkStage, str]] = []
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
        return any(stage == CompassSdkStage.Parsing for error in self.errors for stage, _ in error.items())

    def has_metadata_errors(self) -> bool:
        return any(stage == CompassSdkStage.Metadata for error in self.errors for stage, _ in error.items())

    def has_indexing_errors(self) -> bool:
        return any(stage == CompassSdkStage.Indexing for error in self.errors for stage, _ in error.items())

    @property
    def status(self) -> CompassDocumentStatus:
        if self.has_parsing_errors():
            return CompassDocumentStatus.ParsingErrors

        if not self.ignore_metadata_errors and self.has_metadata_errors():
            return CompassDocumentStatus.MetadataErrors

        if self.has_indexing_errors():
            return CompassDocumentStatus.IndexingErrors

        return CompassDocumentStatus.Success


class MetadataStrategy(str, Enum):
    No_Metadata = "no_metadata"
    Naive_Title = "naive_title"
    KeywordSearch = "keyword_search"
    Bart = "bart"
    Command_R = "command_r"
    Custom = "custom"

    @classmethod
    def _missing_(cls, value):
        return cls.No_Metadata


class LoggerLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @classmethod
    def _missing_(cls, value):
        return cls.INFO


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


class ParsingStrategy(str, Enum):
    Fast = "fast"
    Hi_Res = "hi_res"

    @classmethod
    def _missing_(cls, value):
        return cls.Fast


class ParsingModel(str, Enum):
    Marker = "marker"  # Default model, it is actually a combination of models used by the Marker PDF parser
    YoloX_Quantized = "yolox_quantized"  # Only PDF parsing working option from Unstructured

    @classmethod
    def _missing_(cls, value):
        return cls.Marker


class DocumentFormat(str, Enum):
    Markdown = "markdown"
    Text = "text"

    @classmethod
    def _missing_(cls, value):
        return cls.Markdown


class PDFParsingStrategy(StrEnum):
    QuickText = "QuickText"
    ImageToMarkdown = "ImageToMarkdown"

    @classmethod
    def _missing_(cls, value):
        return cls.QuickText


class PresentationParsingStrategy(StrEnum):
    Unstructured = "Unstructured"
    ImageToMarkdown = "ImageToMarkdown"

    @classmethod
    def _missing_(cls, value):
        return cls.Unstructured


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
    logger_level: LoggerLevel = LoggerLevel.INFO
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
    presentation_parsing_strategy: PresentationParsingStrategy = PresentationParsingStrategy.Unstructured


### Document indexing


class DocumentChunkAsset(BaseModel):
    content_type: str
    asset_data: str


class Chunk(BaseModel):
    chunk_id: str
    sort_id: int
    content: Dict[str, Any]
    origin: Optional[Dict[str, Any]] = None
    assets: Optional[list[DocumentChunkAsset]] = None
    parent_doc_id: str


class Document(BaseModel):
    """
    A document that can be indexed in Compass (i.e., a list of indexable chunks)
    """

    doc_id: str
    path: str
    parent_doc_id: str
    content: Dict[str, Any]
    chunks: List[Chunk]
    index_fields: List[str] = []


class ParseableDocument(BaseModel):
    """
    A document to be sent to Compass in bytes format for parsing on the Compass side
    """

    id: uuid.UUID
    filename: Annotated[
        str,
        StringConstraints(min_length=1),
    ]  # Ensures the filename is a non-empty string
    content_type: str
    content_length_bytes: PositiveInt  # File size must be a non-negative integer
    bytes: str  # Base64-encoded file contents
    context: Dict[str, Any] = Field(default_factory=dict)


class PushDocumentsInput(BaseModel):
    documents: List[ParseableDocument]


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


class PutDocumentsInput(BaseModel):
    """
    A Compass request to put a list of Document
    """

    docs: List[Document]
    authorized_groups: Optional[List[str]] = None
    merge_groups_on_conflict: bool = False


class ProcessFileParameters(ValidatedModel):
    parser_config: ParserConfig
    metadata_config: MetadataConfig
    doc_id: Optional[str] = None
    content_type: Optional[str] = None


class ProcessFilesParameters(ValidatedModel):
    doc_ids: Optional[List[str]] = None
    parser_config: ParserConfig
    metadata_config: MetadataConfig


class GroupAuthorizationActions(str, Enum):
    ADD = "add"
    REMOVE = "remove"


class GroupAuthorizationInput(BaseModel):
    doc_ids: List[str]
    authorized_groups: List[str]
    action: GroupAuthorizationActions
