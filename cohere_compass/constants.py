"""
Constants and default values used throughout the Cohere Compass SDK.

This module defines configuration constants for timeouts, retries, chunk sizes,
and other default parameters used by the SDK.
"""

from datetime import timedelta

DEFAULT_MAX_CHUNKS_PER_REQUEST = 100
DEFAULT_RETRY_WAIT = timedelta(seconds=5)
DEFAULT_MAX_RETRIES = 3
DEFAULT_MAX_ERROR_RATE = 0.5

DEFAULT_MIN_CHARS_PER_ELEMENT = 3
DEFAULT_NUM_TOKENS_PER_CHUNK = 500
DEFAULT_NUM_TOKENS_CHUNK_OVERLAP = 15
DEFAULT_MIN_NUM_TOKENS_CHUNK = 5
DEFAULT_MIN_NUM_CHUNKS_IN_TITLE = 1

DEFAULT_WIDTH_HEIGHT_VERTICAL_RATIO = 0.6
SKIP_INFER_TABLE_TYPES = ["jpg", "png", "xls", "xlsx", "heic"]

# Metadata detection constants
COHERE_API_ENV_VAR = "COHERE_API_KEY"
DEFAULT_COMMANDR_EXTRACTABLE_ATTRIBUTES = ["title", "authors", "date"]
DEFAULT_COMMANDR_PROMPT = """
        Given the following document:
        {text}.
        Extract the following attributes from the document: {attributes}.
        Write the output in JSON format. For example, if the document title is "Hello World"
        and the authors are "John Doe" and "Jane Smith", the output should be:
        {{"title": "Hello World", "authors": ["John Doe", "Jane Smith"]}}.
        Do not write the ```json (...) ``` tag. The output should be a valid JSON.
        If you cannot find the information, write "" for the corresponding field.
        Answer:
        """  # noqa: E501
METADATA_HEURISTICS_ATTRIBUTES = [
    "title",
    "name",
    "date",
    "authors",
]

UUID_NAMESPACE = "00000000-0000-0000-0000-000000000000"

DEFAULT_COMPASS_CLIENT_TIMEOUT = timedelta(minutes=1)  # seconds
DEFAULT_COMPASS_PARSER_CLIENT_TIMEOUT = timedelta(minutes=5)
URL_SAFE_STRING_PATTERN = r"^[^\s\"\/\\?#><']*$"
