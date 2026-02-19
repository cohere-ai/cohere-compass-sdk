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

DEFAULT_MIN_CHARS_PER_ELEMENT = 0
DEFAULT_NUM_TOKENS_PER_CHUNK = 500
DEFAULT_NUM_TOKENS_CHUNK_OVERLAP = 15
DEFAULT_MIN_NUM_TOKENS_CHUNK = 5
DEFAULT_MIN_NUM_CHUNKS_IN_TITLE = 1

SKIP_INFER_TABLE_TYPES = ["jpg", "png", "xls", "xlsx", "heic"]

UUID_NAMESPACE = "00000000-0000-0000-0000-000000000000"

DEFAULT_COMPASS_CLIENT_TIMEOUT = timedelta(minutes=1)  # seconds
DEFAULT_COMPASS_PARSER_CLIENT_TIMEOUT = timedelta(minutes=5)
URL_SAFE_STRING_PATTERN = r"^[^\s\"\/\\?#><']*$"
