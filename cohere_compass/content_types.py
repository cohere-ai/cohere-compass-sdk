"""File types Compass can parse, gated by optional parser capabilities."""

from collections.abc import Collection
from enum import Enum
from typing import NamedTuple

from cohere_compass.models.config import SupportedFileType, SupportedFileTypesResponse


class ParserCapability(str, Enum):
    """A gated parser backend a Compass deployment may have."""

    ASR = "asr"
    VIDEO_LLM = "video_llm"


class _FileType(NamedTuple):
    mime_types: tuple[str, ...]
    extensions: tuple[str, ...]
    required_capabilities: tuple[ParserCapability, ...] = ()


# MIME types, extensions, and any parser capabilities the format requires.
_FILE_TYPES: tuple[_FileType, ...] = (
    _FileType(("text/plain",), (".txt",)),
    _FileType(("text/html",), (".htm", ".html")),
    _FileType(("text/csv",), (".csv",)),
    _FileType(("text/tab-separated-values",), (".tab", ".tsv")),
    _FileType(
        ("text/markdown", "application/markdown", "application/x-markdown", "text/x-markdown"),
        (".md",),
    ),
    _FileType(("text/org",), (".org",)),
    _FileType(("text/prs.fallenstein.rst",), (".rst",)),
    _FileType(("application/json",), (".json",)),
    _FileType(("application/jsonl", "application/json-lines"), (".jsonl",)),
    _FileType(("application/pdf",), (".pdf",)),
    _FileType(("application/xml",), (".xml",)),
    _FileType(("application/msword",), (".doc",)),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",),
        (".docx",),
    ),
    _FileType(("application/vnd.ms-excel",), (".xls",)),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/xlsx"),
        (".xlsx",),
    ),
    _FileType(("application/vnd.ms-excel.sheet.macroEnabled.12",), (".xlsm",)),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.template",),
        (".xltx",),
    ),
    _FileType(("application/vnd.ms-excel.template.macroEnabled.12",), (".xltm",)),
    _FileType(("application/vnd.ms-powerpoint",), (".ppt",)),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.presentationml.presentation",),
        (".pptx",),
    ),
    _FileType(("application/epub+zip",), (".epub",)),
    _FileType(("application/vnd.oasis.opendocument.text",), (".odt",)),
    _FileType(("application/vnd.oasis.opendocument.spreadsheet",), (".ods",)),
    _FileType(("application/vnd.oasis.opendocument.presentation",), (".odp",)),
    _FileType(("application/vnd.ms-outlook",), (".msg",)),
    _FileType(("application/octet-stream",), ()),
    _FileType(("application/rtf",), (".rtf",)),
    _FileType(
        ("application/yaml", "application/x-yaml", "text/x-yaml", "text/yaml"),
        (".yaml", ".yml"),
    ),
    _FileType(("application/vnd.cohere.compassV1+json",), ()),
    _FileType(("application/x-hwp",), (".hwp",)),
    _FileType(("application/x-hwpx",), (".hwpx",)),
    _FileType(("image/jpeg", "image/jpg"), (".jpeg", ".jpg")),
    _FileType(("image/png",), (".png",)),
    _FileType(("image/heic",), (".heic",)),
    _FileType(("image/tiff",), (".tiff",)),
    _FileType(("image/bmp",), (".bmp",)),
    _FileType(("image/gif",), (".gif",)),
    _FileType(("image/svg+xml",), (".svg",)),
    _FileType(("image/webp",), (".webp",)),
    _FileType(("message/rfc822",), (".eml",)),
    _FileType(("audio/mpeg", "audio/mp3"), (".mp3",), (ParserCapability.ASR,)),
    _FileType(("audio/wav",), (".wav",), (ParserCapability.ASR,)),
    _FileType(("video/mp4",), (".mp4",), (ParserCapability.ASR, ParserCapability.VIDEO_LLM)),
    _FileType(("video/x-msvideo",), (".avi",), (ParserCapability.ASR, ParserCapability.VIDEO_LLM)),
)


def supported_file_types(
    capabilities: Collection[ParserCapability] = (),
) -> SupportedFileTypesResponse:
    """File types accepted given the parser backends this deployment is known to have."""
    available = frozenset(capabilities)
    return SupportedFileTypesResponse(
        file_types=[
            SupportedFileType(mime_types=list(file_type.mime_types), extensions=list(file_type.extensions))
            for file_type in _FILE_TYPES
            if set(file_type.required_capabilities) <= available
        ]
    )
