"""File types Compass can parse, gated by optional parser capabilities."""

from collections.abc import Collection
from enum import Enum
from typing import Any, NamedTuple, cast

from cohere_compass.models.config import SupportedFileType, SupportedFileTypesResponse


class ParserCapability(str, Enum):
    """A gated parser backend a Compass deployment may have."""

    ASR = "asr"
    VIDEO_LLM = "video_llm"


class ParserFamily(str, Enum):
    """
    The parser backend Compass routes a file format to.

    SupportedFileType.parser_family stays a plain string because a deployment newer
    than the SDK may send a family missing here. Compare response values against
    members rather than constructing ParserFamily from them.
    """

    PREPARSED = "preparsed"
    PDF = "pdf"
    JSON = "json"
    CSV = "csv"
    TABULAR = "tabular"
    SPREADSHEET = "spreadsheet"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    PRESENTATION = "presentation"
    LIBREOFFICE = "libreoffice"
    UNSTRUCTURED = "unstructured"


class _FileType(NamedTuple):
    mime_types: tuple[str, ...]
    extensions: tuple[str, ...]
    parser_family: ParserFamily
    required_capabilities: tuple[ParserCapability, ...] = ()


# MIME types (canonical first), extensions, the parser family the format routes to, and
# any parser capabilities the format requires.
_FILE_TYPES: tuple[_FileType, ...] = (
    _FileType(("text/plain",), (".txt",), ParserFamily.UNSTRUCTURED),
    _FileType(("text/html",), (".htm", ".html"), ParserFamily.UNSTRUCTURED),
    _FileType(("text/csv",), (".csv",), ParserFamily.CSV),
    _FileType(("text/tab-separated-values",), (".tab", ".tsv"), ParserFamily.TABULAR),
    _FileType(
        ("text/markdown", "application/markdown", "application/x-markdown", "text/x-markdown"),
        (".md",),
        ParserFamily.UNSTRUCTURED,
    ),
    _FileType(("text/org",), (".org",), ParserFamily.UNSTRUCTURED),
    _FileType(("text/prs.fallenstein.rst",), (".rst",), ParserFamily.UNSTRUCTURED),
    _FileType(("application/json",), (".json",), ParserFamily.JSON),
    _FileType(("application/jsonl", "application/json-lines"), (".jsonl",), ParserFamily.JSON),
    _FileType(("application/vnd.apache.parquet",), (".parquet",), ParserFamily.TABULAR),
    _FileType(("application/pdf",), (".pdf",), ParserFamily.PDF),
    _FileType(("application/xml", "text/xml"), (".xml",), ParserFamily.UNSTRUCTURED),
    _FileType(("application/msword",), (".doc",), ParserFamily.LIBREOFFICE),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",),
        (".docx",),
        ParserFamily.LIBREOFFICE,
    ),
    _FileType(("application/vnd.ms-excel",), (".xls",), ParserFamily.SPREADSHEET),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/xlsx"),
        (".xlsx",),
        ParserFamily.SPREADSHEET,
    ),
    _FileType(("application/vnd.ms-excel.sheet.macroEnabled.12",), (".xlsm",), ParserFamily.SPREADSHEET),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.template",),
        (".xltx",),
        ParserFamily.SPREADSHEET,
    ),
    _FileType(("application/vnd.ms-excel.template.macroEnabled.12",), (".xltm",), ParserFamily.SPREADSHEET),
    _FileType(("application/vnd.ms-powerpoint",), (".ppt",), ParserFamily.PRESENTATION),
    _FileType(
        ("application/vnd.openxmlformats-officedocument.presentationml.presentation",),
        (".pptx",),
        ParserFamily.PRESENTATION,
    ),
    _FileType(("application/epub+zip",), (".epub",), ParserFamily.UNSTRUCTURED),
    _FileType(("application/vnd.oasis.opendocument.text",), (".odt",), ParserFamily.LIBREOFFICE),
    _FileType(("application/vnd.oasis.opendocument.spreadsheet",), (".ods",), ParserFamily.SPREADSHEET),
    _FileType(("application/vnd.oasis.opendocument.presentation",), (".odp",), ParserFamily.PRESENTATION),
    _FileType(("application/vnd.ms-outlook",), (".msg",), ParserFamily.UNSTRUCTURED),
    _FileType(("application/octet-stream",), (), ParserFamily.UNSTRUCTURED),
    _FileType(("application/rtf",), (".rtf",), ParserFamily.UNSTRUCTURED),
    _FileType(
        ("application/yaml", "application/x-yaml", "text/x-yaml", "text/yaml"),
        (".yaml", ".yml"),
        ParserFamily.UNSTRUCTURED,
    ),
    _FileType(("application/vnd.cohere.compassV1+json",), (), ParserFamily.PREPARSED),
    _FileType(("application/x-hwp",), (".hwp",), ParserFamily.LIBREOFFICE),
    _FileType(("application/x-hwpx",), (".hwpx",), ParserFamily.LIBREOFFICE),
    _FileType(("image/jpeg", "image/jpg"), (".jpeg", ".jpg"), ParserFamily.IMAGE),
    _FileType(("image/png",), (".png",), ParserFamily.IMAGE),
    _FileType(("image/heic",), (".heic",), ParserFamily.IMAGE),
    _FileType(("image/tiff",), (".tiff",), ParserFamily.IMAGE),
    _FileType(("image/bmp",), (".bmp",), ParserFamily.IMAGE),
    _FileType(("image/gif",), (".gif",), ParserFamily.IMAGE),
    _FileType(("image/svg+xml",), (".svg",), ParserFamily.UNSTRUCTURED),
    _FileType(("image/webp",), (".webp",), ParserFamily.IMAGE),
    _FileType(("message/rfc822",), (".eml",), ParserFamily.UNSTRUCTURED),
    _FileType(("audio/mpeg", "audio/mp3"), (".mp3",), ParserFamily.AUDIO, (ParserCapability.ASR,)),
    _FileType(("audio/wav",), (".wav",), ParserFamily.AUDIO, (ParserCapability.ASR,)),
    _FileType(("video/mp4",), (".mp4",), ParserFamily.VIDEO, (ParserCapability.ASR, ParserCapability.VIDEO_LLM)),
    _FileType(("video/x-msvideo",), (".avi",), ParserFamily.VIDEO, (ParserCapability.ASR, ParserCapability.VIDEO_LLM)),
)

_PARSER_FAMILY_BY_MIME_TYPE: dict[str, ParserFamily] = {
    mime_type: file_type.parser_family for file_type in _FILE_TYPES for mime_type in file_type.mime_types
}


def supported_file_types(
    capabilities: Collection[ParserCapability] = (),
) -> SupportedFileTypesResponse:
    """File types accepted given the parser backends this deployment is known to have."""
    available = frozenset(capabilities)
    return SupportedFileTypesResponse(
        file_types=[
            SupportedFileType(
                content_type=file_type.mime_types[0],
                parser_family=file_type.parser_family.value,
                mime_types=list(file_type.mime_types),
                extensions=list(file_type.extensions),
            )
            for file_type in _FILE_TYPES
            if set(file_type.required_capabilities) <= available
        ]
    )


def parse_supported_file_types(payload: dict[str, Any]) -> SupportedFileTypesResponse:
    """
    Validate a supported-file-types response, filling in fields a deployment omits.

    When an entry omits content_type, its first MIME type is used. When it omits
    parser_family, the family is looked up in the SDK's built-in table, falling back
    to unstructured for types the table does not list. Values the deployment sends
    are kept as they are.

    :param payload: The decoded JSON body of the supported-file-types endpoint.

    :return: The validated response.

    :raises pydantic.ValidationError: If the payload is malformed, including an entry
        with no content_type and no MIME types to take one from.
    """
    file_types: Any = payload.get("file_types")
    if not isinstance(file_types, list):
        return SupportedFileTypesResponse.model_validate(payload)
    return SupportedFileTypesResponse.model_validate(
        {**payload, "file_types": [_with_legacy_defaults(entry) for entry in cast(list[Any], file_types)]}
    )


def _with_legacy_defaults(entry: Any) -> Any:
    if not isinstance(entry, dict):
        return entry
    filled = dict(cast(dict[str, Any], entry))
    mime_types: Any = filled.get("mime_types")
    if filled.get("content_type") is None and isinstance(mime_types, list) and mime_types:
        filled["content_type"] = cast(list[Any], mime_types)[0]
    content_type: Any = filled.get("content_type")
    if filled.get("parser_family") is None and isinstance(content_type, str):
        filled["parser_family"] = _PARSER_FAMILY_BY_MIME_TYPE.get(content_type, ParserFamily.UNSTRUCTURED).value
    return filled
