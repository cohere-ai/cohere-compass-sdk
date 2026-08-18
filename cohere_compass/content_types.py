"""
The file types Compass accepts, as MIME types and the file extensions that map to them.

:data:`BASELINE_SUPPORTED_FILE_TYPES` is the minimal set: the file types every Compass
deployment accepts, whatever its configuration. Audio and video are excluded because they
depend on backends a given deployment may not run. For what a specific deployment
accepts, call ``CompassClient.get_supported_file_types()``.
"""

# Python imports
from collections.abc import Collection
from enum import Enum

# Local imports
from cohere_compass.models.config import SupportedFileType, SupportedFileTypesResponse
from cohere_compass.models.documents import ContentTypeEnum


class ParserCapability(str, Enum):
    """
    An optional backend a Compass deployment may or may not run.

    A content type listed in :data:`CONTENT_TYPE_REQUIRED_CAPABILITIES` is accepted only
    by deployments that have every capability it requires.
    """

    ASR = "asr"
    VIDEO_LLM = "video_llm"


# Content types accepted only by deployments with the listed capabilities. Types absent
# from this mapping are accepted by every deployment. Video requires ASR as well as the
# video LLM, because video parsing transcribes the audio track.
CONTENT_TYPE_REQUIRED_CAPABILITIES: dict[ContentTypeEnum, frozenset[ParserCapability]] = {
    ContentTypeEnum.AudioMpeg: frozenset({ParserCapability.ASR}),
    ContentTypeEnum.AudioWav: frozenset({ParserCapability.ASR}),
    ContentTypeEnum.AudioMp3: frozenset({ParserCapability.ASR}),
    ContentTypeEnum.VideoMp4: frozenset({ParserCapability.ASR, ParserCapability.VIDEO_LLM}),
    ContentTypeEnum.VideoXMsVideo: frozenset({ParserCapability.ASR, ParserCapability.VIDEO_LLM}),
}


# File extension -> content type. Each extension maps to exactly one content type, so
# alias types that share an extension contribute none of their own; the canonical type
# owns the suffix.
EXTENSION_TO_CONTENT_TYPE: dict[str, ContentTypeEnum] = {
    ".txt": ContentTypeEnum.TextPlain,
    ".html": ContentTypeEnum.TextHtml,
    ".htm": ContentTypeEnum.TextHtml,
    ".csv": ContentTypeEnum.TextCsv,
    ".tsv": ContentTypeEnum.TextTsv,
    ".tab": ContentTypeEnum.TextTsv,
    ".md": ContentTypeEnum.TextMarkdown,
    ".org": ContentTypeEnum.TextOrg,
    ".rst": ContentTypeEnum.TextRst,
    ".json": ContentTypeEnum.ApplicationJson,
    ".jsonl": ContentTypeEnum.ApplicationJsonl,
    ".pdf": ContentTypeEnum.ApplicationPdf,
    ".xml": ContentTypeEnum.ApplicationXml,
    ".doc": ContentTypeEnum.ApplicationMsword,
    ".docx": ContentTypeEnum.ApplicationVndOpenXMLDocument,
    ".xls": ContentTypeEnum.ApplicationVndMsExcel,
    ".xlsx": ContentTypeEnum.ApplicationVndOpenXMLSpreadsheet,
    ".xlsm": ContentTypeEnum.ApplicationVndMsExcelSheetMacroEnabled,
    ".xltx": ContentTypeEnum.ApplicationVndOpenXMLSpreadsheetTemplate,
    ".xltm": ContentTypeEnum.ApplicationVndMsExcelTemplateMacroEnabled,
    ".ppt": ContentTypeEnum.ApplicationVndMsPowerpoint,
    ".pptx": ContentTypeEnum.ApplicationVndOpenXMLPresentation,
    ".epub": ContentTypeEnum.ApplicationEpubZip,
    ".odt": ContentTypeEnum.ApplicationVndOasisOpenDocumentText,
    ".ods": ContentTypeEnum.ApplicationVndOasisOpenDocumentSpreadsheet,
    ".odp": ContentTypeEnum.ApplicationVndOasisOpenDocumentPresentation,
    ".msg": ContentTypeEnum.ApplicationMsOutlook,
    ".rtf": ContentTypeEnum.ApplicationRtf,
    ".yaml": ContentTypeEnum.ApplicationYaml,
    ".yml": ContentTypeEnum.ApplicationYaml,
    ".hwp": ContentTypeEnum.ApplicationXHwp,
    ".hwpx": ContentTypeEnum.ApplicationXHwpx,
    ".jpg": ContentTypeEnum.ImageJpeg,
    ".jpeg": ContentTypeEnum.ImageJpeg,
    ".png": ContentTypeEnum.ImagePng,
    ".heic": ContentTypeEnum.ImageHeic,
    ".tiff": ContentTypeEnum.ImageTiff,
    ".bmp": ContentTypeEnum.ImageBmp,
    ".gif": ContentTypeEnum.ImageGif,
    ".svg": ContentTypeEnum.ImageSvgXml,
    ".webp": ContentTypeEnum.ImageWebp,
    ".mp3": ContentTypeEnum.AudioMpeg,
    ".wav": ContentTypeEnum.AudioWav,
    ".mp4": ContentTypeEnum.VideoMp4,
    ".avi": ContentTypeEnum.VideoXMsVideo,
    ".eml": ContentTypeEnum.MessageRfc822,
}


# Alias content type -> canonical content type. Aliases are accepted on upload and share
# the canonical type's extensions.
CONTENT_TYPE_ALIASES: dict[ContentTypeEnum, ContentTypeEnum] = {
    ContentTypeEnum.ApplicationJsonLines: ContentTypeEnum.ApplicationJsonl,
    ContentTypeEnum.AudioMp3: ContentTypeEnum.AudioMpeg,
    ContentTypeEnum.TextXMarkdown: ContentTypeEnum.TextMarkdown,
    ContentTypeEnum.ApplicationMarkdown: ContentTypeEnum.TextMarkdown,
    ContentTypeEnum.ApplicationXMarkdown: ContentTypeEnum.TextMarkdown,
    ContentTypeEnum.ImageJpg: ContentTypeEnum.ImageJpeg,
    ContentTypeEnum.ApplicationXlsx: ContentTypeEnum.ApplicationVndOpenXMLSpreadsheet,
    ContentTypeEnum.ApplicationXYaml: ContentTypeEnum.ApplicationYaml,
    ContentTypeEnum.TextYaml: ContentTypeEnum.ApplicationYaml,
    ContentTypeEnum.TextXYaml: ContentTypeEnum.ApplicationYaml,
}


def _derive_content_type_to_extensions() -> dict[ContentTypeEnum, list[str]]:
    mapping: dict[ContentTypeEnum, list[str]] = {ct: [] for ct in ContentTypeEnum}
    for extension, content_type in EXTENSION_TO_CONTENT_TYPE.items():
        mapping[content_type].append(extension)
    for extensions in mapping.values():
        extensions.sort()
    return mapping


# Content type -> its sorted file extensions, derived from EXTENSION_TO_CONTENT_TYPE so
# the two directions cannot disagree. Empty for types uploaded by MIME type only.
CONTENT_TYPE_TO_EXTENSIONS: dict[ContentTypeEnum, list[str]] = _derive_content_type_to_extensions()


def _supported_content_types(available_capabilities: Collection[ParserCapability]) -> list[ContentTypeEnum]:
    available = frozenset(available_capabilities)
    return [ct for ct in ContentTypeEnum if CONTENT_TYPE_REQUIRED_CAPABILITIES.get(ct, frozenset()) <= available]


def resolve_supported_file_types(content_types: Collection[ContentTypeEnum]) -> list[SupportedFileType]:
    """
    Group content types by canonical type, pairing each with its aliases and extensions.

    An alias appears only when it is itself in ``content_types``, so the result never
    lists a MIME type the caller did not accept.

    :param content_types: The accepted content types.

    :return: One entry per accepted non-alias content type, each listing its canonical
        MIME type first, then its accepted aliases.
    """
    supported = set(content_types)
    aliases_by_canonical: dict[ContentTypeEnum, list[ContentTypeEnum]] = {}
    for alias, canonical in CONTENT_TYPE_ALIASES.items():
        aliases_by_canonical.setdefault(canonical, []).append(alias)

    resolved: list[SupportedFileType] = []
    for ct in ContentTypeEnum:
        if ct in CONTENT_TYPE_ALIASES or ct not in supported:
            continue
        mime_types = [ct.value] + [a.value for a in sorted(aliases_by_canonical.get(ct, [])) if a in supported]
        resolved.append(SupportedFileType(mime_types=mime_types, extensions=CONTENT_TYPE_TO_EXTENSIONS[ct]))
    return resolved


# The file types every Compass deployment accepts, whatever its configuration. Treat as
# read-only: it is shared across the process, so mutating it affects every caller.
BASELINE_SUPPORTED_FILE_TYPES: SupportedFileTypesResponse = SupportedFileTypesResponse(
    file_types=resolve_supported_file_types(_supported_content_types(frozenset())),
)
