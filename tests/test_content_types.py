import pytest
from pydantic import ValidationError

from cohere_compass.content_types import (
    ParserCapability,
    ParserFamily,
    parse_supported_file_types,
    supported_file_types,
)
from cohere_compass.models import ContentTypeEnum


def test_supported_file_types_without_capabilities_excludes_gated_formats() -> None:
    result = supported_file_types()
    assert not any(mime_type.startswith(("audio/", "video/")) for mime_type in result.mime_types)
    assert not {".mp3", ".wav", ".mp4", ".avi"} & result.extensions


def test_supported_file_types_without_capabilities_accepts_ungated_formats() -> None:
    result = supported_file_types()
    assert result.supports(filename="report.pdf") is True
    assert result.supports(filename="values.yml") is True
    assert result.supports(filename="budget.xlsm") is True
    assert result.supports(mime_type="application/x-yaml") is True


def test_supported_file_types_without_capabilities_rejects_gated_and_unknown_formats() -> None:
    result = supported_file_types()
    assert result.supports(filename="interview.mp3") is False
    assert result.supports(mime_type="audio/mpeg") is False
    assert result.supports(filename="archive.zip") is False


def test_supported_file_types_with_asr_includes_audio_but_not_video() -> None:
    result = supported_file_types([ParserCapability.ASR])
    assert result.supports(filename="interview.mp3") is True
    assert result.supports(filename="clip.mp4") is False


def test_supported_file_types_with_asr_and_video_llm_includes_video() -> None:
    result = supported_file_types([ParserCapability.ASR, ParserCapability.VIDEO_LLM])
    assert result.supports(filename="interview.mp3") is True
    assert result.supports(filename="clip.mp4") is True


def test_supported_file_types_accepts_every_type_it_advertises() -> None:
    result = supported_file_types()
    for mime_type in result.mime_types:
        assert result.supports(mime_type=mime_type) is True
    for extension in result.extensions:
        assert result.supports(filename=f"document{extension}") is True


def test_supported_file_types_with_all_capabilities_declares_known_content_type_and_parser_family() -> None:
    """Every format names its first MIME type as a declarable ContentTypeEnum member and a known ParserFamily."""
    result = supported_file_types([ParserCapability.ASR, ParserCapability.VIDEO_LLM])
    for file_type in result.file_types:
        assert file_type.content_type == file_type.mime_types[0]
        assert ContentTypeEnum(file_type.content_type).value == file_type.content_type
        assert ParserFamily(file_type.parser_family).value == file_type.parser_family


def test_supported_file_types_with_all_capabilities_covers_every_content_type_enum_member() -> None:
    """The offline table and ContentTypeEnum hold the same MIME types, so neither gains one the other lacks."""
    result = supported_file_types([ParserCapability.ASR, ParserCapability.VIDEO_LLM])
    assert result.mime_types == {member.value for member in ContentTypeEnum}


def test_parse_supported_file_types_without_content_type_or_mime_types_raises_validation_error() -> None:
    """An entry with nothing to take a content_type from is rejected rather than given an invented one."""
    with pytest.raises(ValidationError):
        parse_supported_file_types({"file_types": [{"mime_types": [], "extensions": []}]})
