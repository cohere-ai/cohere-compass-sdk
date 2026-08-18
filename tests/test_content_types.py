from cohere_compass.content_types import ParserCapability, supported_file_types


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
