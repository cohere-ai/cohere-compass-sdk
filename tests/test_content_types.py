from cohere_compass.content_types import MINIMAL_SUPPORTED_FILE_TYPES


def test_minimal_set_excludes_capability_gated_formats() -> None:
    assert not any(mime_type.startswith(("audio/", "video/")) for mime_type in MINIMAL_SUPPORTED_FILE_TYPES.mime_types)
    assert not {".mp3", ".wav", ".mp4", ".avi"} & MINIMAL_SUPPORTED_FILE_TYPES.extensions


def test_minimal_set_accepts_ungated_formats() -> None:
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(filename="report.pdf") is True
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(filename="values.yml") is True
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(filename="budget.xlsm") is True
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(mime_type="application/x-yaml") is True


def test_minimal_set_rejects_gated_and_unknown_formats() -> None:
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(filename="interview.mp3") is False
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(mime_type="audio/mpeg") is False
    assert MINIMAL_SUPPORTED_FILE_TYPES.supports(filename="archive.zip") is False


def test_minimal_set_accepts_every_type_it_advertises() -> None:
    for mime_type in MINIMAL_SUPPORTED_FILE_TYPES.mime_types:
        assert MINIMAL_SUPPORTED_FILE_TYPES.supports(mime_type=mime_type) is True
    for extension in MINIMAL_SUPPORTED_FILE_TYPES.extensions:
        assert MINIMAL_SUPPORTED_FILE_TYPES.supports(filename=f"document{extension}") is True
