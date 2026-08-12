import pytest

from cohere_compass.models.config import SupportedFileType, SupportedFileTypesResponse


@pytest.fixture
def supported_file_types() -> SupportedFileTypesResponse:
    """A response covering an alias group, a multi-extension format, and a MIME-only format."""
    return SupportedFileTypesResponse(
        file_types=[
            SupportedFileType(mime_types=["application/pdf"], extensions=[".pdf"]),
            SupportedFileType(mime_types=["text/html"], extensions=[".htm", ".html"]),
            SupportedFileType(mime_types=["audio/mpeg", "audio/mp3"], extensions=[".mp3"]),
            SupportedFileType(mime_types=["application/octet-stream"], extensions=[]),
        ]
    )


def test_mime_types_flattens_every_format_including_aliases(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Aliases are surfaced alongside canonical types so callers can match either."""
    assert supported_file_types.mime_types == {
        "application/pdf",
        "text/html",
        "audio/mpeg",
        "audio/mp3",
        "application/octet-stream",
    }


def test_extensions_flattens_every_format_and_omits_mime_only_formats(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Formats with no extensions contribute nothing rather than an empty string entry."""
    assert supported_file_types.extensions == {".pdf", ".htm", ".html", ".mp3"}


def test_supports_matching_extension_returns_true(supported_file_types: SupportedFileTypesResponse) -> None:
    """A filename whose extension maps to an advertised format is supported."""
    assert supported_file_types.supports(filename="quarterly-report.pdf") is True


def test_supports_uppercase_extension_returns_true(supported_file_types: SupportedFileTypesResponse) -> None:
    """Extension matching is case-insensitive, since the server advertises lowercase only."""
    assert supported_file_types.supports(filename="QUARTERLY-REPORT.PDF") is True


def test_supports_extension_on_path_like_filename_returns_true(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Only the final suffix is considered, so full paths from connectors work unchanged."""
    assert supported_file_types.supports(filename="/shared/docs/report.pdf") is True


def test_supports_unknown_extension_returns_false(supported_file_types: SupportedFileTypesResponse) -> None:
    """An extension no advertised format claims is unsupported."""
    assert supported_file_types.supports(filename="archive.zip") is False


def test_supports_filename_without_extension_returns_false(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """An empty suffix must not accidentally match a format that advertises no extensions."""
    assert supported_file_types.supports(filename="README") is False


def test_supports_matching_mime_type_returns_true(supported_file_types: SupportedFileTypesResponse) -> None:
    """A MIME type advertised by any format is supported."""
    assert supported_file_types.supports(mime_type="application/pdf") is True


def test_supports_alias_mime_type_returns_true(supported_file_types: SupportedFileTypesResponse) -> None:
    """Alias MIME types are accepted, not just the canonical type of each format."""
    assert supported_file_types.supports(mime_type="audio/mp3") is True


def test_supports_mime_type_with_parameters_returns_true(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Content-Type parameters such as charset are stripped before matching."""
    assert supported_file_types.supports(mime_type="text/html; charset=utf-8") is True


def test_supports_uppercase_mime_type_returns_true(supported_file_types: SupportedFileTypesResponse) -> None:
    """MIME matching is case-insensitive, as MIME types are case-insensitive per RFC 2045."""
    assert supported_file_types.supports(mime_type="Application/PDF") is True


def test_supports_mime_only_format_returns_true(supported_file_types: SupportedFileTypesResponse) -> None:
    """Formats advertised without extensions are still matchable by MIME type."""
    assert supported_file_types.supports(mime_type="application/octet-stream") is True


def test_supports_unknown_mime_type_returns_false(supported_file_types: SupportedFileTypesResponse) -> None:
    """A MIME type no format advertises is unsupported."""
    assert supported_file_types.supports(mime_type="application/zip") is False


def test_supports_returns_true_when_only_one_of_two_signals_matches(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Either signal matching is enough, so a generic MIME type does not veto a known extension."""
    assert supported_file_types.supports(filename="report.pdf", mime_type="application/zip") is True


def test_supports_returns_false_when_neither_signal_matches(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Both signals failing means the file is unsupported."""
    assert supported_file_types.supports(filename="archive.zip", mime_type="application/zip") is False


def test_supports_without_arguments_raises_value_error(
    supported_file_types: SupportedFileTypesResponse,
) -> None:
    """Calling with no identifiers is a programming error rather than a silent False."""
    with pytest.raises(ValueError, match="At least one of filename or mime_type"):
        supported_file_types.supports()


def test_supports_on_empty_response_returns_false() -> None:
    """A deployment advertising nothing supports nothing."""
    assert SupportedFileTypesResponse().supports(filename="report.pdf") is False
