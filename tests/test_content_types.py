# pyright: reportPrivateUsage=false

from cohere_compass.content_types import (
    BASELINE_SUPPORTED_FILE_TYPES,
    CONTENT_TYPE_ALIASES,
    CONTENT_TYPE_REQUIRED_CAPABILITIES,
    CONTENT_TYPE_TO_EXTENSIONS,
    EXTENSION_TO_CONTENT_TYPE,
    ParserCapability,
    _supported_content_types,
    resolve_supported_file_types,
)
from cohere_compass.models.config import SupportedFileTypesResponse
from cohere_compass.models.documents import ContentTypeEnum

ALL_CAPABILITIES = frozenset(ParserCapability)


def test_supported_content_types_without_capabilities_excludes_every_gated_type() -> None:
    """The baseline must omit anything a deployment could be missing the backend for."""
    baseline = set(_supported_content_types(frozenset()))
    assert baseline.isdisjoint(CONTENT_TYPE_REQUIRED_CAPABILITIES)


def test_supported_content_types_without_capabilities_includes_every_ungated_type() -> None:
    """The gating map is the only thing that removes a type, so the rest must be present."""
    baseline = set(_supported_content_types(frozenset()))
    assert baseline == set(ContentTypeEnum) - set(CONTENT_TYPE_REQUIRED_CAPABILITIES)


def test_supported_content_types_with_asr_includes_audio_but_not_video() -> None:
    """Video needs the video LLM on top of ASR, so ASR alone is not enough."""
    with_asr = set(_supported_content_types([ParserCapability.ASR]))
    assert ContentTypeEnum.AudioMpeg in with_asr
    assert ContentTypeEnum.AudioWav in with_asr
    assert ContentTypeEnum.VideoMp4 not in with_asr
    assert ContentTypeEnum.VideoXMsVideo not in with_asr


def test_supported_content_types_with_every_capability_includes_every_type() -> None:
    """A fully provisioned deployment accepts the whole taxonomy."""
    assert set(_supported_content_types(ALL_CAPABILITIES)) == set(ContentTypeEnum)


def test_resolve_supported_file_types_folds_aliases_into_canonical_type() -> None:
    """Aliases share their canonical type's entry and extensions rather than forming their own."""
    resolved = resolve_supported_file_types(_supported_content_types(ALL_CAPABILITIES))

    markdown = next(f for f in resolved if "text/markdown" in f.mime_types)
    assert markdown.mime_types[0] == "text/markdown"
    assert {"text/x-markdown", "application/markdown", "application/x-markdown"} <= set(markdown.mime_types)
    assert markdown.extensions == [".md"]

    yaml = next(f for f in resolved if "application/yaml" in f.mime_types)
    assert yaml.mime_types[0] == "application/yaml"
    assert {"application/x-yaml", "text/yaml", "text/x-yaml"} <= set(yaml.mime_types)
    assert yaml.extensions == [".yaml", ".yml"]

    jpeg = next(f for f in resolved if "image/jpeg" in f.mime_types)
    assert jpeg.mime_types[0] == "image/jpeg"
    assert "image/jpg" in jpeg.mime_types
    assert jpeg.extensions == [".jpeg", ".jpg"]


def test_resolve_supported_file_types_never_emits_an_alias_as_a_canonical_type() -> None:
    """Each entry leads with the canonical type, which is what upload validation keys on."""
    resolved = resolve_supported_file_types(_supported_content_types(ALL_CAPABILITIES))
    canonical_mime_types = {f.mime_types[0] for f in resolved}
    assert not (canonical_mime_types & {alias.value for alias in CONTENT_TYPE_ALIASES})


def test_resolve_supported_file_types_covers_every_non_alias_type_exactly_once() -> None:
    """No accepted type is dropped from, or duplicated across, the resolved entries."""
    resolved = resolve_supported_file_types(_supported_content_types(ALL_CAPABILITIES))
    canonical_mime_types = [f.mime_types[0] for f in resolved]

    assert len(canonical_mime_types) == len(set(canonical_mime_types))
    assert set(canonical_mime_types) == {ct.value for ct in ContentTypeEnum} - {
        alias.value for alias in CONTENT_TYPE_ALIASES
    }


def test_resolve_supported_file_types_omits_aliases_whose_canonical_type_is_unsupported() -> None:
    """An alias must not leak in on its own; audio/mp3 tracks audio/mpeg's ASR gate."""
    baseline = resolve_supported_file_types(_supported_content_types(frozenset()))
    assert "audio/mp3" not in {m for f in baseline for m in f.mime_types}


def test_resolve_supported_file_types_omits_unsupported_aliases_of_a_supported_canonical_type() -> None:
    """An entry lists only the aliases the caller actually accepts."""
    resolved = resolve_supported_file_types([ContentTypeEnum.ImageJpeg])
    assert [f.mime_types for f in resolved] == [["image/jpeg"]]


def test_resolve_supported_file_types_gives_extensionless_types_an_empty_extension_list() -> None:
    """MIME-only formats advertise no suffix rather than an empty-string suffix."""
    resolved = resolve_supported_file_types(_supported_content_types(ALL_CAPABILITIES))
    octet_stream = next(f for f in resolved if "application/octet-stream" in f.mime_types)
    assert octet_stream.extensions == []


def test_content_type_to_extensions_round_trips_every_authored_extension() -> None:
    """The derived reverse mapping cannot drift from the authored extension table."""
    for extension, content_type in EXTENSION_TO_CONTENT_TYPE.items():
        assert extension in CONTENT_TYPE_TO_EXTENSIONS[content_type]


def test_extension_table_never_maps_an_extension_to_an_alias() -> None:
    """Extensions resolve to canonical types, the direction file detection runs."""
    assert not (set(EXTENSION_TO_CONTENT_TYPE.values()) & set(CONTENT_TYPE_ALIASES))


def test_only_known_content_types_lack_a_file_extension() -> None:
    """A new content type must not silently end up with no extension; add one or list it here."""
    extensionless = {ct for ct, extensions in CONTENT_TYPE_TO_EXTENSIONS.items() if not extensions}
    assert extensionless - set(CONTENT_TYPE_ALIASES) == {
        ContentTypeEnum.ApplicationOctetStream,
        ContentTypeEnum.ApplicationVndCohereCompassV1Json,
    }


def test_baseline_supported_file_types_matches_the_capability_free_derivation() -> None:
    """The published constant is the derivation, not a hand-maintained literal."""
    assert BASELINE_SUPPORTED_FILE_TYPES == SupportedFileTypesResponse(
        file_types=resolve_supported_file_types(_supported_content_types(frozenset()))
    )


def test_baseline_supported_file_types_advertises_no_audio_or_video() -> None:
    """The SDK cannot detect ASR or the video LLM, so the baseline must never promise media."""
    assert not any(m.startswith(("audio/", "video/")) for m in BASELINE_SUPPORTED_FILE_TYPES.mime_types)
    assert not {".mp3", ".wav", ".mp4", ".avi"} & BASELINE_SUPPORTED_FILE_TYPES.extensions


def test_baseline_supported_file_types_supports_ungated_formats() -> None:
    """Formats needing no gated backend are safe to advertise on any deployment."""
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(filename="report.pdf") is True
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(filename="values.yml") is True
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(filename="budget.xlsm") is True
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(mime_type="application/x-yaml") is True


def test_baseline_supported_file_types_rejects_gated_and_unknown_formats() -> None:
    """Gated media and formats Compass never accepts both answer False."""
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(filename="interview.mp3") is False
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(mime_type="audio/mpeg") is False
    assert BASELINE_SUPPORTED_FILE_TYPES.supports(filename="archive.zip") is False


def test_baseline_supported_file_types_extensions_are_lowercase_and_dot_prefixed() -> None:
    """SupportedFileType documents this shape, and supports() relies on it when matching."""
    for extension in BASELINE_SUPPORTED_FILE_TYPES.extensions:
        assert extension == extension.lower()
        assert extension.startswith(".")


def test_baseline_supported_file_types_accepts_every_mime_type_it_advertises() -> None:
    """Anything the baseline lists must pass supports(); mixed-case types such as macroEnabled.12 included."""
    for mime_type in BASELINE_SUPPORTED_FILE_TYPES.mime_types:
        assert BASELINE_SUPPORTED_FILE_TYPES.supports(mime_type=mime_type) is True


def test_baseline_supported_file_types_accepts_every_extension_it_advertises() -> None:
    """Anything the baseline lists must pass supports(), so callers and the tables cannot disagree."""
    for extension in BASELINE_SUPPORTED_FILE_TYPES.extensions:
        assert BASELINE_SUPPORTED_FILE_TYPES.supports(filename=f"document{extension}") is True
