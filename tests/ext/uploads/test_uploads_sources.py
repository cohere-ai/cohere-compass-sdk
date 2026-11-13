from pathlib import Path
from typing import ClassVar

import pytest

from cohere_compass.ext.uploads import sources
from cohere_compass.ext.uploads.exceptions import SourceError
from cohere_compass.ext.uploads.models import (
    SourceFile,
    SourceType,
    UploadOptions,
)
from cohere_compass.ext.uploads.sources import (
    content_type_for,
    discover_files,
    is_gcs_source,
    read_source_bytes,
    source_type_for,
)


def _options(
    source: str,
    source_type: SourceType,
    *,
    recursive: bool = True,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
) -> UploadOptions:
    return UploadOptions(
        source_uri=source,
        source_type=source_type,
        index_name="index",
        workers=1,
        recursive=recursive,
        include_patterns=include,
        exclude_patterns=exclude,
        timeout_seconds=1,
        poll_interval_seconds=1,
        wait=True,
        force=False,
    )


@pytest.mark.asyncio
async def test_local_file_discovery_and_read(tmp_path: Path) -> None:
    file = tmp_path / "one.pdf"
    file.write_bytes(b"one")
    source = (await discover_files(_options(str(file), SourceType.LOCAL_FILE)))[0]
    assert (source.uri, source.relative_path, source.size) == (str(file), "one.pdf", 3)
    assert await read_source_bytes(source) == b"one"


@pytest.mark.asyncio
async def test_local_directory_recursion_patterns_sorting_and_irregular_entries(
    tmp_path: Path,
) -> None:
    (tmp_path / "archive").mkdir()
    (tmp_path / "nested").mkdir()
    (tmp_path / "z.pdf").write_text("z")
    (tmp_path / "nested" / "a.pdf").write_text("a")
    (tmp_path / "archive" / "old.pdf").write_text("old")
    (tmp_path / ".hidden.pdf").write_text("hidden")
    found = await discover_files(
        _options(
            str(tmp_path),
            SourceType.LOCAL_DIRECTORY,
            include=("*.pdf",),
            exclude=("archive/**",),
        )
    )
    assert [item.relative_path for item in found] == [
        ".hidden.pdf",
        "nested/a.pdf",
        "z.pdf",
    ]
    found = await discover_files(_options(str(tmp_path), SourceType.LOCAL_DIRECTORY, recursive=False))
    assert [item.relative_path for item in found] == [".hidden.pdf", "z.pdf"]


@pytest.mark.asyncio
async def test_invalid_local_sources(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="does not exist"):
        await discover_files(_options(str(tmp_path / "missing"), SourceType.LOCAL_FILE))
    with pytest.raises(SourceError, match="kind"):
        await discover_files(_options(str(tmp_path), SourceType.LOCAL_FILE))
    with pytest.raises(SourceError, match="Unsupported"):
        await discover_files(_options("s3://bucket/a", SourceType.GCS_FILE))


class _FakeGcs:
    """Stands in for `gcsfs.GCSFileSystem`'s *async* surface.

    Production reaches GCS through the coroutine twins (`_info`, `_isdir`,
    `_find`, `_ls`, `_cat_file`) rather than the generated sync wrappers, so the
    fake mirrors those names. `_cat_file` replaces the old `open()` + `read()`
    pair, which is why the read fake no longer returns a file object.
    """

    files: ClassVar[dict[str, bytes]] = {
        "bucket/a.pdf": b"a",
        "bucket/prefix/z.pdf": b"z",
        "bucket/prefix/nested/a.pdf": b"nested",
    }

    async def _info(self, path: str) -> dict[str, object]:
        if path in self.files:
            return {
                "name": path,
                "type": "file",
                "size": len(self.files[path]),
                "generation": "1",
            }
        if await self._isdir(path):
            return {"name": path, "type": "directory"}
        raise FileNotFoundError(path)

    async def _isdir(self, path: str) -> bool:
        return any(name.startswith(path.rstrip("/") + "/") for name in self.files)

    async def _find(self, path: str, *, detail: bool) -> dict[str, dict[str, object]]:
        return {name: await self._info(name) for name in self.files if name.startswith(path.rstrip("/") + "/")}

    async def _ls(self, path: str, *, detail: bool) -> list[dict[str, object]]:
        prefix = path.rstrip("/") + "/"
        return [
            await self._info(name) for name in self.files if name.startswith(prefix) and "/" not in name[len(prefix) :]
        ]

    async def _cat_file(self, path: str) -> bytes:
        return self.files[path]


def _fake_url_to_fs(uri: str, fake: _FakeGcs) -> tuple[_FakeGcs, str]:
    return fake, uri.removeprefix("gs://")


@pytest.mark.asyncio
async def test_mocked_gcs_discovery_and_read(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeGcs()

    def fake_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        return _fake_url_to_fs(uri, fake)

    monkeypatch.setattr(
        "cohere_compass.ext.uploads.sources.url_to_fs",
        fake_url_to_fs,
    )
    found = await discover_files(_options("gs://bucket/prefix/", SourceType.GCS_DIRECTORY))
    assert [item.uri for item in found] == [
        "gs://bucket/prefix/nested/a.pdf",
        "gs://bucket/prefix/z.pdf",
    ]
    assert [item.relative_path for item in found] == ["nested/a.pdf", "z.pdf"]
    assert await read_source_bytes(found[0]) == b"nested"
    with pytest.raises(SourceError, match="kind"):
        await discover_files(_options("gs://bucket/a.pdf", SourceType.GCS_DIRECTORY))


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("gs://", "bucket"),
        ("gs://bucket/a?query=yes", "plain"),
        ("https://bucket/a", "Unsupported"),
    ],
)
def test_source_normalization_rejects_malformed_remote_uris(uri: str, message: str) -> None:
    with pytest.raises(SourceError, match=message):
        vars(sources)["_normalize_source"](uri)


def test_pattern_matching_and_source_metadata_variants() -> None:
    matches = vars(sources)["_matches"]
    included = vars(sources)["_included"]
    source_file = vars(sources)["_source_file"]
    assert matches("nested/a.pdf", ("*.pdf",))
    assert matches("nested/a.pdf", ("nested/*.pdf",))
    assert matches("nested/deep/a.pdf", ("nested/*.pdf",))
    assert not matches("nested/a.pdf", ("*.txt",))
    assert matches("nested/a.pdf", ("other/*.pdf", "*.pdf"))
    options = _options("/tmp", SourceType.LOCAL_DIRECTORY, include=("*.pdf",))
    assert included("a.pdf", options)
    assert not included("a.txt", options)
    assert source_file("gs://b/a", "a", {"etag": "x"}).uri == "gs://b/a"
    assert source_file("/a", "a", {"size": True}).size is None


@pytest.mark.asyncio
async def test_gcs_discovery_errors_and_nonrecursive_unknown_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenGcs(_FakeGcs):
        async def _ls(self, path: str, *, detail: bool) -> list[dict[str, object]]:
            return [{"name": 1}, {"name": "bucket/prefix/z.pdf", "type": "file"}]

    fake = BrokenGcs()

    def fake_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        return _fake_url_to_fs(uri, fake)

    monkeypatch.setattr(sources, "url_to_fs", fake_url_to_fs)
    found = await discover_files(_options("gs://bucket/prefix", SourceType.GCS_DIRECTORY, recursive=False))
    assert [item.relative_path for item in found] == ["z.pdf"]

    def broken_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        del uri
        raise OSError("adc")

    monkeypatch.setattr(sources, "url_to_fs", broken_url_to_fs)
    with pytest.raises(SourceError, match="Unable to discover"):
        await discover_files(_options("gs://bucket/prefix", SourceType.GCS_DIRECTORY))


@pytest.mark.asyncio
async def test_read_source_wraps_local_and_gcs_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(SourceError, match="Unable to read"):
        await read_source_bytes(SourceFile(uri=str(tmp_path / "missing"), relative_path="missing"))

    class BrokenGcs(_FakeGcs):
        async def _cat_file(self, path: str) -> bytes:
            del path
            raise OSError("no read")

    fake = BrokenGcs()

    def fake_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        return _fake_url_to_fs(uri, fake)

    monkeypatch.setattr(sources, "url_to_fs", fake_url_to_fs)
    with pytest.raises(SourceError, match="Unable to read"):
        await read_source_bytes(SourceFile(uri="gs://bucket/a.pdf", relative_path="a.pdf"))


@pytest.mark.asyncio
async def test_gcs_missing_single_file_and_scheme_type_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeGcs()

    def fake_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        return _fake_url_to_fs(uri, fake)

    monkeypatch.setattr(sources, "url_to_fs", fake_url_to_fs)
    single = await discover_files(_options("gs://bucket/a.pdf", SourceType.GCS_FILE))
    assert single[0].relative_path == "a.pdf"
    with pytest.raises(SourceError, match="does not exist"):
        await discover_files(_options("gs://bucket/missing", SourceType.GCS_FILE))
    with pytest.raises(SourceError, match="does not match"):
        await discover_files(_options("gs://bucket/a.pdf", SourceType.LOCAL_FILE))
    with pytest.raises(SourceError, match="does not match"):
        await discover_files(_options("/tmp", SourceType.GCS_DIRECTORY))


@pytest.mark.asyncio
async def test_fsspec_wrapper_and_gcs_unknown_entries_and_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeGcs()

    def fake_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        return _fake_url_to_fs(uri, fake)

    monkeypatch.setattr(sources, "_url_to_fs", fake_url_to_fs)
    assert sources.url_to_fs("gs://bucket/a.pdf")[1] == "bucket/a.pdf"
    monkeypatch.setattr(sources, "url_to_fs", fake_url_to_fs)
    found = await discover_files(_options("gs://bucket/prefix", SourceType.GCS_DIRECTORY, include=("*.txt",)))
    assert found == []


@pytest.mark.asyncio
async def test_gcs_discovery_skips_non_file_find_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DirectoryEntryGcs(_FakeGcs):
        async def _find(self, path: str, *, detail: bool) -> dict[str, dict[str, object]]:
            result = await super()._find(path, detail=detail)
            result["bucket/prefix/empty"] = {"type": "directory"}
            return result

    fake = DirectoryEntryGcs()

    def fake_url_to_fs(uri: str) -> tuple[_FakeGcs, str]:
        return _fake_url_to_fs(uri, fake)

    monkeypatch.setattr(sources, "url_to_fs", fake_url_to_fs)
    assert len(await discover_files(_options("gs://bucket/prefix", SourceType.GCS_DIRECTORY))) == 2


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("gs://bucket/a.pdf", True),
        ("/tmp/report.pdf", False),
        ("report.pdf", False),
        ("https://bucket/a.pdf", False),
    ],
)
def test_is_gcs_source_classifies_by_scheme_only(uri: str, expected: bool) -> None:
    assert is_gcs_source(uri) is expected


def test_content_type_fallback_is_handled(monkeypatch: pytest.MonkeyPatch) -> None:
    assert content_type_for("unknown.extension") is None

    def custom_type(_: str, strict: bool = True) -> tuple[str, None]:
        del strict
        return "application/x-custom", None

    monkeypatch.setattr(sources.mimetypes, "guess_type", custom_type)
    assert content_type_for("custom.file") is None


@pytest.mark.parametrize(
    ("source", "directory", "expected"),
    [
        ("report.pdf", False, SourceType.LOCAL_FILE),
        ("reports", True, SourceType.LOCAL_DIRECTORY),
        ("gs://bucket/report.pdf", False, SourceType.GCS_FILE),
        ("gs://bucket/reports", True, SourceType.GCS_DIRECTORY),
    ],
)
def test_source_type_for(source: str, directory: bool, expected: SourceType) -> None:
    assert source_type_for(source, directory=directory) is expected
