"""
Discover and read credential-free upload sources.

Only local paths and ``gs://`` URIs are supported; GCS access relies on
Application Default Credentials and needs the `gcsfs` package, which the
`uploads` extra installs.

GCS is reached through a private `GcsFilesystem` from
`cohere_compass.utils.fs.open_gcs_filesystem`, never through fsspec's shared
cached instance, whose session belongs to fsspec's own event loop; that
module's docstring explains the failure the private filesystem prevents.
Its methods are coroutines awaited directly on the caller's loop, so no call
needs a thread.

`discover_files` and `read_source_bytes` accept that filesystem as
*filesystem*, so a caller that makes many calls, such as `run_upload_job`,
shares one session and its connections. Without it, each call opens and
closes a filesystem of its own.
"""

import asyncio
import fnmatch
import mimetypes
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from typing import cast
from urllib.parse import urlsplit

from cohere_compass.ext.uploads import _fs as local_fs
from cohere_compass.ext.uploads.exceptions import SourceError
from cohere_compass.ext.uploads.models import (
    SourceFile,
    SourceType,
    UploadOptions,
)
from cohere_compass.models import ContentTypeEnum
from cohere_compass.utils.fs import GcsFilesystem, open_gcs_filesystem


@asynccontextmanager
async def _gcs(filesystem: GcsFilesystem | None) -> AsyncIterator[GcsFilesystem]:
    """Yield *filesystem*, or a private one opened and closed around the block when it is `None`."""
    if filesystem is not None:
        yield filesystem
        return
    async with open_gcs_filesystem() as opened:
        yield opened


def is_gcs_source(source_uri: str) -> bool:
    """
    Return whether `source_uri` names a ``gs://`` object, by scheme alone.

    Unlike `_normalize_source`, this never raises: it is the classification
    used before a source is known to be well-formed, for example to pick a
    `SourceType` for a user-supplied path.
    """
    return urlsplit(source_uri).scheme == "gs"


def source_type_for(source_uri: str, *, directory: bool) -> SourceType:
    """Return the `SourceType` of *source_uri*, a directory if *directory* is true."""
    if is_gcs_source(source_uri):
        return SourceType.GCS_DIRECTORY if directory else SourceType.GCS_FILE
    return SourceType.LOCAL_DIRECTORY if directory else SourceType.LOCAL_FILE


def _normalize_source(source_uri: str) -> tuple[bool, str, str]:
    """
    Classify and validate a source URI.

    Returns ``(is_gcs, normalized_uri, path)``: for a ``gs://`` URI, `path`
    is the bucket-and-key with no scheme; for a local path, both
    `normalized_uri` and `path` are the expanded, resolved absolute path.
    Raises `SourceError` for anything else.
    """
    parsed = urlsplit(source_uri)
    if is_gcs_source(source_uri):
        if not parsed.netloc or parsed.query or parsed.fragment:
            raise SourceError("GCS source must be a plain gs://bucket/path URI.")
        path = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
        if not path:
            raise SourceError("GCS source must include a bucket.")
        return True, f"gs://{path}", path
    if parsed.scheme:
        raise SourceError(f"Unsupported source scheme: {parsed.scheme!r}.")
    path = Path(source_uri).expanduser().resolve()
    return False, str(path), str(path)


def _matches(relative_path: str, patterns: tuple[str, ...]) -> bool:
    """
    Match POSIX relative paths; basename patterns match at every depth.

    Patterns containing a slash match relative paths.  ``**`` matches directory
    descendants, including the conventional ``archive/**`` prefix form.
    """
    path = PurePosixPath(relative_path)
    for pattern in patterns:
        if "/" not in pattern:
            if fnmatch.fnmatchcase(path.name, pattern):
                return True
        elif pattern.endswith("/**"):
            if relative_path.startswith(pattern[:-2]):
                return True
        elif path.match(pattern) or fnmatch.fnmatchcase(relative_path, pattern):
            return True
    return False


def _included(relative_path: str, options: UploadOptions) -> bool:
    return (not options.include_patterns or _matches(relative_path, options.include_patterns)) and not _matches(
        relative_path, options.exclude_patterns
    )


def _source_file(uri: str, relative_path: str, info: Mapping[str, object]) -> SourceFile:
    raw_size = info.get("size")
    size = raw_size if isinstance(raw_size, int) and not isinstance(raw_size, bool) else None
    return SourceFile(uri=uri, relative_path=relative_path, size=size)


def _local_info(path: Path) -> Mapping[str, object]:
    stat = path.stat()
    return {"size": stat.st_size}


def _check_kind(actual_file: bool, options: UploadOptions) -> None:
    if actual_file != options.source_type.is_single_file:
        raise SourceError(f"Source kind does not match {options.source_type.value}.")


def _sort_key(source: SourceFile) -> tuple[str, str]:
    return source.uri, source.relative_path


def _discover_local(path: Path, options: UploadOptions) -> list[SourceFile]:
    if not path.exists():
        raise SourceError(f"Source does not exist: {path}.")
    actual_file = path.is_file()
    _check_kind(actual_file, options)
    candidates = [path] if actual_file else (list(path.rglob("*")) if options.recursive else list(path.iterdir()))
    files = [candidate for candidate in candidates if candidate.is_file()]
    result: list[SourceFile] = []
    for candidate in files:
        relative = candidate.name if actual_file else candidate.relative_to(path).as_posix()
        if _included(relative, options):
            result.append(_source_file(str(candidate), relative, _local_info(candidate)))
    return sorted(result, key=_sort_key)


def _remote_uri(path: str) -> str:
    return f"gs://{path}"


def _is_file(info: Mapping[str, object]) -> bool:
    return info.get("type") == "file"


async def _discover_gcs(path: str, options: UploadOptions, filesystem: GcsFilesystem | None) -> list[SourceFile]:
    try:
        async with _gcs(filesystem) as fs:
            try:
                info: Mapping[str, object] = await fs.info(path)
            except (FileNotFoundError, OSError):
                info = {}
            actual_file = _is_file(info)
            actual_directory = not actual_file and await fs.is_dir(path)
            if not actual_file and not actual_directory:
                raise SourceError(f"Source does not exist: {_remote_uri(path)}.")
            _check_kind(actual_file, options)
            if actual_file:
                candidates: Mapping[str, Mapping[str, object]] = {path: info}
            elif options.recursive:
                candidates = await fs.find(path)
            else:
                listed = await fs.ls(path)
                candidates = {cast(str, item["name"]): item for item in listed if isinstance(item.get("name"), str)}
    except SourceError:
        raise
    except (OSError, ValueError, KeyError):
        raise SourceError(f"Unable to discover source: {_remote_uri(path)}.") from None

    result: list[SourceFile] = []
    for candidate, candidate_info in candidates.items():
        if not _is_file(candidate_info):
            continue
        relative = candidate.rsplit("/", 1)[-1] if actual_file else candidate[len(path + "/") :]
        if _included(relative, options):
            result.append(_source_file(_remote_uri(candidate), relative, candidate_info))
    return sorted(result, key=_sort_key)


async def discover_files(options: UploadOptions, *, filesystem: GcsFilesystem | None = None) -> list[SourceFile]:
    """
    Discover regular files for an upload without reading their contents.

    Include and exclude patterns use POSIX relative paths. Basename patterns
    (such as ``*.pdf``) apply at every depth; exclusion patterns take priority.

    *filesystem* lists a ``gs://`` source; it must come from
    `cohere_compass.utils.fs.open_gcs_filesystem`, opened on the running loop.
    Without one, a private filesystem is opened for this call. Local sources
    ignore it.
    """
    is_gcs, normalized, path = _normalize_source(options.source_uri)
    expected_gcs = options.source_type in {
        SourceType.GCS_FILE,
        SourceType.GCS_DIRECTORY,
    }
    if is_gcs != expected_gcs:
        raise SourceError(f"Source URI does not match {options.source_type.value}.")
    if is_gcs:
        return await _discover_gcs(path, options, filesystem)
    # One offload for the whole local walk: a recursive directory is thousands
    # of `stat` calls, and awaiting each one individually would be slower than
    # a single hop as well as noisier.
    return await asyncio.to_thread(_discover_local, Path(normalized), options)


async def read_source_bytes(source: SourceFile, *, filesystem: GcsFilesystem | None = None) -> bytes:
    """
    Read a previously discovered source, without exposing backend details.

    *filesystem* reads a ``gs://`` source, as for `discover_files`. Without
    one, a private filesystem is opened for this call, which costs a new
    connection to GCS, so pass one when reading many files.
    """
    is_gcs, normalized, path = _normalize_source(source.uri)
    try:
        if not is_gcs:
            return await local_fs.read_bytes(Path(normalized))
        async with _gcs(filesystem) as fs:
            return await fs.read_bytes(path)
    except (OSError, ValueError):
        raise SourceError(f"Unable to read source: {source.uri}.") from None


def content_type_for(path: str) -> ContentTypeEnum | None:
    """Guess a source's upload content type from its filename; `None` if unknown."""
    mime, _ = mimetypes.guess_type(Path(path).name)
    try:
        return ContentTypeEnum(mime) if mime is not None else None
    except ValueError:
        return None
