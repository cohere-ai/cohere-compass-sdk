# pyright: reportPrivateUsage=false
import asyncio
import os
import subprocess
import sys
import textwrap
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import fsspec  # type: ignore
import pytest

from cohere_compass.utils import fs as fs_module
from cohere_compass.utils.fs import GCS_EXTRA_HINT, GcsFilesystem, open_gcs_filesystem

pytest.importorskip("gcsfs")

from aiohttp import web


@pytest.mark.asyncio
async def test_open_gcs_filesystem_is_private_to_each_block() -> None:
    shared = fsspec.filesystem("gs", token="anon")  # type: ignore
    async with open_gcs_filesystem(token="anon") as first, open_gcs_filesystem(token="anon") as second:
        assert first._filesystem is not shared
        assert first._filesystem is not second._filesystem
        assert all(getattr(filesystem._filesystem, "asynchronous", False) for filesystem in (first, second))


@pytest.mark.asyncio
async def test_open_gcs_filesystem_closes_its_session_on_exit() -> None:
    async with open_gcs_filesystem(token="anon") as filesystem:
        session = await getattr(filesystem._filesystem, "_set_session")()
        assert not session.closed
    assert session.closed


@pytest.mark.asyncio
async def test_open_gcs_filesystem_closes_resources_before_the_session() -> None:
    closed: list[str] = []

    class Session:
        closed = False

        async def close(self) -> None:
            closed.append("session")

    class Filesystem:
        _session = Session()

        async def _close_resources(self) -> None:
            closed.append("resources")

    await fs_module._close_connections(Filesystem())
    assert closed == ["resources", "session"]


@pytest.mark.asyncio
async def test_open_gcs_filesystem_names_the_extra_when_gcsfs_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(*args: object, **kwargs: object) -> None:
        raise ImportError("Install gcsfs to access Google Storage")

    monkeypatch.setattr(fs_module.fsspec, "filesystem", missing)
    with pytest.raises(ImportError, match=r"cohere-compass-sdk\[uploads\]"):
        async with open_gcs_filesystem():
            pass
    assert "uploads" in GCS_EXTRA_HINT


# A stand-in for the GCS JSON API, serving the objects below, so the regression
# test can make real gcsfs requests without network access or credentials.
OBJECTS = {"bucket/prefix/a.pdf": b"alpha", "bucket/prefix/nested/b.pdf": b"beta"}


def _metadata(name: str) -> dict[str, Any]:
    bucket, key = name.split("/", 1)
    return {"kind": "storage#object", "bucket": bucket, "name": key, "size": str(len(OBJECTS[name])), "generation": "1"}


async def _handle(request: web.Request) -> web.StreamResponse:
    _, _, rest = request.path.partition("/b/")
    bucket, _, key = rest.partition("/o")
    key = key.removeprefix("/")
    name = f"{bucket}/{key}"
    if request.query.get("alt") == "media" and name in OBJECTS:
        return web.Response(body=OBJECTS[name])
    if key and name in OBJECTS:
        return web.json_response(_metadata(name))
    if not key:
        prefix = f"{bucket}/{request.query.get('prefix', '')}"
        names = [object_name for object_name in OBJECTS if object_name.startswith(prefix)]
        if request.query.get("delimiter") == "/":
            prefixes = sorted({n[: n.index("/", len(prefix)) + 1] for n in names if "/" in n[len(prefix) :]})
            items = [n for n in names if "/" not in n[len(prefix) :]]
            return web.json_response(
                {
                    "items": [_metadata(n) for n in items],
                    "prefixes": [p.split("/", 1)[1] for p in prefixes],
                }
            )
        return web.json_response({"items": [_metadata(n) for n in names]})
    return web.json_response({"error": {"code": 404, "message": "Not Found"}}, status=404)


@pytest.fixture
def gcs_emulator() -> Iterator[str]:
    loop = asyncio.new_event_loop()
    started = threading.Event()
    address: list[str] = []
    runner = web.AppRunner(web.Application())

    async def start() -> None:
        runner.app.router.add_route("*", "/{tail:.*}", _handle)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        address.append(f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}")  # type: ignore[union-attr]
        started.set()

    thread = threading.Thread(target=lambda: (loop.run_until_complete(start()), loop.run_forever()), daemon=True)
    thread.start()
    started.wait(timeout=10)
    try:
        yield address[0]
    finally:
        asyncio.run_coroutine_threadsafe(runner.cleanup(), loop).result(timeout=10)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
        loop.close()


# Each ordering runs in a fresh interpreter, so that fsspec's process-wide
# instance cache starts empty and gcsfs reads the environment set below when it
# is first imported.
SCRIPT = textwrap.dedent(
    """
    import asyncio
    import sys

    import fsspec

    from cohere_compass.ext.uploads import SourceType, UploadOptions, discover_files, read_source_bytes

    OPTIONS = UploadOptions(source_uri="gs://bucket/prefix", source_type=SourceType.GCS_DIRECTORY, index_name="i")


    def sync_listing():
        # Ordinary synchronous fsspec use of gs://, as pandas and other libraries do.
        return sorted(fsspec.filesystem("gs").find("bucket/prefix"))


    async def sdk_round():
        files = await discover_files(OPTIONS)
        data = [await read_source_bytes(source) for source in files]
        in_loop = sync_listing()
        in_thread = await asyncio.to_thread(sync_listing)
        return [source.relative_path for source in files], data, in_loop == in_thread


    if sys.argv[1] == "sync-first":
        print(sync_listing())
    for _ in range(2):
        print(asyncio.run(sdk_round()))
    print(sync_listing())
    """
)


@pytest.mark.parametrize("order", ["sdk-first", "sync-first"])
def test_uploads_and_synchronous_fsspec_share_a_process(gcs_emulator: str, order: str, tmp_path: Path) -> None:
    env = {
        **{key: value for key, value in os.environ.items() if key != "GOOGLE_APPLICATION_CREDENTIALS"},
        "STORAGE_EMULATOR_HOST": gcs_emulator,
        # Hide any Application Default Credentials and skip probing the GCE
        # metadata server, so that gcsfs falls back to anonymous access.
        "CLOUDSDK_CONFIG": str(tmp_path),
        "NO_GCE_CHECK": "true",
        # Use gcsfs's HTTP-only filesystem. The default one also looks up each
        # bucket's type over gRPC, which the HTTP stand-in above cannot serve.
        "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT": "false",
    }
    result = subprocess.run(
        [sys.executable, "-W", "error::ResourceWarning", "-c", SCRIPT, order],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    listing = "['bucket/prefix/a.pdf', 'bucket/prefix/nested/b.pdf']"
    sdk_round = "(['a.pdf', 'nested/b.pdf'], [b'alpha', b'beta'], True)"
    expected = ([listing] if order == "sync-first" else []) + [sdk_round, sdk_round, listing]
    assert result.stdout.splitlines() == expected


@pytest.mark.asyncio
async def test_gcs_filesystem_forwards_to_the_detail_coroutines() -> None:
    calls: list[tuple[str, str, bool | None]] = []

    class Fsspec:
        async def _info(self, path: str) -> dict[str, object]:
            calls.append(("info", path, None))
            return {"name": path}

        async def _isdir(self, path: str) -> bool:
            calls.append(("isdir", path, None))
            return True

        async def _find(self, path: str, *, detail: bool) -> dict[str, dict[str, object]]:
            calls.append(("find", path, detail))
            return {}

        async def _ls(self, path: str, *, detail: bool) -> list[dict[str, object]]:
            calls.append(("ls", path, detail))
            return []

        async def _cat_file(self, path: str) -> bytes:
            calls.append(("cat_file", path, None))
            return b"data"

    filesystem = GcsFilesystem(Fsspec())
    assert await filesystem.info("b/a") == {"name": "b/a"}
    assert await filesystem.is_dir("b")
    assert await filesystem.find("b") == {}
    assert await filesystem.ls("b") == []
    assert await filesystem.read_bytes("b/a") == b"data"
    assert calls == [
        ("info", "b/a", None),
        ("isdir", "b", None),
        ("find", "b", True),
        ("ls", "b", True),
        ("cat_file", "b/a", None),
    ]
