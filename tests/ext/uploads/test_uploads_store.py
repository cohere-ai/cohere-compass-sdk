import asyncio
import json
import os
import sqlite3
import stat
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from cohere_compass.ext.uploads import _fs
from cohere_compass.ext.uploads.models import (
    FileRecord,
    FileStatus,
    SourceFile,
    SourceType,
    UploadJob,
    UploadOptions,
)
from cohere_compass.ext.uploads.store import (
    FileUploadJobStore,
    InMemoryUploadJobStore,
    SqliteUploadJobStore,
    UploadJobStore,
    UploadStateError,
    _upgraded_state_json,  # pyright: ignore[reportPrivateUsage]
)


def _job(job_id: str = "job-1", *, updated_at: datetime | None = None, files: int = 1) -> UploadJob:
    now = datetime.now(UTC)
    return UploadJob(
        job_id=job_id,
        created_at=now,
        updated_at=updated_at or now,
        api_url="https://api.example.test",
        metadata={"profile_name": "work"},
        options=UploadOptions(
            source_uri="/tmp/docs",
            source_type=SourceType.LOCAL_DIRECTORY,
            index_name="docs",
        ),
        files={
            f"/tmp/docs/{number}.pdf": FileRecord(
                source=SourceFile(uri=f"/tmp/docs/{number}.pdf", relative_path=f"{number}.pdf")
            )
            for number in range(files)
        },
    )


@pytest.fixture(params=["file", "sqlite", "memory"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[UploadJobStore]:
    if request.param == "file":
        yield FileUploadJobStore(tmp_path / "uploads")
    elif request.param == "sqlite":
        sqlite = SqliteUploadJobStore(tmp_path / "state" / "uploads.db")
        yield sqlite
        sqlite.close()
    else:
        yield InMemoryUploadJobStore()


@pytest.mark.asyncio
async def test_create_load_round_trip(store: UploadJobStore) -> None:
    job = _job()
    await store.create(job)

    assert await store.load(job.job_id) == job


@pytest.mark.asyncio
async def test_create_refuses_an_existing_job(store: UploadJobStore) -> None:
    await store.create(_job())

    with pytest.raises(UploadStateError, match="already exists"):
        await store.create(_job())


@pytest.mark.asyncio
async def test_generated_ids_are_unused(store: UploadJobStore) -> None:
    first = await store.generate_job_id()
    await store.create(_job(first))

    second = await store.generate_job_id()

    assert first != second and len(second) == 12


@pytest.mark.asyncio
async def test_update_file_changes_only_that_file(store: UploadJobStore) -> None:
    job = _job(files=2)
    await store.create(job)
    first, second = job.files
    record = job.files[first].model_copy(update={"status": FileStatus.COMPLETED, "document_id": "doc"})

    await store.update_file(job.job_id, first, record)

    saved = await store.load(job.job_id)
    assert saved.files[first].status is FileStatus.COMPLETED
    assert saved.files[first].document_id == "doc"
    assert saved.files[second] == job.files[second]
    assert saved.updated_at >= job.updated_at


@pytest.mark.asyncio
async def test_loaded_jobs_are_copies(store: UploadJobStore) -> None:
    job = _job()
    await store.create(job)
    loaded = await store.load(job.job_id)
    key = next(iter(loaded.files))

    loaded.files[key].status = FileStatus.COMPLETED

    assert (await store.load(job.job_id)).files[key].status is FileStatus.PENDING


@pytest.mark.asyncio
async def test_list_order_delete_and_not_found(store: UploadJobStore) -> None:
    assert await store.list_jobs() == []
    now = datetime.now(UTC)
    await store.create(_job("old", updated_at=now - timedelta(hours=1)))
    await store.create(_job("new", updated_at=now))

    assert [job.job_id for job in await store.list_jobs()] == ["new", "old"]

    await store.delete("old")
    assert [job.job_id for job in await store.list_jobs()] == ["new"]
    for operation in (store.load("old"), store.delete("old")):
        with pytest.raises(UploadStateError, match="not found"):
            await operation
    with pytest.raises(UploadStateError, match="not found"):
        await store.update_file("old", "key", next(iter(_job().files.values())))


@pytest.mark.asyncio
async def test_concurrent_file_updates_are_all_kept(store: UploadJobStore) -> None:
    job = _job(files=20)
    await store.create(job)

    await asyncio.gather(
        *(
            store.update_file(job.job_id, key, record.model_copy(update={"status": FileStatus.COMPLETED}))
            for key, record in job.files.items()
        )
    )

    saved = await store.load(job.job_id)
    assert {record.status for record in saved.files.values()} == {FileStatus.COMPLETED}


def test_memory_store_has_no_location() -> None:
    assert InMemoryUploadJobStore().location("job-1") is None


@pytest.mark.asyncio
async def test_file_store_writes_private_files_in_the_given_directory(tmp_path: Path) -> None:
    directory = tmp_path / "nested" / "uploads"
    store = FileUploadJobStore(directory)
    job = _job()

    await store.create(job)

    path = store.path_for(job.job_id)
    assert store.location(job.job_id) == str(path) == str(directory / "job-1.json")
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert json.loads(path.read_text())["job_id"] == "job-1"


@pytest.mark.asyncio
async def test_file_store_lists_nothing_before_the_directory_exists(tmp_path: Path) -> None:
    assert await FileUploadJobStore(tmp_path / "missing").list_jobs() == []


@pytest.mark.parametrize("job_id", ["", ".", "..", "../escape", "a/b"])
def test_file_store_rejects_ids_that_are_not_file_names(tmp_path: Path, job_id: str) -> None:
    with pytest.raises(UploadStateError, match="file name"):
        FileUploadJobStore(tmp_path).path_for(job_id)


@pytest.mark.asyncio
async def test_file_store_reports_corrupt_or_mismatched_files(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path)
    store.path_for("broken").write_text("{not json")
    store.path_for("renamed").write_text(_job("other").model_dump_json())

    for job_id in ("broken", "renamed"):
        with pytest.raises(UploadStateError, match="corrupt or unreadable"):
            await store.load(job_id)


@pytest.mark.asyncio
async def test_file_store_loads_state_saved_without_parser_config(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path)
    serialized = _job().model_dump(mode="json")
    cast(dict[str, object], serialized["options"]).pop("parser_config")
    store.path_for("job-1").write_text(json.dumps(serialized))

    restored = await store.load("job-1")

    assert restored.options.parser_config.model_dump(mode="json")


def test_parser_config_upgrade_leaves_other_shapes_alone() -> None:
    assert json.loads(_upgraded_state_json([1])) == [1]
    assert json.loads(_upgraded_state_json({"options": None})) == {"options": None}


@pytest.mark.asyncio
async def test_file_store_reports_directory_and_write_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("")
    with pytest.raises(UploadStateError, match="Could not create upload state directory"):
        await FileUploadJobStore(blocker / "uploads").create(_job())

    store = FileUploadJobStore(tmp_path / "uploads")

    async def fail_write(path: Path, data: bytes) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(_fs, "atomic_write_bytes", fail_write)
    with pytest.raises(UploadStateError, match="disk full"):
        await store.create(_job())


@pytest.mark.asyncio
async def test_file_store_reports_delete_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FileUploadJobStore(tmp_path)
    await store.create(_job())

    async def fail_remove(path: Path) -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr(_fs, "remove", fail_remove)
    with pytest.raises(UploadStateError, match="Could not delete"):
        await store.delete("job-1")


def test_atomic_write_removes_its_temporary_file_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_replace(source: str, target: Path) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        _fs._atomic_write_bytes(tmp_path / "job.json", b"{}", 0o600)  # pyright: ignore[reportPrivateUsage]

    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_update_file_adds_a_file_the_job_did_not_have(store: UploadJobStore) -> None:
    job = _job(files=1)
    await store.create(job)
    record = FileRecord(source=SourceFile(uri="/tmp/docs/new.pdf", relative_path="new.pdf"))

    await store.update_file(job.job_id, record.source.uri, record)

    assert list((await store.load(job.job_id)).files) == [*job.files, record.source.uri]


@pytest.mark.asyncio
async def test_metadata_and_api_url_round_trip(store: UploadJobStore) -> None:
    job = _job().model_copy(update={"metadata": {"profile_name": "work", "run": "7"}, "api_url": None})
    await store.create(job)

    loaded = await store.load(job.job_id)

    assert loaded.metadata == {"profile_name": "work", "run": "7"} and loaded.api_url is None


@pytest.mark.asyncio
async def test_file_store_moves_a_legacy_profile_name_into_metadata(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path)
    serialized = _job().model_dump(mode="json")
    serialized.pop("metadata")
    serialized["profile_name"] = "work"
    store.path_for("job-1").write_text(json.dumps(serialized))
    serialized["job_id"] = "job-2"
    serialized["profile_name"] = None
    store.path_for("job-2").write_text(json.dumps(serialized))

    assert (await store.load("job-1")).metadata == {"profile_name": "work"}
    assert (await store.load("job-2")).metadata == {}


@pytest.mark.asyncio
async def test_sqlite_store_creates_a_private_database_and_survives_reopening(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "uploads.db"
    store = SqliteUploadJobStore(path)
    job = _job(files=3)
    await store.create(job)
    store.close()

    reopened = SqliteUploadJobStore(path)
    loaded = await reopened.load(job.job_id)
    reopened.close()

    assert loaded == job
    assert reopened.location(job.job_id) == str(path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["file", "sqlite"])
async def test_stores_leave_the_mode_of_an_existing_directory_alone(tmp_path: Path, kind: str) -> None:
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o755)
    store = FileUploadJobStore(shared) if kind == "file" else SqliteUploadJobStore(shared / "uploads.db")

    async with store:
        await store.create(_job())

    assert stat.S_IMODE(shared.stat().st_mode) == 0o755


@pytest.mark.asyncio
async def test_stores_close_when_used_as_async_context_managers(store: UploadJobStore) -> None:
    async with store as entered:
        await entered.create(_job())

    assert entered is store
    if isinstance(store, SqliteUploadJobStore):
        assert store._connection is None  # pyright: ignore[reportPrivateUsage]
        # A closed SQLite store reopens its database when used again.
        assert (await store.load("job-1")).job_id == "job-1"


def _user_version(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return cast(int, connection.execute("PRAGMA user_version").fetchone()[0])
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_sqlite_store_records_and_checks_its_table_layout_version(tmp_path: Path) -> None:
    path = tmp_path / "uploads.db"
    async with SqliteUploadJobStore(path) as store:
        await store.create(_job())
    assert _user_version(path) == 1

    # A database saved without a version has the same layout as version 1.
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version = 0")
    connection.close()
    async with SqliteUploadJobStore(path) as store:
        assert (await store.load("job-1")).job_id == "job-1"
    assert _user_version(path) == 1

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version = 2")
    connection.close()
    with pytest.raises(UploadStateError, match="layout version 2"):
        await SqliteUploadJobStore(path).list_jobs()
    assert _user_version(path) == 2


@pytest.mark.asyncio
async def test_sqlite_store_upgrades_older_saved_jobs(tmp_path: Path) -> None:
    path = tmp_path / "uploads.db"
    async with SqliteUploadJobStore(path) as store:
        await store.create(_job())
    connection = sqlite3.connect(path)
    with connection:
        (header,) = connection.execute("SELECT header FROM jobs").fetchone()
        state = json.loads(header)
        del state["metadata"]
        state["profile_name"] = "work"
        connection.execute("UPDATE jobs SET header = ?", (json.dumps(state),))
    connection.close()

    async with SqliteUploadJobStore(path) as store:
        assert (await store.load("job-1")).metadata == {"profile_name": "work"}


@pytest.mark.asyncio
async def test_sqlite_store_reports_corrupt_rows_and_unusable_paths(tmp_path: Path) -> None:
    store = SqliteUploadJobStore(tmp_path / "uploads.db")
    await store.create(_job())
    connection = sqlite3.connect(tmp_path / "uploads.db")
    with connection:
        connection.execute("UPDATE files SET record = '{not json'")
    connection.close()

    with pytest.raises(UploadStateError, match="corrupt or unreadable"):
        await store.load("job-1")
    store.close()

    blocker = tmp_path / "file"
    blocker.write_text("")
    with pytest.raises(UploadStateError, match="Could not create upload state directory"):
        await SqliteUploadJobStore(blocker / "uploads.db").list_jobs()
    (tmp_path / "directory.db").mkdir()
    with pytest.raises(UploadStateError, match="Could not use upload state database"):
        await SqliteUploadJobStore(tmp_path / "directory.db").list_jobs()
