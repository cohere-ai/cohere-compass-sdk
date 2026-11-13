"""
Where upload jobs keep the state that lets them resume.

The upload workflow never chooses a storage location itself. Callers pass an
`UploadJobStore`:

- `SqliteUploadJobStore` keeps jobs in a SQLite database file the caller
  names. A status change costs the same however large the job is.
- `FileUploadJobStore` keeps one JSON file per job in a directory the caller
  names. A status change rewrites the whole file, so it suits jobs of up to a
  few thousand files.
- `InMemoryUploadJobStore` keeps jobs only for the life of the process.

No store ever holds credentials or document contents. Every store can be used
as an async context manager, which closes it on exit.
"""

import asyncio
import json
import secrets
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Self, TypeVar, cast

from pydantic import ValidationError

from cohere_compass.ext.uploads import _fs as fs
from cohere_compass.ext.uploads.exceptions import UploadStateError
from cohere_compass.ext.uploads.models import FileRecord, UploadJob

_JOB_ID_BYTES = 6
# The layout of `SqliteUploadJobStore`'s tables, saved as the database's
# `user_version`. Databases created before the version was saved have 0 and
# the same layout as version 1.
_SQLITE_SCHEMA_VERSION = 1
_Result = TypeVar("_Result")


class UploadJobStore(ABC):
    """
    Persistence for upload jobs, which `run_upload_job` updates as files progress.

    Implementations must keep a job's files independent: `update_file` replaces
    one file's record and leaves the others as they are. Methods raise
    `UploadStateError` for a missing or unusable job.
    """

    @abstractmethod
    async def generate_job_id(self) -> str:
        """Return a job ID that no stored job uses."""

    @abstractmethod
    async def create(self, job: UploadJob) -> None:
        """Store a new job; raise `UploadStateError` if its ID is already taken."""

    @abstractmethod
    async def load(self, job_id: str) -> UploadJob:
        """Return the stored job."""

    @abstractmethod
    async def update_file(self, job_id: str, key: str, record: FileRecord) -> None:
        """Replace the record of the job's file stored under *key*."""

    @abstractmethod
    async def delete(self, job_id: str) -> None:
        """Remove the stored job."""

    @abstractmethod
    async def list_jobs(self) -> list[UploadJob]:
        """Return every stored job, most recently updated first."""

    def location(self, job_id: str) -> str | None:
        """Describe where the job is stored, for messages to people; None if nowhere."""
        del job_id
        return None

    def close(self) -> None:
        """Release what the store holds open, such as a database connection; by default nothing."""

    async def __aenter__(self) -> Self:
        """Return the store, which is closed when the `async with` block exits."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the store."""
        self.close()


class InMemoryUploadJobStore(UploadJobStore):
    """Keeps jobs in memory, so a job lasts only as long as the process."""

    def __init__(self) -> None:
        """Start with no jobs."""
        self._jobs: dict[str, UploadJob] = {}

    async def generate_job_id(self) -> str:
        """Return a short random ID that no stored job uses."""
        while (job_id := secrets.token_hex(_JOB_ID_BYTES)) in self._jobs:
            pass  # pragma: no cover - a collision of 48 random bits
        return job_id

    async def create(self, job: UploadJob) -> None:
        """Store a copy of *job*."""
        if job.job_id in self._jobs:
            raise UploadStateError(f"Upload job {job.job_id!r} already exists.")
        self._jobs[job.job_id] = job.model_copy(deep=True)

    async def load(self, job_id: str) -> UploadJob:
        """Return a copy of the stored job, so callers cannot change it in place."""
        return self._get(job_id).model_copy(deep=True)

    async def update_file(self, job_id: str, key: str, record: FileRecord) -> None:
        """Replace one file's record with a copy of *record*."""
        job = self._get(job_id)
        job.files[key] = record.model_copy(deep=True)
        job.updated_at = datetime.now(UTC)

    async def delete(self, job_id: str) -> None:
        """Forget the job."""
        self._get(job_id)
        del self._jobs[job_id]

    async def list_jobs(self) -> list[UploadJob]:
        """Return copies of every job, most recently updated first."""
        jobs = [job.model_copy(deep=True) for job in self._jobs.values()]
        return sorted(jobs, key=lambda job: job.updated_at, reverse=True)

    def _get(self, job_id: str) -> UploadJob:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise UploadStateError(f"Upload job {job_id!r} was not found.") from None


class FileUploadJobStore(UploadJobStore):
    """
    Keeps each job as an atomically written JSON file in a caller-chosen directory.

    A missing directory is created with mode 0700, and a directory that already
    exists keeps its mode. Each job file is written with mode 0600. Every disk
    operation runs off the event loop.

    `update_file` re-reads, rewrites and syncs the whole job file, and a job's
    updates run one at a time. Each file changes status about six times during
    a fresh upload, and three times on a rerun that finds it already indexed.
    On a typical cloud VM one change takes about 25 ms in a job of 1,000 files
    and 250 ms in a job of 10,000, so a fresh upload of 10,000 files spends
    about four hours on its job file alone. Use `SqliteUploadJobStore` for
    jobs of more than a few thousand files.

    Concurrent updates to one job are serialized within a store instance, but
    not between processes: two processes must not run the same job at once.
    """

    def __init__(self, directory: str | Path) -> None:
        """Use *directory* for job files; nothing is created until a job is."""
        self.directory = Path(directory)
        self._locks: dict[str, asyncio.Lock] = {}

    def path_for(self, job_id: str) -> Path:
        """Return the job's file path; raise `UploadStateError` for an unsafe ID."""
        if not job_id or Path(job_id).name != job_id or job_id in {".", ".."}:
            raise UploadStateError("Upload job ID must be a non-empty file name.")
        return self.directory / f"{job_id}.json"

    def location(self, job_id: str) -> str:
        """Return the job file's path."""
        return str(self.path_for(job_id))

    async def generate_job_id(self) -> str:
        """Return a short random ID with no file in the directory yet."""
        while True:
            job_id = secrets.token_hex(_JOB_ID_BYTES)
            if not await fs.path_exists(self.path_for(job_id)):
                return job_id

    async def create(self, job: UploadJob) -> None:
        """Write a new job file, creating the directory if needed."""
        try:
            await fs.ensure_private_dir(self.directory)
        except OSError as error:
            raise UploadStateError(f"Could not create upload state directory {self.directory}: {error}.") from error
        path = self.path_for(job.job_id)
        if await fs.path_exists(path):
            raise UploadStateError(f"Upload job {job.job_id!r} already exists at {path}; refusing to overwrite it.")
        await self._write(job, path)

    async def load(self, job_id: str) -> UploadJob:
        """Read and validate the job file."""
        path = self.path_for(job_id)
        if not await fs.is_file(path):
            raise UploadStateError(f"Upload job {job_id!r} was not found at {path}.")
        return await self._load_path(path, job_id)

    async def update_file(self, job_id: str, key: str, record: FileRecord) -> None:
        """Rewrite the job file with *record* as the file's new record."""
        lock = self._locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            job = await self.load(job_id)
            job.files[key] = record.model_copy(deep=True)
            job.updated_at = datetime.now(UTC)
            await self._write(job, self.path_for(job_id))

    async def delete(self, job_id: str) -> None:
        """Delete the job file."""
        path = self.path_for(job_id)
        try:
            await fs.remove(path)
        except FileNotFoundError as error:
            raise UploadStateError(f"Upload job {job_id!r} was not found at {path}.") from error
        except OSError as error:
            raise UploadStateError(f"Could not delete upload job {job_id!r} at {path}: {error}.") from error

    async def list_jobs(self) -> list[UploadJob]:
        """Read every job file in the directory, most recently updated first."""
        if not await fs.path_exists(self.directory):
            return []
        paths = await fs.glob(self.directory, "*.json")
        jobs = [await self._load_path(path, path.stem) for path in paths]
        return sorted(jobs, key=lambda job: job.updated_at, reverse=True)

    async def _write(self, job: UploadJob, path: Path) -> None:
        try:
            payload = json.dumps(job.model_dump(mode="json"), indent=2, sort_keys=True)
            await fs.atomic_write_bytes(path, (payload + "\n").encode("utf-8"))
        except (OSError, TypeError, ValueError) as error:
            raise UploadStateError(f"Could not write upload job {job.job_id!r} to {path}: {error}.") from error

    async def _load_path(self, path: Path, expected_job_id: str) -> UploadJob:
        try:
            value: object = json.loads(await fs.read_bytes(path))
            job = UploadJob.model_validate_json(_upgraded_state_json(value))
            if job.job_id != expected_job_id:
                raise ValueError(f"job_id {job.job_id!r} does not match filename {path.name!r}")
            return job
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValidationError,
            ValueError,
        ) as error:
            raise UploadStateError(f"Upload state file {path} is corrupt or unreadable: {error}.") from error


class SqliteUploadJobStore(UploadJobStore):
    """
    Keeps jobs in a SQLite database file the caller chooses, one row per file.

    `update_file` writes only the changed file's row, so its cost does not grow
    with the size of the job. The database is created on first use with mode
    0600. Its directory is created with mode 0700 if it is missing, and keeps
    its mode if it already exists. Every database operation runs off the event
    loop.

    SQLite locks the database, so several processes may share it, provided the
    database is on a local disk: the store uses SQLite's WAL mode, which does
    not work on network file systems such as NFS. Two processes must still not
    run the same job at once.

    Close the store when done with it, by calling `close` or by using it as an
    async context manager. A database written by a newer SDK, with a newer
    table layout, raises `UploadStateError` rather than being changed.
    """

    def __init__(self, path: str | Path) -> None:
        """Use the database at *path*; nothing is created until first use."""
        self.path = Path(path)
        self._connection: sqlite3.Connection | None = None
        # The connection is shared by the threads `asyncio.to_thread` uses,
        # so calls on it are serialized.
        self._lock = threading.Lock()

    def location(self, job_id: str) -> str:
        """Return the database's path."""
        del job_id
        return str(self.path)

    def close(self) -> None:
        """Close the database connection. The store reopens it if used again."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    async def generate_job_id(self) -> str:
        """Return a short random ID that no stored job uses."""

        def generate(connection: sqlite3.Connection) -> str:
            while True:
                job_id = secrets.token_hex(_JOB_ID_BYTES)
                if connection.execute("SELECT 1 FROM jobs WHERE job_id = ?", (job_id,)).fetchone() is None:
                    return job_id

        return await self._run(generate)

    async def create(self, job: UploadJob) -> None:
        """Store a new job and all its files."""
        header = job.model_dump_json(exclude={"files", "updated_at"})
        rows = [
            (job.job_id, key, position, record.model_dump_json())
            for position, (key, record) in enumerate(job.files.items())
        ]

        def create(connection: sqlite3.Connection) -> None:
            try:
                with connection:
                    connection.execute(
                        "INSERT INTO jobs (job_id, header, updated_at) VALUES (?, ?, ?)",
                        (job.job_id, header, job.updated_at.isoformat()),
                    )
                    connection.executemany(
                        "INSERT INTO files (job_id, key, position, record) VALUES (?, ?, ?, ?)", rows
                    )
            except sqlite3.IntegrityError:
                raise UploadStateError(f"Upload job {job.job_id!r} already exists in {self.path}.") from None

        await self._run(create)

    async def load(self, job_id: str) -> UploadJob:
        """Read and validate the stored job."""

        def load(connection: sqlite3.Connection) -> UploadJob:
            row = connection.execute("SELECT header, updated_at FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise UploadStateError(f"Upload job {job_id!r} was not found in {self.path}.")
            files = connection.execute(
                "SELECT key, record FROM files WHERE job_id = ? ORDER BY position", (job_id,)
            ).fetchall()
            return self._job(job_id, row[0], row[1], files)

        return await self._run(load)

    async def update_file(self, job_id: str, key: str, record: FileRecord) -> None:
        """Replace one file's row, adding it if the job did not have the file."""
        data = record.model_dump_json()
        updated_at = datetime.now(UTC).isoformat()

        def update(connection: sqlite3.Connection) -> None:
            with connection:
                changed = connection.execute(
                    "UPDATE jobs SET updated_at = ? WHERE job_id = ?", (updated_at, job_id)
                ).rowcount
                if not changed:
                    raise UploadStateError(f"Upload job {job_id!r} was not found in {self.path}.")
                replaced = connection.execute(
                    "UPDATE files SET record = ? WHERE job_id = ? AND key = ?", (data, job_id, key)
                ).rowcount
                if not replaced:
                    # Only a new file needs a position, so only it pays for finding the last one.
                    connection.execute(
                        """
                        INSERT INTO files (job_id, key, position, record)
                        VALUES (?, ?, (SELECT COALESCE(MAX(position), -1) + 1 FROM files WHERE job_id = ?), ?)
                        """,
                        (job_id, key, job_id, data),
                    )

        await self._run(update)

    async def delete(self, job_id: str) -> None:
        """Remove the job and its files."""

        def delete(connection: sqlite3.Connection) -> None:
            with connection:
                connection.execute("DELETE FROM files WHERE job_id = ?", (job_id,))
                if not connection.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,)).rowcount:
                    raise UploadStateError(f"Upload job {job_id!r} was not found in {self.path}.")

        await self._run(delete)

    async def list_jobs(self) -> list[UploadJob]:
        """Read every stored job, most recently updated first."""

        def list_jobs(connection: sqlite3.Connection) -> list[UploadJob]:
            jobs = connection.execute("SELECT job_id, header, updated_at FROM jobs").fetchall()
            files: dict[str, list[tuple[str, str]]] = {job_id: [] for job_id, _, _ in jobs}
            for job_id, key, record in connection.execute(
                "SELECT job_id, key, record FROM files ORDER BY job_id, position"
            ):
                files.setdefault(job_id, []).append((key, record))
            return [self._job(job_id, header, updated_at, files[job_id]) for job_id, header, updated_at in jobs]

        jobs = await self._run(list_jobs)
        return sorted(jobs, key=lambda job: job.updated_at, reverse=True)

    def _job(self, job_id: str, header: str, updated_at: str, files: list[tuple[str, str]]) -> UploadJob:
        try:
            state = cast(dict[str, object], json.loads(header))
            state["updated_at"] = updated_at
            state["files"] = {key: json.loads(record) for key, record in files}
            return UploadJob.model_validate_json(_upgraded_state_json(state))
        except (TypeError, ValueError, ValidationError) as error:
            raise UploadStateError(
                f"Upload job {job_id!r} in {self.path} is corrupt or unreadable: {error}."
            ) from error

    async def _run(self, operation: Callable[[sqlite3.Connection], _Result]) -> _Result:
        def run() -> _Result:
            with self._lock:
                try:
                    return operation(self._connect())
                except sqlite3.Error as error:
                    raise UploadStateError(f"Could not use upload state database {self.path}: {error}.") from error

        return await asyncio.to_thread(run)

    def _connect(self) -> sqlite3.Connection:
        if self._connection is not None:
            return self._connection
        try:
            fs.ensure_private_dir_sync(self.path.parent)
        except OSError as error:
            raise UploadStateError(f"Could not create upload state directory {self.path.parent}: {error}.") from error
        new = not self.path.exists()
        # A busy timeout lets another process finish its write instead of failing at once.
        connection = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        if new:
            self.path.chmod(fs.PRIVATE_FILE_MODE)
        version = cast(int, connection.execute("PRAGMA user_version").fetchone()[0])
        if version > _SQLITE_SCHEMA_VERSION:
            connection.close()
            raise UploadStateError(
                f"Upload state database {self.path} has table layout version {version}, but this SDK supports "
                f"version {_SQLITE_SCHEMA_VERSION} at most. Upgrade cohere-compass-sdk to use it."
            )
        # WAL with NORMAL syncing makes each commit cheap and keeps the database
        # consistent after a crash; at worst the last few status changes are lost,
        # which a resumed job recovers by checking those files again.
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                header TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS files (
                job_id TEXT NOT NULL,
                key TEXT NOT NULL,
                position INTEGER NOT NULL,
                record TEXT NOT NULL,
                PRIMARY KEY (job_id, key)
            );
            """
        )
        if version < _SQLITE_SCHEMA_VERSION:
            # PRAGMA statements take no parameters; the value is an int constant.
            connection.execute(f"PRAGMA user_version = {_SQLITE_SCHEMA_VERSION}")
        self._connection = connection
        return connection


def _upgraded_state_json(value: object) -> str:
    """
    Return saved job JSON in the current format, accepting older saved jobs.

    Both `FileUploadJobStore` and `SqliteUploadJobStore` pass every job they
    load through this function. Older jobs may lack `options.parser_config`,
    or have a top-level `profile_name`, which moves into `metadata` under the
    same key.
    """
    if not isinstance(value, dict):
        return json.dumps(value)
    state = dict(cast(Mapping[str, object], value))
    options = state.get("options")
    if isinstance(options, dict):
        parsed_options = dict(cast(Mapping[str, object], options))
        parsed_options.setdefault("parser_config", {})
        state["options"] = parsed_options
    if "profile_name" in state:
        profile_name = state.pop("profile_name")
        metadata = state.get("metadata")
        merged = dict(cast(Mapping[str, object], metadata)) if isinstance(metadata, dict) else {}
        if isinstance(profile_name, str):
            merged.setdefault("profile_name", profile_name)
        state["metadata"] = merged
    return json.dumps(state)
