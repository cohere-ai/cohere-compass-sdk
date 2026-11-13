"""
Bounded, resumable upload orchestration.

Each worker owns a file end-to-end: through upload and Compass-side
processing, not just the upload call.
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic

from pydantic import BaseModel, ConfigDict

from cohere_compass.clients import CompassAsyncClient
from cohere_compass.exceptions import CompassClientError, CompassError
from cohere_compass.ext.uploads.exceptions import SourceError
from cohere_compass.ext.uploads.identity import content_identity
from cohere_compass.ext.uploads.models import (
    FileRecord,
    FileStatus,
    SourceFile,
    UploadJob,
    UploadOptions,
    UploadOutcome,
)
from cohere_compass.ext.uploads.polling import PollResult, wait_for_ingestion
from cohere_compass.ext.uploads.pruning import PruneResult, prune_index
from cohere_compass.ext.uploads.reporting import UploadReporter
from cohere_compass.ext.uploads.sources import (
    content_type_for,
    read_source_bytes,
)
from cohere_compass.ext.uploads.store import UploadJobStore
from cohere_compass.models.documents import ParseableDocumentConfig

Poller = Callable[[CompassAsyncClient, str, str, Sequence[str], float, float], Awaitable[PollResult]]
Reader = Callable[[SourceFile], Awaitable[bytes]]

# A dry-run job is never saved, so it needs no unique ID.
DRY_RUN_JOB_ID = "dry-run"


class UploadResult(BaseModel):
    """What one run of an upload job did."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    job_id: str
    outcomes: tuple[UploadOutcome, ...]
    # Where the store keeps the job, or None for a dry run or a store that
    # does not say.
    location: str | None = None
    # Every file of the job as it stands after this run, including files that
    # an earlier run of the same job settled. `outcomes` covers only this run.
    files: tuple[FileRecord, ...] = ()
    # Set when the job pruned the index, which it skips if any file could not
    # be read.
    prune: PruneResult | None = None
    dry_run: bool = False

    @property
    def succeeded(self) -> bool:
        """Whether every file and every deletion of this run succeeded."""
        return all(self._settled(outcome.status) for outcome in self.outcomes) and (
            self.prune is None or self.prune.succeeded
        )

    def _settled(self, status: FileStatus) -> bool:
        # A dry run leaves the files it would upload pending.
        return status.is_successful_terminal or (self.dry_run and status is FileStatus.PENDING)

    @property
    def submitted(self) -> bool:
        """Whether a file was left processing, which only a run without waiting does."""
        return not self.dry_run and any(not outcome.status.is_terminal for outcome in self.outcomes)


async def create_upload_job(
    *,
    options: UploadOptions,
    files: Sequence[SourceFile],
    store: UploadJobStore,
    job_id: str | None = None,
    api_url: str | None = None,
    metadata: Mapping[str, str] | None = None,
) -> UploadJob:
    """
    Create a job for *files* and save it in *store* before anything is uploaded.

    *job_id* names the job; by default the store generates a random one. A
    caller that names its jobs, for example after the index, can find its own
    unfinished job with `store.load` instead of searching `store.list_jobs`.
    Creating a job under an ID the store already holds raises `UploadStateError`.

    *api_url* and *metadata* are saved with the job for the caller's own use,
    such as checking that it resumes the job against the same deployment; the
    workflow never reads them, and neither may hold credentials.

    A dry run's job is returned without being saved.
    """
    now = datetime.now(UTC)
    if job_id is None:
        job_id = DRY_RUN_JOB_ID if options.dry_run else await store.generate_job_id()
    job = UploadJob(
        job_id=job_id,
        created_at=now,
        updated_at=now,
        api_url=api_url,
        metadata=dict(metadata or {}),
        options=options,
        files={source.uri: FileRecord(source=source) for source in files},
    )
    if not options.dry_run:
        await store.create(job)
    return job


async def run_upload_job(
    job: UploadJob,
    *,
    client: CompassAsyncClient,
    store: UploadJobStore,
    reporter: UploadReporter | None = None,
    poller: Poller = wait_for_ingestion,
    reader: Reader = read_source_bytes,
) -> UploadResult:
    """
    Run a new or saved job, or resume one, with its configured number of workers.

    Each file resumes from its saved status: settled files are skipped, and a
    file that was processing is polled again before anything is resubmitted.
    The job is deleted from *store* once every file and deletion succeeded,
    and kept otherwise, so running it again retries what is left.

    A dry run reads, identifies and checks each file as a real run does, but
    stops before uploading it, prunes nothing, and saves no state.

    Raises `CompassError` for failures that stop the whole job, such as a
    missing index. If the run is cancelled, every file's progress so far is
    already saved in *store*; the cancellation propagates after
    `reporter.job_interrupted` is called.
    """
    reporter = reporter or UploadReporter()
    dry_run = job.options.dry_run
    await client.get_index_details(index_name=job.options.index_name)
    location = None if dry_run else store.location(job.job_id)
    reporter.job_started(
        None if dry_run else job.job_id,
        location,
        len(job.files),
        job.options.workers,
    )
    outcomes: list[UploadOutcome] = []
    prune: PruneResult | None = None
    try:
        outcomes = await _run_files(job, client, store, reporter, poller, reader)
        # Every file that was read has a known document ID, so it counts as
        # part of the source even if its upload or processing failed. A file
        # that could not be read has none, so its document in the index would
        # look stale and be deleted; pruning is skipped instead.
        if job.options.prune and all(record.document_id is not None for record in job.files.values()):
            prune = await prune_index(
                client,
                job.options.index_name,
                _document_ids(job),
                workers=job.options.workers,
                dry_run=dry_run,
            )
            reporter.prune_completed(prune)
    except (asyncio.CancelledError, KeyboardInterrupt):
        reporter.job_interrupted(None if dry_run else job.job_id)
        raise
    finally:
        reporter.job_finished()

    result = UploadResult(
        job_id=job.job_id,
        outcomes=tuple(outcomes),
        location=location,
        files=tuple(record.model_copy(deep=True) for record in job.files.values()),
        prune=prune,
        dry_run=dry_run,
    )
    if result.succeeded and not dry_run:
        await store.delete(job.job_id)
    return result


def _document_ids(job: UploadJob) -> set[str]:
    return {record.document_id for record in job.files.values() if record.document_id is not None}


async def _run_files(
    job: UploadJob,
    client: CompassAsyncClient,
    store: UploadJobStore,
    reporter: UploadReporter,
    poller: Poller,
    reader: Reader,
) -> list[UploadOutcome]:
    """Process the job's files with its workers, returning one outcome per file."""
    queue: asyncio.Queue[tuple[str, FileRecord] | None] = asyncio.Queue()
    for key, record in job.files.items():
        queue.put_nowait((key, record))
    for _ in range(job.options.workers):
        queue.put_nowait(None)

    outcomes: list[UploadOutcome] = []

    async def worker() -> None:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                key, record = item
                outcomes.append(await _process_file(job, key, record, client, store, reporter, poller, reader))
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(job.options.workers)]
    try:
        # Do not use Queue.join here.  A worker which raises before it drains
        # the queue would otherwise leave join waiting forever.  Waiting for
        # the workers also makes an unexpected SDK/programming error visible
        # immediately, at which point no more queued files may be started.
        pending: set[asyncio.Task[None]] = set(workers)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_EXCEPTION)
            error = next(
                (task.exception() for task in done if not task.cancelled() and task.exception() is not None),
                None,
            )
            if error is not None:
                raise error
    finally:
        # Reached on a worker error and on cancellation alike: no worker may
        # outlive this function, or it would keep starting queued files.
        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    return outcomes


async def _process_file(
    job: UploadJob,
    key: str,
    record: FileRecord,
    client: CompassAsyncClient,
    store: UploadJobStore,
    reporter: UploadReporter,
    poller: Poller,
    reader: Reader,
) -> UploadOutcome:
    """
    Advance one file through hashing, dedup-check, upload, and polling.

    Resumes from wherever `record`'s saved status left off, so a saved
    terminal upload/ingestion failure is retried at most once here.
    """
    started = monotonic()
    if record.status.is_successful_terminal or record.status is FileStatus.SOURCE_CHANGED:
        return _outcome(record, started, reporter)

    # A saved upload always gets polled before considering a re-upload.  In
    # particular, timeouts and status errors retain their upload ID: polling
    # them on a later resume is safer than submitting duplicate content.
    if record.upload_id is not None:
        await _poll(job, key, record, client, store, reporter, poller)
        if record.status is not FileStatus.INGESTION_FAILED:
            return _outcome(record, started, reporter)
        # An ingestion failure is retried at most once for this invocation.
        record.upload_id = None

    # A fresh invocation never retries a file in the same run.  The only
    # retry path is a saved terminal upload/ingestion failure handled above.
    if record.status is FileStatus.UPLOAD_FAILED:
        record.upload_id = None

    try:
        await _update_file_status(store, job, key, record, FileStatus.HASHING)
        reporter.file_phase_changed(record.source, FileStatus.HASHING)
        data = await reader(record.source)
    except (SourceError, OSError) as error:
        await _update_file_status(store, job, key, record, FileStatus.UPLOAD_FAILED, str(error))
        return _outcome(record, started, reporter)

    digest, document_id = content_identity(data, job.options.parser_config)
    if record.content_sha256 is not None and record.content_sha256 != digest:
        await _update_file_status(store, job, key, record, FileStatus.SOURCE_CHANGED)
        return _outcome(record, started, reporter)
    record.content_sha256, record.document_id = digest, document_id
    await _update_file_status(store, job, key, record, FileStatus.CHECKING)
    reporter.file_phase_changed(record.source, FileStatus.CHECKING)
    # `force` bypasses the existing-document check entirely.
    if not job.options.force:
        try:
            await client.get_document(
                index_name=job.options.index_name,
                document_id=document_id,
            )
        except CompassClientError as error:
            if error.code != 404:
                raise
        else:
            await _update_file_status(store, job, key, record, FileStatus.ALREADY_INDEXED)
            return _outcome(record, started, reporter)
    if job.options.dry_run:
        # Pending, in a dry run, marks a file the real run would upload.
        await _update_file_status(store, job, key, record, FileStatus.PENDING)
        return _outcome(record, started, reporter)

    await _update_file_status(store, job, key, record, FileStatus.UPLOADING)
    reporter.file_phase_changed(record.source, FileStatus.UPLOADING)
    try:
        submission = await client.upload_document(
            index_name=job.options.index_name,
            filename=Path(record.source.relative_path).name,
            filebytes=data,
            document_id=document_id,
            config=ParseableDocumentConfig(parser_config=job.options.parser_config),
            content_type=content_type_for(record.source.relative_path),
        )
    except CompassError as error:
        await _update_file_status(store, job, key, record, FileStatus.UPLOAD_FAILED, str(error))
        return _outcome(record, started, reporter)
    record.upload_id = str(submission.upload_id)
    record.attempts += 1
    if not job.options.wait:
        await _update_file_status(store, job, key, record, FileStatus.PROCESSING)
        reporter.file_phase_changed(record.source, FileStatus.PROCESSING)
        return _outcome(record, started, reporter)
    # `_poll` saves the record, with its new upload ID, before it starts polling.
    await _poll(job, key, record, client, store, reporter, poller)
    return _outcome(record, started, reporter)


async def _poll(
    job: UploadJob,
    key: str,
    record: FileRecord,
    client: CompassAsyncClient,
    store: UploadJobStore,
    reporter: UploadReporter,
    poller: Poller,
) -> PollResult:
    assert record.upload_id is not None and record.document_id is not None
    await _update_file_status(store, job, key, record, FileStatus.PROCESSING)
    reporter.file_phase_changed(record.source, FileStatus.PROCESSING)
    try:
        result = await poller(
            client,
            job.options.index_name,
            record.upload_id,
            (record.document_id,),
            job.options.timeout_seconds,
            job.options.poll_interval_seconds,
        )
    except CompassError as error:
        result = PollResult(status=FileStatus.STATUS_ERROR, error=str(error))
    await _update_file_status(store, job, key, record, result.status, result.error)
    return result


async def _update_file_status(
    store: UploadJobStore,
    job: UploadJob,
    key: str,
    record: FileRecord,
    status: FileStatus,
    error: str | None = None,
) -> None:
    """
    Set `record`'s status in place and save the record to the stored job.

    A dry-run job is never saved, so its status changes stay in memory.
    """
    record.status, record.last_error, record.updated_at = (
        status,
        error,
        datetime.now(UTC),
    )
    if not job.options.dry_run:
        await store.update_file(job.job_id, key, record)


def _outcome(record: FileRecord, started: float, reporter: UploadReporter) -> UploadOutcome:
    outcome = UploadOutcome(
        source_uri=record.source.uri,
        status=record.status,
        document_id=record.document_id,
        upload_id=record.upload_id,
        error=record.last_error,
        elapsed_seconds=monotonic() - started,
    )
    reporter.file_outcome(record.source, outcome)
    return outcome
