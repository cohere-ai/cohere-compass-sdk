import asyncio
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, ConfigDict

from cohere_compass.clients import CompassAsyncClient
from cohere_compass.exceptions import CompassClientError, CompassError
from cohere_compass.ext.uploads.exceptions import SourceError
from cohere_compass.ext.uploads.identity import content_identity
from cohere_compass.ext.uploads.models import (
    FileRecord,
    FileStatus,
    SourceFile,
    SourceType,
    UploadJob,
    UploadOptions,
    UploadOutcome,
)
from cohere_compass.ext.uploads.polling import PollResult
from cohere_compass.ext.uploads.pruning import PruneResult
from cohere_compass.ext.uploads.reporting import UploadReporter
from cohere_compass.ext.uploads.service import (
    DRY_RUN_JOB_ID,
    Reader,
    UploadResult,
    create_upload_job,
    run_upload_job,
)
from cohere_compass.ext.uploads.store import FileUploadJobStore, InMemoryUploadJobStore, UploadStateError
from cohere_compass.models import (
    ContentTypeEnum,
    UploadDocumentsResult,
    UploadDocumentsStatus,
)
from cohere_compass.models.config import ParserConfig, PDFParsingStrategy
from cohere_compass.models.documents import ParseableDocumentConfig
from cohere_compass.models.indexes import IndexDetails
from cohere_compass.models.search import RetrievedDocument


class Submission(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    upload_id: UUID


DEFAULT_PARSEABLE_DOCUMENT_CONFIG = ParseableDocumentConfig()


class Client:
    """An async fake mirroring `CompassAsyncClient`.

    Every method is a coroutine, as production now awaits them directly rather
    than pushing them through `asyncio.to_thread`. The six subclasses below
    override individual methods and must stay `async def` in lockstep, or
    pyright strict reports override variance.
    """

    def __init__(self) -> None:
        self.uploads: list[str] = []
        self.upload_configs: list[ParseableDocumentConfig] = []
        self.preflight_calls = 0

    async def get_index_details(self, *, index_name: str) -> IndexDetails:
        self.preflight_calls += 1
        return cast(IndexDetails, object())

    async def get_document(self, *, index_name: str, document_id: str) -> RetrievedDocument:
        raise CompassClientError("missing", 404)

    async def upload_document(
        self,
        *,
        index_name: str,
        filename: str,
        filebytes: bytes,
        document_id: str,
        config: ParseableDocumentConfig = DEFAULT_PARSEABLE_DOCUMENT_CONFIG,
        content_type: ContentTypeEnum | None = None,
    ) -> UploadDocumentsResult:
        self.uploads.append(document_id)
        self.upload_configs.append(config)
        return cast(UploadDocumentsResult, Submission(upload_id=uuid4()))

    async def upload_document_status(self, *, index_name: str, upload_id: UUID) -> list[UploadDocumentsStatus] | None:
        return []


async def _save(store: FileUploadJobStore, job: UploadJob) -> None:
    """Write every file of *job*, as changed by the test, back to *store*."""
    for key, record in job.files.items():
        await store.update_file(job.job_id, key, record)


def _reader(read: Callable[[SourceFile], bytes]) -> Reader:
    """Wrap a plain byte-producing lambda as the coroutine `Reader` production wants."""

    async def reader(source: SourceFile) -> bytes:
        return read(source)

    return reader


def _options(
    *,
    workers: int = 2,
    wait: bool = True,
    prune: bool = False,
    dry_run: bool = False,
) -> UploadOptions:
    return UploadOptions(
        source_uri="source",
        source_type=SourceType.LOCAL_DIRECTORY if wait else SourceType.LOCAL_FILE,
        index_name="index",
        workers=workers,
        recursive=True,
        include_patterns=(),
        exclude_patterns=(),
        timeout_seconds=1,
        poll_interval_seconds=1,
        wait=wait,
        force=False,
        prune=prune,
        dry_run=dry_run,
    )


@pytest.mark.asyncio
async def test_bounded_workers_and_terminal_state_deletion(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    files = [SourceFile(uri=f"file-{number}", relative_path=f"{number}.txt") for number in range(4)]
    job = await create_upload_job(
        api_url="https://api",
        metadata={"profile_name": "work"},
        options=_options(),
        files=files,
        store=store,
    )
    active = 0
    maximum = 0

    async def poller(*args: object) -> PollResult:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return PollResult(status=FileStatus.COMPLETED)

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
        poller=poller,
        reader=_reader(lambda source: source.uri.encode()),
    )
    assert maximum == 2
    assert result.succeeded
    assert not store.path_for(job.job_id).exists()
    assert sorted(record.source.uri for record in result.files) == [f"file-{number}" for number in range(4)]
    assert {record.status for record in result.files} == {FileStatus.COMPLETED}


@pytest.mark.asyncio
async def test_resume_polls_existing_upload_without_reupload(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="file", relative_path="file.txt")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(),
        files=[source],
        store=store,
    )
    record = job.files[source.uri]
    record.document_id = "document"
    record.upload_id = str(uuid4())
    record.status = FileStatus.TIMED_OUT
    await _save(store, job)
    client = Client()

    async def poller(*args: object) -> PollResult:
        return PollResult(status=FileStatus.COMPLETED)

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=poller,
    )
    assert result.succeeded
    assert client.uploads == []
    assert client.upload_configs == []


@pytest.mark.asyncio
async def test_resume_polls_without_reconstructing_upload_config(
    tmp_path: Path,
) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="file", relative_path="file.txt")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(),
        files=[source],
        store=store,
    )
    record = job.files[source.uri]
    record.document_id = "document"
    record.upload_id = str(uuid4())
    record.status = FileStatus.TIMED_OUT
    await _save(store, job)
    client = Client()

    async def unreadable(_: SourceFile) -> bytes:
        raise AssertionError("must not read")

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=unreadable,
    )
    assert result.succeeded
    assert client.uploads == []
    assert client.upload_configs == []


@pytest.mark.asyncio
async def test_default_parser_config_is_sent_to_sdk(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[SourceFile(uri="one", relative_path="one.txt")],
        store=store,
    )
    client = Client()

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=_reader(lambda _: b"contents"),
    )

    assert result.succeeded
    assert client.upload_configs == [ParseableDocumentConfig()]


@pytest.mark.parametrize(
    ("strategy", "expected"),
    [
        ("QuickText", PDFParsingStrategy.QuickText),
        ("ImageToMarkdown", PDFParsingStrategy.ImageToMarkdown),
        ("Smart", PDFParsingStrategy.Smart),
    ],
)
@pytest.mark.asyncio
async def test_parser_strategy_is_sent_to_sdk(tmp_path: Path, strategy: str, expected: PDFParsingStrategy) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    options = UploadOptions(
        source_uri="source",
        source_type=SourceType.LOCAL_DIRECTORY,
        index_name="index",
        workers=1,
        recursive=True,
        include_patterns=(),
        exclude_patterns=(),
        timeout_seconds=1,
        poll_interval_seconds=1,
        wait=True,
        force=False,
        parser_config=ParserConfig.model_validate({"pdf_parsing_strategy": strategy}),
    )
    job = await create_upload_job(
        api_url="api",
        options=options,
        files=[SourceFile(uri="one", relative_path="one.pdf")],
        store=store,
    )
    client = Client()

    await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=_reader(lambda _: b"contents"),
    )

    assert client.upload_configs == [ParseableDocumentConfig(parser_config=options.parser_config)]
    assert client.upload_configs[0].parser_config.pdf_parsing_strategy is expected


@pytest.mark.asyncio
async def test_generic_and_nested_parser_config_are_sent_to_sdk(tmp_path: Path) -> None:
    parsing_options = ParserConfig.model_validate(
        {
            "num_tokens_per_chunk": 300,
            "num_tokens_overlap": 20,
            "pdf_parsing_config": {
                "enable_classification": False,
                "visual_parsing_threshold": 0.8,
            },
        }
    )
    store = FileUploadJobStore(tmp_path / "state")
    options = UploadOptions(
        source_uri="source",
        source_type=SourceType.LOCAL_DIRECTORY,
        index_name="index",
        workers=1,
        recursive=True,
        include_patterns=(),
        exclude_patterns=(),
        timeout_seconds=1,
        poll_interval_seconds=1,
        wait=True,
        force=False,
        parser_config=parsing_options,
    )
    job = await create_upload_job(
        api_url="api",
        options=options,
        files=[SourceFile(uri="one", relative_path="one.pdf")],
        store=store,
    )
    client = Client()

    await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=_reader(lambda _: b"contents"),
    )

    assert client.upload_configs == [ParseableDocumentConfig(parser_config=parsing_options)]
    config = client.upload_configs[0].parser_config
    assert config.num_tokens_per_chunk == 300
    assert config.num_tokens_overlap == 20
    assert config.pdf_parsing_config.enable_classification is False
    assert config.pdf_parsing_config.visual_parsing_threshold == 0.8


@pytest.mark.asyncio
async def test_parser_options_change_document_identity_and_existing_document_checks(
    tmp_path: Path,
) -> None:
    class ExistingClient(Client):
        def __init__(self, existing_document_id: str) -> None:
            super().__init__()
            self.existing_document_id = existing_document_id
            self.checked_document_ids: list[str] = []

        async def get_document(self, *, index_name: str, document_id: str) -> RetrievedDocument:
            self.checked_document_ids.append(document_id)
            if document_id == self.existing_document_id:
                return cast(RetrievedDocument, object())
            raise CompassClientError("missing", 404)

    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="one", relative_path="one.pdf")
    default_job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[source],
        store=store,
    )
    _, default_document_id = content_identity(b"same bytes", ParserConfig())
    client = ExistingClient(default_document_id)
    default_result = await run_upload_job(
        default_job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        reader=_reader(lambda _: b"same bytes"),
    )

    smart_options = UploadOptions(
        source_uri="source",
        source_type=SourceType.LOCAL_DIRECTORY,
        index_name="index",
        workers=1,
        recursive=True,
        include_patterns=(),
        exclude_patterns=(),
        timeout_seconds=1,
        poll_interval_seconds=1,
        wait=True,
        force=False,
        parser_config=ParserConfig.model_validate({"pdf_parsing_strategy": "Smart"}),
    )
    smart_job = await create_upload_job(
        api_url="api",
        options=smart_options,
        files=[source],
        store=store,
    )
    smart_result = await run_upload_job(
        smart_job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=_reader(lambda _: b"same bytes"),
    )

    assert default_result.outcomes[0].status is FileStatus.ALREADY_INDEXED
    assert smart_result.succeeded
    assert smart_result.outcomes[0].document_id != default_document_id
    assert client.checked_document_ids == [
        default_document_id,
        smart_result.outcomes[0].document_id,
    ]
    assert client.uploads == [smart_result.outcomes[0].document_id]


@pytest.mark.asyncio
async def test_no_wait_retains_processing_state(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    options = _options(wait=False)
    source = SourceFile(uri="file", relative_path="file.txt")
    job = await create_upload_job(
        api_url="https://api",
        options=options,
        files=[source],
        store=store,
    )
    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
        reader=_reader(lambda _: b"x"),
    )
    assert result.submitted
    assert (await store.load(job.job_id)).files[source.uri].status is FileStatus.PROCESSING


@pytest.mark.asyncio
async def test_timeout_releases_worker_without_reuploading(tmp_path: Path) -> None:
    class TimedClient(Client):
        def __init__(self) -> None:
            super().__init__()
            self.upload_ids: list[UUID] = []

        async def upload_document(
            self,
            *,
            index_name: str,
            filename: str,
            filebytes: bytes,
            document_id: str,
            config: ParseableDocumentConfig = DEFAULT_PARSEABLE_DOCUMENT_CONFIG,
            content_type: ContentTypeEnum | None = None,
        ) -> UploadDocumentsResult:
            self.uploads.append(document_id)
            self.upload_configs.append(config)
            upload_id = uuid4()
            self.upload_ids.append(upload_id)
            return cast(UploadDocumentsResult, Submission(upload_id=upload_id))

    store = FileUploadJobStore(tmp_path / "state")
    files = [SourceFile(uri=f"file-{number}", relative_path=f"{number}.txt") for number in range(2)]
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=1),
        files=files,
        store=store,
    )
    client = TimedClient()

    async def poller(*args: object) -> PollResult:
        upload_id = args[2]
        return PollResult(
            status=(FileStatus.TIMED_OUT if upload_id == str(client.upload_ids[0]) else FileStatus.COMPLETED)
        )

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=poller,
        reader=_reader(lambda source: source.uri.encode()),
    )
    assert [outcome.status for outcome in result.outcomes] == [
        FileStatus.TIMED_OUT,
        FileStatus.COMPLETED,
    ]
    assert len(client.uploads) == 2
    saved = (await store.load(job.job_id)).files[files[0].uri]
    assert saved.upload_id == str(client.upload_ids[0])


@pytest.mark.asyncio
async def test_fatal_worker_error_cancels_job_without_queue_deadlock(
    tmp_path: Path,
) -> None:
    class FatalClient(Client):
        async def get_document(self, *, index_name: str, document_id: str) -> RetrievedDocument:
            raise CompassClientError("backend failure", 500)

    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=1),
        files=[
            SourceFile(uri="one", relative_path="one.txt"),
            SourceFile(uri="two", relative_path="two.txt"),
        ],
        store=store,
    )

    async def run() -> None:
        await run_upload_job(
            job,
            client=cast(CompassAsyncClient, FatalClient()),
            store=store,
            reporter=UploadReporter(),
            reader=_reader(lambda source: source.uri.encode()),
        )

    try:
        await asyncio.wait_for(run(), timeout=2)
    except CompassClientError as error:
        assert error.code == 500
    else:
        raise AssertionError("fatal worker error was not propagated")


@pytest.mark.asyncio
async def test_fatal_worker_error_cancels_pending_peer_workers(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    files = [
        SourceFile(uri="one", relative_path="one.txt"),
        SourceFile(uri="two", relative_path="two.txt"),
    ]
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=2),
        files=files,
        store=store,
    )
    _, safe_document_id = content_identity(b"two", ParserConfig())

    class FatalClient(Client):
        async def get_document(self, *, index_name: str, document_id: str) -> RetrievedDocument:
            if document_id == safe_document_id:
                raise CompassClientError("missing", 404)
            raise CompassClientError("backend failure", 500)

    async def blocked_poller(*args: object) -> PollResult:
        del args
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def reader(source: SourceFile) -> bytes:
        return source.relative_path.removesuffix(".txt").encode()

    async def run() -> None:
        await run_upload_job(
            job,
            client=cast(CompassAsyncClient, FatalClient()),
            store=store,
            reporter=UploadReporter(),
            poller=blocked_poller,
            reader=reader,
        )

    try:
        await asyncio.wait_for(run(), timeout=2)
    except CompassClientError as error:
        assert error.code == 500
    else:
        raise AssertionError("fatal worker error was not propagated")


@pytest.mark.asyncio
async def test_existing_force_and_upload_failures_preserve_meaningful_state(
    tmp_path: Path,
) -> None:
    class ExistingClient(Client):
        async def get_document(self, *, index_name: str, document_id: str) -> RetrievedDocument:
            return cast(RetrievedDocument, object())

    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="one", relative_path="one.unknown")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[source],
        store=store,
    )
    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, ExistingClient()),
        store=store,
        reporter=UploadReporter(),
        reader=_reader(lambda _: b"x"),
    )
    assert result.outcomes[0].status is FileStatus.ALREADY_INDEXED

    options = UploadOptions(
        source_uri="source",
        source_type=SourceType.LOCAL_DIRECTORY,
        index_name="index",
        workers=1,
        recursive=True,
        include_patterns=(),
        exclude_patterns=(),
        timeout_seconds=1,
        poll_interval_seconds=1,
        wait=True,
        force=True,
    )
    forced = await create_upload_job(
        api_url="api",
        options=options,
        files=[SourceFile(uri="two", relative_path="two.txt")],
        store=store,
    )
    client = ExistingClient()
    result = await run_upload_job(
        forced,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=_reader(lambda _: b"x"),
    )
    assert result.succeeded
    assert len(client.uploads) == 1


async def _completed(*args: object) -> PollResult:
    del args
    return PollResult(status=FileStatus.COMPLETED)


@pytest.mark.asyncio
async def test_source_changed_reader_upload_and_poller_errors(tmp_path: Path) -> None:
    class FailingClient(Client):
        async def upload_document(
            self,
            *,
            index_name: str,
            filename: str,
            filebytes: bytes,
            document_id: str,
            config: ParseableDocumentConfig = DEFAULT_PARSEABLE_DOCUMENT_CONFIG,
            content_type: ContentTypeEnum | None = None,
        ) -> UploadDocumentsResult:
            del (
                index_name,
                filename,
                filebytes,
                document_id,
                config,
                content_type,
            )
            raise CompassError("upload bad")

    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[SourceFile(uri="one", relative_path="one.txt")],
        store=store,
    )
    job.files["one"].content_sha256 = "old"
    await _save(store, job)
    changed = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
        reader=_reader(lambda _: b"new"),
    )
    assert changed.outcomes[0].status is FileStatus.SOURCE_CHANGED

    failed = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[SourceFile(uri="two", relative_path="two.txt")],
        store=store,
    )
    result = await run_upload_job(
        failed,
        client=cast(CompassAsyncClient, FailingClient()),
        store=store,
        reporter=UploadReporter(),
        reader=_reader(lambda _: b"x"),
    )
    assert result.outcomes[0].status is FileStatus.UPLOAD_FAILED


@pytest.mark.asyncio
async def test_resumed_ingestion_failure_reuploads_once_and_poller_error_is_status_error(
    tmp_path: Path,
) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="one", relative_path="one.txt")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[source],
        store=store,
    )
    record = job.files[source.uri]
    record.document_id, record.upload_id, record.status = (
        "doc",
        str(uuid4()),
        FileStatus.INGESTION_FAILED,
    )
    await _save(store, job)
    calls = 0

    async def retry_poller(*args: object) -> PollResult:
        nonlocal calls
        calls += 1
        return PollResult(status=(FileStatus.INGESTION_FAILED if calls == 1 else FileStatus.COMPLETED))

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
        poller=retry_poller,
        reader=_reader(lambda _: b"x"),
    )
    assert result.succeeded and calls == 2


@pytest.mark.asyncio
async def test_resumed_upload_failed_without_saved_upload_id_reuploads(
    tmp_path: Path,
) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="one", relative_path="one.txt")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[source],
        store=store,
    )
    record = job.files[source.uri]
    record.status = FileStatus.UPLOAD_FAILED
    await _save(store, job)
    client = Client()

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, client),
        store=store,
        reporter=UploadReporter(),
        poller=_completed,
        reader=_reader(lambda _: b"x"),
    )
    assert result.succeeded
    assert len(client.uploads) == 1


@pytest.mark.asyncio
async def test_saved_terminal_reader_and_polling_errors_are_reported(
    tmp_path: Path,
) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    source = SourceFile(uri="one", relative_path="one.txt")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[source],
        store=store,
    )
    job.files[source.uri].status = FileStatus.COMPLETED
    await _save(store, job)
    terminal = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
    )
    assert terminal.succeeded

    reading = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[SourceFile(uri="two", relative_path="two.txt")],
        store=store,
    )

    async def unreadable(_: SourceFile) -> bytes:
        raise OSError("read failed")

    result = await run_upload_job(
        reading,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
        reader=unreadable,
    )
    assert result.outcomes[0].status is FileStatus.UPLOAD_FAILED

    polling = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[SourceFile(uri="three", relative_path="three.txt")],
        store=store,
    )

    async def failing_poller(*args: object) -> PollResult:
        del args
        raise CompassError("status failed")

    result = await run_upload_job(
        polling,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=UploadReporter(),
        poller=failing_poller,
        reader=_reader(lambda _: b"x"),
    )
    assert result.outcomes[0].status is FileStatus.STATUS_ERROR


@pytest.mark.asyncio
async def test_cancelling_a_running_job_saves_progress_and_propagates(
    tmp_path: Path,
) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="api",
        options=_options(workers=1),
        files=[SourceFile(uri="one", relative_path="one.txt")],
        store=store,
    )
    entered = asyncio.Event()
    reporter = InterruptionReporter()

    async def blocked_poller(*args: object) -> PollResult:
        del args
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(
        run_upload_job(
            job,
            client=cast(CompassAsyncClient, Client()),
            store=store,
            reporter=reporter,
            poller=blocked_poller,
            reader=_reader(lambda _: b"x"),
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert reporter.interruptions == [job.job_id] and reporter.finished
    saved = await store.load(job.job_id)
    assert [record.status for record in saved.files.values()] == [FileStatus.PROCESSING]


class InterruptionReporter(UploadReporter):
    def __init__(self) -> None:
        self.interruptions: list[str | None] = []
        self.finished = False

    def job_interrupted(self, job_id: str | None) -> None:
        self.interruptions.append(job_id)

    def job_finished(self) -> None:
        self.finished = True


class PruneRecorder:
    def __init__(self, result: PruneResult) -> None:
        self.result = result
        self.calls: list[tuple[str, set[str], int, bool]] = []

    async def __call__(
        self,
        client: object,
        index_name: str,
        expected: set[str],
        *,
        workers: int,
        dry_run: bool,
    ) -> PruneResult:
        self.calls.append((index_name, expected, workers, dry_run))
        return self.result


class PruneReporter(UploadReporter):
    def __init__(self) -> None:
        super().__init__()
        self.prunes: list[PruneResult] = []

    def prune_completed(self, result: PruneResult) -> None:
        self.prunes.append(result)


async def _prune_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prune: PruneResult,
    poll: FileStatus,
    reader: Reader | None = None,
) -> tuple[UploadResult, PruneRecorder, PruneReporter, FileUploadJobStore, str]:
    recorder = PruneRecorder(prune)
    monkeypatch.setattr("cohere_compass.ext.uploads.service.prune_index", recorder)
    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=3, prune=True),
        files=[
            SourceFile(uri="one", relative_path="one.txt"),
            SourceFile(uri="two", relative_path="two.txt"),
        ],
        store=store,
    )

    async def poller(*args: object) -> PollResult:
        expected = cast(Sequence[str], args[3])
        status = poll if expected[0] == content_identity(b"two", ParserConfig())[1] else FileStatus.COMPLETED
        return PollResult(status=status)

    reporter = PruneReporter()
    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=reporter,
        poller=poller,
        reader=reader or _reader(lambda source: source.uri.encode()),
    )
    return result, recorder, reporter, store, job.job_id


DOCUMENT_IDS = {content_identity(data, ParserConfig())[1] for data in (b"one", b"two")}


@pytest.mark.asyncio
async def test_prune_deletes_documents_missing_from_the_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prune = PruneResult(stale_document_ids=("old",), deleted_document_ids=("old",))
    result, recorder, reporter, store, job_id = await _prune_job(tmp_path, monkeypatch, prune, FileStatus.COMPLETED)

    assert recorder.calls == [("index", DOCUMENT_IDS, 3, False)]
    assert result.prune == prune and result.succeeded
    assert reporter.prunes == [prune]
    assert not store.path_for(job_id).exists()


@pytest.mark.asyncio
async def test_failed_files_still_count_as_part_of_the_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prune = PruneResult(stale_document_ids=("old",), deleted_document_ids=("old",))
    result, recorder, _, store, job_id = await _prune_job(tmp_path, monkeypatch, prune, FileStatus.INGESTION_FAILED)

    assert recorder.calls == [("index", DOCUMENT_IDS, 3, False)]
    assert result.prune == prune and not result.succeeded
    assert store.path_for(job_id).exists()


@pytest.mark.asyncio
async def test_prune_is_skipped_when_a_file_cannot_be_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def reader(source: SourceFile) -> bytes:
        if source.uri == "two":
            raise SourceError("unreadable")
        return source.uri.encode()

    prune = PruneResult(stale_document_ids=())
    result, recorder, _, store, job_id = await _prune_job(tmp_path, monkeypatch, prune, FileStatus.COMPLETED, reader)

    assert recorder.calls == []
    assert result.prune is None and not result.succeeded
    assert store.path_for(job_id).exists()


@pytest.mark.asyncio
async def test_failed_deletions_keep_the_job_for_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prune = PruneResult(stale_document_ids=("old",), errors={"old": "forbidden"})
    result, _, _, store, job_id = await _prune_job(tmp_path, monkeypatch, prune, FileStatus.COMPLETED)

    assert not result.succeeded
    assert store.path_for(job_id).exists()


class DryRunReporter(PruneReporter):
    def __init__(self) -> None:
        super().__init__()
        self.started: list[tuple[str | None, str | None]] = []
        self.interruptions: list[str | None] = []

    def job_started(
        self,
        job_id: str | None,
        location: str | None,
        total_files: int,
        workers: int,
    ) -> None:
        super().job_started(job_id, location, total_files, workers)
        self.started.append((job_id, location))

    def job_interrupted(self, job_id: str | None) -> None:
        self.interruptions.append(job_id)


class ExistingOldClient(Client):
    """Has `old.txt` indexed already and fails on any upload."""

    async def get_document(self, *, index_name: str, document_id: str) -> RetrievedDocument:
        if document_id == content_identity(b"old", ParserConfig())[1]:
            return cast(RetrievedDocument, object())
        raise CompassClientError("missing", 404)

    async def upload_document(
        self,
        *,
        index_name: str,
        filename: str,
        filebytes: bytes,
        document_id: str,
        config: ParseableDocumentConfig = DEFAULT_PARSEABLE_DOCUMENT_CONFIG,
        content_type: ContentTypeEnum | None = None,
    ) -> UploadDocumentsResult:
        raise AssertionError("a dry run must not upload")


@pytest.mark.asyncio
async def test_dry_run_checks_files_without_uploading_or_saving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prune = PruneResult(stale_document_ids=("stale",), dry_run=True)
    recorder = PruneRecorder(prune)
    monkeypatch.setattr("cohere_compass.ext.uploads.service.prune_index", recorder)
    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=2, prune=True, dry_run=True),
        files=[
            SourceFile(uri="new", relative_path="new.txt"),
            SourceFile(uri="old", relative_path="old.txt"),
        ],
        store=store,
    )
    reporter = DryRunReporter()

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, ExistingOldClient()),
        store=store,
        reporter=reporter,
        reader=_reader(lambda source: source.uri.encode()),
    )

    assert job.job_id == DRY_RUN_JOB_ID
    assert {record.source.uri: record.status for record in result.files} == {
        "new": FileStatus.PENDING,
        "old": FileStatus.ALREADY_INDEXED,
    }
    assert all(record.document_id is not None for record in result.files)
    assert result.succeeded and result.dry_run and not result.submitted
    assert result.prune == prune and recorder.calls[0][3] is True
    assert reporter.started == [(None, None)]
    assert not (tmp_path / "state").exists()


@pytest.mark.asyncio
async def test_dry_run_with_an_unreadable_file_fails(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=1, dry_run=True),
        files=[SourceFile(uri="broken", relative_path="broken.txt")],
        store=store,
    )

    async def unreadable(source: SourceFile) -> bytes:
        raise SourceError("unreadable")

    result = await run_upload_job(
        job,
        client=cast(CompassAsyncClient, ExistingOldClient()),
        store=store,
        reporter=UploadReporter(),
        reader=unreadable,
    )

    assert not result.succeeded
    assert result.files[0].status is FileStatus.UPLOAD_FAILED


@pytest.mark.asyncio
async def test_interrupted_dry_run_offers_no_resume(tmp_path: Path) -> None:
    store = FileUploadJobStore(tmp_path / "state")
    job = await create_upload_job(
        api_url="https://api",
        options=_options(workers=1, dry_run=True),
        files=[SourceFile(uri="new", relative_path="new.txt")],
        store=store,
    )
    reporter = DryRunReporter()
    entered = asyncio.Event()

    async def blocked(source: SourceFile) -> bytes:
        del source
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    task = asyncio.create_task(
        run_upload_job(
            job,
            client=cast(CompassAsyncClient, ExistingOldClient()),
            store=store,
            reporter=reporter,
            reader=blocked,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert reporter.interruptions == [None]


class EventReporter(UploadReporter):
    def __init__(self) -> None:
        self.events: list[str] = []

    def file_phase_changed(self, source: SourceFile, status: FileStatus) -> None:
        self.events.append(status.value)

    def file_outcome(self, source: SourceFile, outcome: UploadOutcome) -> None:
        self.events.append(f"outcome:{outcome.status.value}")


class RecordingStore(InMemoryUploadJobStore):
    def __init__(self) -> None:
        super().__init__()
        self.saved: list[tuple[FileStatus, str | None]] = []

    async def update_file(self, job_id: str, key: str, record: FileRecord) -> None:
        self.saved.append((record.status, record.upload_id))
        await super().update_file(job_id, key, record)


@pytest.mark.asyncio
@pytest.mark.parametrize("wait", [True, False])
async def test_each_phase_is_reported_and_saved_once(wait: bool) -> None:
    store = RecordingStore()
    job = await create_upload_job(
        options=_options(workers=1, wait=wait),
        files=[SourceFile(uri="one", relative_path="one.txt")],
        store=store,
    )
    reporter = EventReporter()

    await run_upload_job(
        job,
        client=cast(CompassAsyncClient, Client()),
        store=store,
        reporter=reporter,
        poller=_completed,
        reader=_reader(lambda _: b"x"),
    )

    if wait:
        assert reporter.events == ["hashing", "checking", "uploading", "processing", "outcome:completed"]
        assert [status for status, _ in store.saved] == ["hashing", "checking", "uploading", "processing", "completed"]
    else:
        assert reporter.events == ["hashing", "checking", "uploading", "processing", "outcome:processing"]
        assert [status for status, _ in store.saved] == ["hashing", "checking", "uploading", "processing"]
    # The upload ID is saved with the first status after the upload, before polling starts.
    assert store.saved[3][1] is not None


@pytest.mark.asyncio
async def test_jobs_take_a_caller_chosen_id_and_metadata() -> None:
    store = InMemoryUploadJobStore()
    files = [SourceFile(uri="one", relative_path="one.txt")]

    job = await create_upload_job(
        options=_options(), files=files, store=store, job_id="reports", metadata={"profile_name": "work"}
    )

    saved = await store.load("reports")
    assert job.job_id == "reports" and saved.metadata == {"profile_name": "work"} and saved.api_url is None
    with pytest.raises(UploadStateError, match="already exists"):
        await create_upload_job(options=_options(), files=files, store=store, job_id="reports")
