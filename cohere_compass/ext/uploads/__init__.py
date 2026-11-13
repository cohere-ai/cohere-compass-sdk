"""
Resumable uploads of a whole directory or `gs://` prefix to a Compass index.

A job lists the source's files, gives each a document ID derived from its
content and parser options, skips documents the index already has, and uploads
the rest with a bounded number of workers, each waiting for its file to finish
processing. With `prune`, it then deletes index documents the source lacks.

Jobs are saved in an `UploadJobStore` the caller provides, so a failed or
cancelled job can be run again and continue where it stopped::

    from cohere_compass.clients import CompassAsyncClient
    from cohere_compass.ext.uploads import (
        SqliteUploadJobStore,
        UploadOptions,
        create_upload_job,
        discover_files,
        run_upload_job,
        source_type_for,
    )

    options = UploadOptions(
        source_uri="gs://bucket/reports",
        source_type=source_type_for("gs://bucket/reports", directory=True),
        index_name="reports",
        prune=True,
    )
    async with (
        SqliteUploadJobStore("/var/lib/my-app/uploads.db") as store,
        CompassAsyncClient(index_url=url, bearer_token=token) as client,
    ):
        job = await create_upload_job(
            options=options,
            files=await discover_files(options),
            store=store,
            api_url=url,
        )
        result = await run_upload_job(job, client=client, store=store)
"""

from cohere_compass.ext.uploads.exceptions import SourceError, UploadStateError
from cohere_compass.ext.uploads.identity import content_identity
from cohere_compass.ext.uploads.models import (
    RETRYABLE_STATES,
    SUCCESSFUL_TERMINAL_STATES,
    TERMINAL_STATES,
    UNSUCCESSFUL_TERMINAL_STATES,
    FileRecord,
    FileStatus,
    SourceFile,
    SourceType,
    UploadJob,
    UploadOptions,
    UploadOutcome,
)
from cohere_compass.ext.uploads.polling import PollResult, poll_upload_status, wait_for_ingestion
from cohere_compass.ext.uploads.pruning import PruneResult, find_stale_documents, prune_index
from cohere_compass.ext.uploads.reporting import UploadReporter
from cohere_compass.ext.uploads.service import (
    DRY_RUN_JOB_ID,
    UploadResult,
    create_upload_job,
    run_upload_job,
)
from cohere_compass.ext.uploads.sources import (
    content_type_for,
    discover_files,
    is_gcs_source,
    read_source_bytes,
    source_type_for,
)
from cohere_compass.ext.uploads.store import (
    FileUploadJobStore,
    InMemoryUploadJobStore,
    SqliteUploadJobStore,
    UploadJobStore,
)

__all__ = [
    "DRY_RUN_JOB_ID",
    "RETRYABLE_STATES",
    "SUCCESSFUL_TERMINAL_STATES",
    "TERMINAL_STATES",
    "UNSUCCESSFUL_TERMINAL_STATES",
    "FileRecord",
    "FileStatus",
    "FileUploadJobStore",
    "InMemoryUploadJobStore",
    "PollResult",
    "PruneResult",
    "SourceError",
    "SourceFile",
    "SourceType",
    "SqliteUploadJobStore",
    "UploadJob",
    "UploadJobStore",
    "UploadOptions",
    "UploadOutcome",
    "UploadReporter",
    "UploadResult",
    "UploadStateError",
    "content_identity",
    "content_type_for",
    "create_upload_job",
    "discover_files",
    "find_stale_documents",
    "is_gcs_source",
    "poll_upload_status",
    "prune_index",
    "read_source_bytes",
    "run_upload_job",
    "source_type_for",
    "wait_for_ingestion",
]
