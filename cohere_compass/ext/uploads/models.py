"""Credential-free models of upload jobs, their files, and their options."""

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePath
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from cohere_compass.models.config import ParserConfig

SCHEMA_VERSION = 1

NonEmptyString = Annotated[str, Field(min_length=1)]
NonNegativeInt = Annotated[int, Field(ge=0)]
PositiveFloat = Annotated[float, Field(gt=0, allow_inf_nan=False)]
NonNegativeFloat = Annotated[float, Field(ge=0, allow_inf_nan=False)]


def _utc_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("timestamp must be timezone-aware UTC.")
    return value


UtcDatetime = Annotated[datetime, AfterValidator(_utc_timestamp)]


class SourceType(StrEnum):
    """Whether a source is one file or a directory, on local disk or in GCS."""

    LOCAL_FILE = "local_file"
    LOCAL_DIRECTORY = "local_directory"
    GCS_FILE = "gcs_file"
    GCS_DIRECTORY = "gcs_directory"

    @property
    def is_single_file(self) -> bool:
        """Whether the source is one file."""
        return self in {SourceType.LOCAL_FILE, SourceType.GCS_FILE}

    @property
    def is_directory(self) -> bool:
        """Whether the source is a directory or prefix."""
        return not self.is_single_file


class FileStatus(StrEnum):
    """
    Where a file of an upload job stands.

    `PENDING` is a file not yet started, or, in a dry run, one that would be
    uploaded. The terminal statuses are those in `TERMINAL_STATES`.
    """

    ALREADY_INDEXED = "already_indexed"
    CHECKING = "checking"
    COMPLETED = "completed"
    HASHING = "hashing"
    INGESTION_FAILED = "ingestion_failed"
    PENDING = "pending"
    PROCESSING = "processing"
    SOURCE_CHANGED = "source_changed"
    STATUS_ERROR = "status_error"
    TIMED_OUT = "timed_out"
    UPLOAD_FAILED = "upload_failed"
    UPLOADING = "uploading"

    @property
    def is_successful_terminal(self) -> bool:
        """Whether the file is in the index: uploaded and processed, or already there."""
        return self in SUCCESSFUL_TERMINAL_STATES

    @property
    def is_terminal(self) -> bool:
        """Whether the job is done with the file for this run."""
        return self in TERMINAL_STATES

    @property
    def is_retryable(self) -> bool:
        """Whether running the job again retries the file."""
        return self in RETRYABLE_STATES

    @property
    def is_resumable(self) -> bool:
        """Whether running the job again does more work on the file."""
        return not self.is_terminal or self.is_retryable


SUCCESSFUL_TERMINAL_STATES = frozenset({FileStatus.COMPLETED, FileStatus.ALREADY_INDEXED})
UNSUCCESSFUL_TERMINAL_STATES = frozenset(
    {
        FileStatus.UPLOAD_FAILED,
        FileStatus.INGESTION_FAILED,
        FileStatus.TIMED_OUT,
        FileStatus.STATUS_ERROR,
        FileStatus.SOURCE_CHANGED,
    }
)
TERMINAL_STATES = SUCCESSFUL_TERMINAL_STATES | UNSUCCESSFUL_TERMINAL_STATES
RETRYABLE_STATES = frozenset(
    {
        FileStatus.UPLOAD_FAILED,
        FileStatus.INGESTION_FAILED,
        FileStatus.TIMED_OUT,
        FileStatus.STATUS_ERROR,
    }
)


class UploadModel(BaseModel):
    """Base of the upload models: strict, and rejecting unknown fields."""

    model_config = ConfigDict(extra="forbid", strict=True)


class SourceFile(UploadModel):
    """One file of a source, as discovered."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    uri: NonEmptyString
    relative_path: NonEmptyString
    size: NonNegativeInt | None = None

    @field_validator("uri", mode="before")
    @classmethod
    def strip_uri(cls, value: object) -> object:
        """Remove surrounding whitespace."""
        return value.strip() if isinstance(value, str) else value


class UploadOptions(UploadModel):
    """
    What an upload job uploads, to which index, and how.

    Saved with the job, so a resumed job keeps the options it started with.
    Unlike the other upload models, it converts compatible input, such as a
    list of patterns, a `Path` source, or a `SourceType` value given as a string.
    """

    model_config = ConfigDict(extra="forbid", strict=False, frozen=True)

    source_uri: NonEmptyString
    source_type: SourceType
    index_name: NonEmptyString
    # How many files are uploading or processing at once. A worker stays busy
    # with its file until Compass finishes processing it.
    # Strict, so that `True` is not taken for one worker.
    workers: Annotated[int, Field(gt=0, strict=True)] = 8
    recursive: bool = True
    # POSIX relative-path patterns. Basename patterns such as `*.pdf` match at
    # every depth, and exclusions win over inclusions.
    include_patterns: tuple[NonEmptyString, ...] = ()
    exclude_patterns: tuple[NonEmptyString, ...] = ()
    # How long to wait for Compass to process each file, and how often to ask.
    timeout_seconds: PositiveFloat = 300.0
    poll_interval_seconds: PositiveFloat = 2.0
    # Wait for each file to finish processing; only a single file may skip it.
    wait: bool = True
    # Upload every file, even ones the index already has.
    force: bool = False
    # After uploading, delete every index document that is not one of the files
    # this job uploads, which are the source's files that pass
    # `include_patterns` and `exclude_patterns`. Documents uploaded earlier from
    # files the patterns now leave out are deleted too. Defaulted so that job
    # state saved before pruning existed still loads.
    prune: bool = False
    # Stop each file before uploading it and prune nothing, so the run reports
    # what it would do. A dry-run job is never saved.
    dry_run: bool = False
    parser_config: ParserConfig = Field(default_factory=ParserConfig)

    @field_validator("source_uri", mode="before")
    @classmethod
    def strip_source_uri(cls, value: object) -> object:
        """Accept a path, and remove surrounding whitespace."""
        if isinstance(value, PurePath):
            return str(value)
        return value.strip() if isinstance(value, str) else value

    @field_validator("parser_config")
    @classmethod
    def copy_parser_config(cls, value: ParserConfig) -> ParserConfig:
        """Copy the configuration, so changes to the caller's object do not leak in."""
        return value.model_copy(deep=True)

    @model_validator(mode="after")
    def validate_wait_source(self) -> Self:
        """Reject `wait=False` for a directory, which must wait for each file."""
        if not self.wait and not self.source_type.is_single_file:
            raise ValueError("wait=False is only valid for single-file sources.")
        return self


class FileRecord(UploadModel):
    """The saved progress of one file of a job."""

    model_config = ConfigDict(extra="forbid", strict=True, validate_assignment=True)

    source: SourceFile
    content_sha256: NonEmptyString | None = None
    document_id: NonEmptyString | None = None
    upload_id: NonEmptyString | None = None
    status: FileStatus = FileStatus.PENDING
    attempts: NonNegativeInt = 0
    last_error: NonEmptyString | None = None
    updated_at: UtcDatetime = Field(default_factory=lambda: datetime.now(UTC))


class UploadJob(UploadModel):
    """
    An upload of one source's files, as saved in an `UploadJobStore`.

    `api_url` records the deployment the job uploads to, so a caller can check
    that it resumes the job against the same one. `metadata` holds whatever the
    caller needs to recognise or resume its own jobs, such as the name of the
    credentials it used; the workflow never reads it. Neither holds credentials.
    """

    model_config = ConfigDict(extra="forbid", strict=True, validate_assignment=True)

    schema_version: Literal[1] = SCHEMA_VERSION
    job_id: NonEmptyString
    created_at: UtcDatetime
    updated_at: UtcDatetime
    api_url: NonEmptyString | None = None
    options: UploadOptions
    files: dict[NonEmptyString, FileRecord]
    metadata: dict[NonEmptyString, str] = Field(default_factory=dict)


class UploadOutcome(UploadModel):
    """What one run of a job did with one file."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    source_uri: NonEmptyString
    status: FileStatus
    document_id: NonEmptyString | None = None
    upload_id: NonEmptyString | None = None
    error: NonEmptyString | None = None
    elapsed_seconds: NonNegativeFloat | None = None

    @field_validator("source_uri", mode="before")
    @classmethod
    def strip_source_uri(cls, value: object) -> object:
        """Remove surrounding whitespace."""
        return value.strip() if isinstance(value, str) else value
