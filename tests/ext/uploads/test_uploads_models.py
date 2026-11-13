from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from cohere_compass.ext.uploads.models import (
    FileRecord,
    FileStatus,
    SourceFile,
    SourceType,
    UploadJob,
    UploadOptions,
)
from cohere_compass.models.config import ParserConfig


def options() -> UploadOptions:
    return UploadOptions(
        source_uri="gs://bucket/report.pdf",
        source_type=SourceType.GCS_FILE,
        index_name="reports",
        workers=2,
        recursive=False,
        include_patterns=("*.pdf",),
        exclude_patterns=(),
        timeout_seconds=30,
        poll_interval_seconds=2,
        wait=True,
        force=False,
    )


def test_models_are_strict_and_frozen_where_appropriate() -> None:
    source = SourceFile(uri=" /tmp/report.pdf ", relative_path="report.pdf", size=10)
    assert source.uri == "/tmp/report.pdf"
    with pytest.raises(ValidationError):
        SourceFile.model_validate({"uri": "a", "relative_path": "a", "extra": 1})
    with pytest.raises(ValidationError):
        UploadOptions.model_validate({**options().model_dump(), "workers": True})
    with pytest.raises(ValidationError):
        source.uri = "other"


def test_upload_models_json_round_trip() -> None:
    now = datetime.now(UTC)
    job = UploadJob(
        job_id="job",
        created_at=now,
        updated_at=now,
        api_url="https://api.example",
        options=options(),
        files={"/tmp/report.pdf": FileRecord(source=SourceFile(uri="/tmp/report.pdf", relative_path="report.pdf"))},
    )

    restored = UploadJob.model_validate(job.model_dump())

    assert restored.model_dump(mode="json") == job.model_dump(mode="json")


def test_options_persist_sdk_parser_config() -> None:
    config = ParserConfig.model_validate({"pdf_parsing_strategy": "Smart"})
    configured = options().model_copy(update={"parser_config": config})

    restored = UploadOptions.model_validate(configured.model_dump())

    assert restored.parser_config.model_dump(mode="json") == config.model_dump(mode="json")


def test_options_defensively_copy_mutable_parser_config() -> None:
    config = ParserConfig.model_validate({"num_tokens_per_chunk": 300})
    configured = UploadOptions(
        **options().model_dump(exclude={"parser_config"}),
        parser_config=config,
    )

    config.num_tokens_per_chunk = 400

    assert configured.parser_config.num_tokens_per_chunk == 300


def test_file_status_terminal_properties() -> None:
    assert FileStatus.COMPLETED.is_successful_terminal
    assert FileStatus.UPLOAD_FAILED.is_terminal and FileStatus.UPLOAD_FAILED.is_retryable
    assert not FileStatus.SOURCE_CHANGED.is_resumable


def test_source_type_is_directory() -> None:
    assert SourceType.LOCAL_DIRECTORY.is_directory
    assert SourceType.GCS_DIRECTORY.is_directory
    assert not SourceType.LOCAL_FILE.is_directory


def test_utc_datetime_rejects_naive_or_non_utc_timestamps() -> None:
    from datetime import timedelta, timezone

    with pytest.raises(ValidationError):
        FileRecord(
            source=SourceFile(uri="a", relative_path="a"),
            updated_at=datetime(2024, 1, 1),  # Intentionally naive.
        )
    with pytest.raises(ValidationError):
        FileRecord(
            source=SourceFile(uri="a", relative_path="a"),
            updated_at=datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        )


def test_upload_options_rejects_no_wait_for_directory_sources() -> None:
    with pytest.raises(ValidationError, match="single-file"):
        UploadOptions.model_validate(
            {
                **options().model_dump(),
                "source_type": SourceType.GCS_DIRECTORY,
                "wait": False,
            }
        )


def test_options_accept_ordinary_caller_input() -> None:
    configured = UploadOptions(
        source_uri=Path("/data"),  # pyright: ignore[reportArgumentType]
        source_type="local_directory",  # pyright: ignore[reportArgumentType]
        index_name="docs",
        include_patterns=["*.pdf"],  # pyright: ignore[reportArgumentType]
        timeout_seconds=60,
    )

    assert configured.source_uri == "/data"
    assert configured.source_type is SourceType.LOCAL_DIRECTORY
    assert configured.include_patterns == ("*.pdf",)
    assert configured.timeout_seconds == 60.0
