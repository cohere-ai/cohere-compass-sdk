"""Polling Compass for the ingestion status of submitted uploads."""

import asyncio
import math
import uuid
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from time import monotonic

from pydantic import BaseModel, ConfigDict

from cohere_compass.clients import CompassAsyncClient
from cohere_compass.ext.uploads.models import FileStatus
from cohere_compass.models import UploadDocumentsStatus

MonotonicClock = Callable[[], float]
AsyncSleep = Callable[[float], Awaitable[object]]

_TERMINAL_UPLOAD_STATES: frozenset[str] = frozenset({"COMPLETED", "FAILED"})


class PollResult(BaseModel):
    """The final status of a file after polling its upload."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: FileStatus
    error: str | None = None


class StatusSnapshot(BaseModel):
    """One poll's worth of upload status, reduced to what every caller needs."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    states: dict[str, str]
    errors: dict[str, str | None]

    @property
    def summary(self) -> str:
        """A sorted `STATE=N` tally, e.g. `COMPLETED=1 FAILED=2`."""
        return _state_summary(_count_by_state(self.states.values()))

    def terminal_ids(self) -> frozenset[str]:
        """Return the IDs of documents that finished processing, successfully or not."""
        return frozenset(document_id for document_id, state in self.states.items() if state in _TERMINAL_UPLOAD_STATES)

    def is_settled(self, expected_ids: frozenset[str] | None = None) -> bool:
        """
        Whether every document of interest reached a terminal state.

        An empty status list is **not** settled: the server has not listed
        any document yet, so nothing can have finished.
        """
        if expected_ids is None:
            return bool(self.states) and self.terminal_ids() == frozenset(self.states)
        return expected_ids <= self.terminal_ids()

    def failed_ids(self, expected_ids: frozenset[str] | None = None) -> list[str]:
        """Return the IDs of documents that failed, among *expected_ids* if given."""
        scope = self.states if expected_ids is None else expected_ids
        return [document_id for document_id in scope if self.states.get(document_id) == "FAILED"]


def _failure_message(snapshot: StatusSnapshot, failed_ids: Sequence[str]) -> str:
    """One message per failed document, falling back when the server sent none."""
    errors = [
        error.strip()
        if (error := snapshot.errors.get(document_id)) is not None and error.strip()
        else f"Document {document_id} ingestion failed."
        for document_id in failed_ids
    ]
    return "; ".join(errors)


def _normalize_state(state: str | None) -> str:
    """Uppercase and strip a wire state string; `""` for `None`."""
    return state.strip().upper() if state is not None else ""


def _count_by_state(states: Iterable[str]) -> dict[str, int]:
    """Tally occurrences of each state string."""
    counts: dict[str, int] = {}
    for state in states:
        counts[state] = counts.get(state, 0) + 1
    return counts


def _state_summary(counts: Mapping[str, int]) -> str:
    """Render *counts* as a sorted `STATE=N` summary, e.g. `COMPLETED=1 FAILED=2`."""
    return " ".join(f"{state}={count}" for state, count in sorted(counts.items()))


def _snapshot(statuses: Sequence[UploadDocumentsStatus] | None) -> StatusSnapshot:
    if statuses is None:
        return StatusSnapshot(states={}, errors={})
    return StatusSnapshot(
        states={status.document_id: _normalize_state(status.state) for status in statuses},
        errors={status.document_id: status.last_error for status in statuses},
    )


async def poll_upload_status(
    client: CompassAsyncClient,
    *,
    index_name: str,
    upload_id: uuid.UUID,
    poll_interval: float,
    timeout: float,
    _clock: MonotonicClock = monotonic,
    _sleep: AsyncSleep = asyncio.sleep,
) -> AsyncGenerator[StatusSnapshot, None]:
    """
    Yield one snapshot per poll until the deadline passes, then stop.

    The generator ending without a caller having decided is exactly a
    timeout; each adapter chooses how to report that.
    """
    deadline = _clock() + timeout
    while True:
        remaining = deadline - _clock()
        if remaining <= 0:
            return
        await _sleep(min(poll_interval, remaining))
        if _clock() >= deadline:
            return
        statuses = await client.upload_document_status(index_name=index_name, upload_id=upload_id)
        yield _snapshot(statuses)


async def wait_for_ingestion(
    client: CompassAsyncClient,
    index_name: str,
    upload_id: str,
    expected_document_ids: Sequence[str],
    timeout_seconds: float,
    poll_interval_seconds: float,
    *,
    _clock: MonotonicClock = monotonic,
    _sleep: AsyncSleep = asyncio.sleep,
) -> PollResult:
    """Wait until every expected document reaches an ingestion terminal state."""
    _validate_positive_number(timeout_seconds, "timeout_seconds")
    _validate_positive_number(poll_interval_seconds, "poll_interval_seconds")
    if not expected_document_ids or any(not item for item in expected_document_ids):
        raise ValueError("expected_document_ids must contain non-empty strings.")
    expected_ids = frozenset(expected_document_ids)
    try:
        parsed_upload_id = uuid.UUID(upload_id)
    except ValueError as error:
        raise ValueError("upload_id must be a valid UUID string.") from error

    snapshots = poll_upload_status(
        client,
        index_name=index_name,
        upload_id=parsed_upload_id,
        poll_interval=poll_interval_seconds,
        timeout=timeout_seconds,
        _clock=_clock,
        _sleep=_sleep,
    )
    async for snapshot in snapshots:
        if not snapshot.is_settled(expected_ids):
            continue
        failed = snapshot.failed_ids(expected_ids)
        if not failed:
            return PollResult(status=FileStatus.COMPLETED)
        return PollResult(
            status=FileStatus.INGESTION_FAILED,
            error=_failure_message(snapshot, failed),
        )
    return PollResult(status=FileStatus.TIMED_OUT)


def _validate_positive_number(value: float, name: str) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive, finite number.")
