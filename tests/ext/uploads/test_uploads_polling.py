import uuid
from collections.abc import Callable, Sequence
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import BaseModel, ConfigDict

from cohere_compass.clients import CompassAsyncClient
from cohere_compass.ext.uploads.models import FileStatus
from cohere_compass.ext.uploads.polling import (
    _TERMINAL_UPLOAD_STATES,  # type: ignore
    PollResult,
    StatusSnapshot,
    poll_upload_status,
    wait_for_ingestion,
)

Handler = Callable[[httpx.Request], httpx.Response]


class Status(BaseModel):
    model_config = ConfigDict(strict=True)

    document_id: str
    state: str | None
    last_error: str | None = None


class Client:
    def __init__(self, responses: Sequence[Sequence[Status] | None | Exception]) -> None:
        self.responses = iter(responses)
        self.calls: list[tuple[str, UUID]] = []

    async def upload_document_status(self, *, index_name: str, upload_id: UUID) -> Sequence[Status] | None:
        self.calls.append((index_name, upload_id))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class Clock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


async def _wait_for_ingestion(
    client: CompassAsyncClient,
    responses: Sequence[str] = ("document",),
    *,
    timeout_seconds: float = 5,
    interval: float = 1,
) -> tuple[PollResult, Clock]:
    clock = Clock()
    result = await wait_for_ingestion(
        client,
        "docs",
        str(uuid4()),
        responses,
        timeout_seconds,
        interval,
        _clock=clock,
        _sleep=clock.sleep,
    )
    return result, clock


def test_wait_accepts_sdk_client() -> None:
    """The real SDK client satisfies the signature; the coroutine is never awaited."""
    client = CompassAsyncClient(index_url="https://api.example", bearer_token="token")
    coroutine = wait_for_ingestion(client, "docs", str(uuid4()), ("document",), 1, 1)
    coroutine.close()


@pytest.mark.asyncio
async def test_wait_completes_after_configured_interval() -> None:
    result, clock = await _wait_for_ingestion(
        cast(
            CompassAsyncClient,
            Client([[Status(document_id="document", state="completed")]]),
        )
    )
    assert result == PollResult(status=FileStatus.COMPLETED)
    assert clock.sleeps == [1]


@pytest.mark.asyncio
async def test_wait_handles_pending_mixed_and_lowercase_states() -> None:
    result, clock = await _wait_for_ingestion(
        cast(
            CompassAsyncClient,
            Client(
                [
                    [Status(document_id="document", state="pending")],
                    [
                        Status(document_id="document", state="COMPLETED"),
                        Status(document_id="other", state="failed"),
                    ],
                ]
            ),
        )
    )
    assert result.status is FileStatus.COMPLETED
    assert clock.sleeps == [1, 1]


@pytest.mark.parametrize("state", [None, "", "processing"])
@pytest.mark.asyncio
async def test_wait_keeps_empty_or_nonterminal_states_pending(
    state: str | None,
) -> None:
    result, _ = await _wait_for_ingestion(
        cast(
            CompassAsyncClient,
            Client([[Status(document_id="document", state=state)]]),
        ),
        timeout_seconds=1,
        interval=0.5,
    )
    assert result.status is FileStatus.TIMED_OUT


@pytest.mark.parametrize("statuses", [None, [Status(document_id="other", state="completed")]])
@pytest.mark.asyncio
async def test_wait_keeps_missing_or_unrelated_document_statuses_pending(
    statuses: Sequence[Status] | None,
) -> None:
    result, _ = await _wait_for_ingestion(cast(CompassAsyncClient, Client([statuses])), timeout_seconds=1, interval=0.5)
    assert result.status is FileStatus.TIMED_OUT


@pytest.mark.asyncio
async def test_wait_requires_all_expected_documents_and_reports_failures() -> None:
    result, _ = await _wait_for_ingestion(
        cast(
            CompassAsyncClient,
            Client(
                [
                    [
                        Status(document_id="one", state="completed"),
                        Status(
                            document_id="two",
                            state="failed",
                            last_error="parser error",
                        ),
                    ]
                ]
            ),
        ),
        ("one", "two"),
    )
    assert result == PollResult(status=FileStatus.INGESTION_FAILED, error="parser error")


@pytest.mark.asyncio
async def test_wait_falls_back_to_a_useful_failure_message() -> None:
    result, _ = await _wait_for_ingestion(
        cast(
            CompassAsyncClient,
            Client([[Status(document_id="document", state="FAILED")]]),
        )
    )
    assert result == PollResult(
        status=FileStatus.INGESTION_FAILED,
        error="Document document ingestion failed.",
    )


@pytest.mark.asyncio
async def test_wait_times_out_at_deadline_without_polling() -> None:
    client = Client([])
    result, clock = await _wait_for_ingestion(cast(CompassAsyncClient, client), timeout_seconds=1, interval=2)
    assert result.status is FileStatus.TIMED_OUT
    assert clock.sleeps == [1]
    assert client.calls == []


@pytest.mark.asyncio
async def test_wait_times_out_when_deadline_elapses_during_a_slow_poll_call() -> None:
    clock = Clock()

    class SlowClient(Client):
        async def upload_document_status(self, *, index_name: str, upload_id: UUID) -> Sequence[Status] | None:
            clock.value += 100
            return await super().upload_document_status(index_name=index_name, upload_id=upload_id)

    client = SlowClient([[Status(document_id="document", state="pending")]])
    result = await wait_for_ingestion(
        cast(CompassAsyncClient, client),
        "docs",
        str(uuid4()),
        ("document",),
        5,
        1,
        _clock=clock,
        _sleep=clock.sleep,
    )
    assert result.status is FileStatus.TIMED_OUT


@pytest.mark.asyncio
async def test_wait_validates_inputs_and_propagates_client_errors() -> None:
    client = Client([[Status(document_id="document", state="completed")]])
    with pytest.raises(ValueError, match="upload_id"):
        await wait_for_ingestion(
            cast(CompassAsyncClient, client),
            "docs",
            "not-a-uuid",
            ("document",),
            1,
            1,
        )
    with pytest.raises(ValueError, match="timeout_seconds"):
        await wait_for_ingestion(
            cast(CompassAsyncClient, client),
            "docs",
            str(uuid4()),
            ("document",),
            0,
            1,
        )
    with pytest.raises(ValueError, match="poll_interval_seconds"):
        await wait_for_ingestion(
            cast(CompassAsyncClient, client),
            "docs",
            str(uuid4()),
            ("document",),
            1,
            0,
        )
    with pytest.raises(ValueError, match="expected_document_ids"):
        await wait_for_ingestion(cast(CompassAsyncClient, client), "docs", str(uuid4()), (), 1, 1)
    with pytest.raises(RuntimeError, match="network"):
        await _wait_for_ingestion(cast(CompassAsyncClient, Client([RuntimeError("network")])))


def _status(state: str | None, *, document_id: str = "doc-1") -> SimpleNamespace:
    return SimpleNamespace(
        state=state,
        document_id=document_id,
        last_error="boom" if state == "FAILED" else None,
    )


def _client(*responses: object) -> Mock:
    client = Mock(spec=CompassAsyncClient)
    if len(responses) == 1:
        client.upload_document_status.return_value = responses[0]
    else:
        client.upload_document_status.side_effect = list(responses)
    return client


@pytest.mark.asyncio
async def test_poll_upload_status_yields_one_snapshot_per_poll() -> None:
    client = _client([_status("PROCESSING")], [_status("COMPLETED")])
    clock = Clock()

    snapshots = [
        snapshot
        async for snapshot in poll_upload_status(
            client,
            index_name="idx",
            upload_id=uuid.uuid4(),
            poll_interval=1,
            timeout=2.5,
            _clock=clock,
            _sleep=clock.sleep,
        )
    ]

    assert [snapshot.summary for snapshot in snapshots] == [
        "PROCESSING=1",
        "COMPLETED=1",
    ]
    # Two full intervals, then a final interval clamped to the deadline before
    # the generator stops.
    assert clock.sleeps == [1, 1, 0.5]


@pytest.mark.asyncio
async def test_poll_upload_status_clamps_the_last_sleep_to_the_deadline() -> None:
    client = _client([_status("PROCESSING")])
    clock = Clock()

    async for _snapshot in poll_upload_status(
        client,
        index_name="idx",
        upload_id=uuid.uuid4(),
        poll_interval=10,
        timeout=4,
        _clock=clock,
        _sleep=clock.sleep,
    ):
        pass

    assert clock.sleeps == [4]


def test_snapshot_scopes_settlement_and_failure_to_expected_ids() -> None:
    snapshot = StatusSnapshot(
        states={"a": "COMPLETED", "b": "PROCESSING", "c": "FAILED"},
        errors={"a": None, "b": None, "c": "boom"},
    )

    assert snapshot.is_settled(frozenset({"a"})) is True
    assert snapshot.is_settled(frozenset({"a", "b"})) is False
    assert snapshot.is_settled() is False
    assert snapshot.failed_ids(frozenset({"a", "c"})) == ["c"]


def test_snapshot_normalises_wire_states() -> None:
    snapshot = StatusSnapshot(states={"a": "COMPLETED"}, errors={"a": None})

    assert snapshot.terminal_ids() == frozenset({"a"})
    assert snapshot.summary == "COMPLETED=1"


def test_terminal_upload_states_contains_completed_and_failed() -> None:
    assert _TERMINAL_UPLOAD_STATES == {"COMPLETED", "FAILED"}
