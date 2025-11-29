"""Utility functions related to async for the Cohere Compass SDK."""

# Python imports
import asyncio
import logging

# 3rd party imports
# Local imports
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import TypeVar

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")

logger = logging.getLogger(__name__)


async def async_enumerate(
    iterable: AsyncIterable[T], start: int = 0
) -> AsyncIterator[tuple[int, T]]:
    """
    Enumerate an async iterable, just like Python's `enumerate()` but for async.

    :param iterable: The iterable to enumerate.
    :param start: The start index.

    :return: An async iterator of tuples of the index and the item.
    """
    i = start
    async for item in iterable:
        yield i, item
        i += 1


async def async_map(
    func: Callable[[T], Awaitable[R]],
    iterable: Iterable[T],
    limit: int | None = None,
) -> list[R]:
    """
    Run an async function over an iterable with a limit on concurrent executions.

    The function preserves the original order of results.

    :param func: An async function to apply to each item.
    :param iterable: An iterable of input items.
    :param limit: Maximum number of concurrent tasks.

    :return: A list of results, in the same order as the input iterable.

    """
    results: dict[int, R] = {}

    if limit is not None:
        semaphore = asyncio.Semaphore(limit)

        async def _task(idx: int, item: T):
            async with semaphore:
                results[idx] = await func(item)
    else:

        async def _task(idx: int, item: T):
            results[idx] = await func(item)

    tasks = [_task(idx, item) for idx, item in enumerate(iterable)]
    await asyncio.gather(*tasks)

    return [results[i] for i in sorted(results)]


async def async_apply(
    func: Callable[..., Awaitable[None]],
    iterable: Iterable[T] | AsyncIterable[T],
    limit: int | None = None,
) -> None:
    """
    Apply an async function over an iterable with a limit on concurrent executions.

    :param func: An async function to apply to the arguments.
    :param iterable: One or more iterables whose elements will be passed to func.
    :param limit: Maximum number of concurrent tasks.
    """
    if limit is not None:
        semaphore = asyncio.Semaphore(limit)

        async def _task(item: T):
            async with semaphore:
                await func(item)
    else:

        async def _task(item: T):
            await func(item)

    if isinstance(iterable, AsyncIterable):
        tasks = [_task(item) async for item in iterable]
    else:
        tasks = [_task(item) for item in iterable]
    await asyncio.gather(*tasks)
