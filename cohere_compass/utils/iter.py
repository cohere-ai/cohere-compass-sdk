from collections.abc import Callable, Iterable
from concurrent import futures
from concurrent.futures import Executor
from typing import TypeVar

from cohere_compass.utils.documents import logger

T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


def imap_parallel(
    executor: Executor, f: Callable[[T], U], it: Iterable[T], max_parallelism: int
):
    """
    Map a function over an iterable using parallel execution with parallelism limit.

    Similar to Python's built-in map(), but uses an executor to parallelize
    the function calls and limits the number of concurrent futures.

    :param executor: The executor to use for parallel execution.
    :param f: The function to apply to each item.
    :param it: The iterable to map over.
    :param max_parallelism: Maximum number of futures to keep in flight.

    :return: Yields results from applying f to each item in it.

    :raises ValueError: If max_parallelism is less than 1.

    """
    if max_parallelism < 1:
        raise ValueError("max_parallelism must be at least 1")
    futures_set: set[futures.Future[U]] = set()

    for x in it:
        futures_set.add(executor.submit(f, x))
        while len(futures_set) > max_parallelism:
            done, futures_set = futures.wait(
                futures_set, return_when=futures.FIRST_COMPLETED
            )

            for future in done:
                try:
                    yield future.result()
                except Exception as e:
                    logger.exception(f"Error in processing file: {e}")

    for future in futures.as_completed(futures_set):
        try:
            yield future.result()
        except Exception as e:
            logger.exception(f"Error in processing file: {e}")
