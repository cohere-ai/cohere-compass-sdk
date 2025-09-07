import asyncio
from typing import Any

from cohere_compass.clients.compass_async import CompassAsyncClient


class SyncifiedCompassAsyncClient:
    """
    A synchronous wrapper around CompassAsyncClient that converts async methods to sync.
    This class provides a synchronous interface to the Compass API by wrapping
    the async client and running async methods in a controlled event loop.

    Example:
        >>> client = SyncifiedCompassAsyncClient(index_url="https://api.example.com")
        >>> indexes = client.list_indexes()  # Returns sync result
        >>> client.close()  # Clean up resources

    This class is useful for testing purposes. Instead of having to repeat the same test
    code in sync for async code, we can pass in an instance of this class to a sync
    test code. With PyTest fixtures, this means the same exact code can be used to test
    both clients.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        self.client = CompassAsyncClient(*args, **kwargs)

    def close(self):
        asyncio.run(self.client.aclose())

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        self.close()


def _syncify_method(method: str):
    def wrapper(self: SyncifiedCompassAsyncClient, *args: Any, **kwargs: Any):
        # Ideally, we should avoid repeatedly calling asyncio.run() as it is expensive
        # because it creates a new event loop. However, since this client is used for
        # testing purposes, it doesn't really matter.
        return asyncio.run(getattr(self.client, method)(*args, **kwargs))

    return wrapper


# Define all the methods of CompassAsyncClient as sync methods
for method in dir(CompassAsyncClient):
    if method.startswith("_"):
        continue

    print(f"Syncifying method: {method}")
    setattr(SyncifiedCompassAsyncClient, method, _syncify_method(method))
