"""
Client classes for the Cohere Compass SDK.

This module exports all client classes for interacting with Compass APIs:
- CompassClient: Synchronous client for index operations
- CompassAsyncClient: Asynchronous client for index operations
- CompassParserClient: Synchronous client for parsing operations
- CompassParserAsyncClient: Asynchronous client for parsing operations
- CompassRootClient: RBAC access control client
"""

from cohere_compass.clients.access_control import CompassRootClient
from cohere_compass.clients.compass import CompassClient
from cohere_compass.clients.compass_async import CompassAsyncClient
from cohere_compass.clients.parser import CompassParserClient
from cohere_compass.clients.parser_async import CompassParserAsyncClient

__all__ = [
    "CompassAsyncClient",
    "CompassClient",
    "CompassParserAsyncClient",
    "CompassParserClient",
    "CompassRootClient",
]
