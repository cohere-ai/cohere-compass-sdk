# Python imports
import logging
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Literal,
    Optional,
    ClassVar,
    TypedDict,
    TypeVar,
    Generic,
)
from abc import ABC, abstractmethod


# 3rd party imports
from pydantic import BaseModel

from cohere.compass.constants import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_RETRY_SECONDS,
)
from cohere.compass.exceptions import (
    CompassAuthError,
    CompassClientError,
)
from cohere.compass.models.config import IndexConfig
from cohere.compass.models.documents import DocumentAttributes


@dataclass
class RetryResult:
    """
    A class to represent the result of a retryable operation.

    The class contains the following fields:
    - result: The result of the operation if successful, otherwise None.
    - error (Optional[str]): The error message if the operation failed, otherwise None.

    Notice that this is an internal class and should not be exposed to clients.
    """

    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None


logger = logging.getLogger(__name__)


_HttpMethods = Literal["GET", "POST", "PUT", "DELETE"]


class _CompassRequest(TypedDict):
    max_retries: int
    sleep_retry_seconds: int
    method: str
    data: dict[str, Any] | None
    headers: dict[str, str] | None


T = TypeVar("T", RetryResult, Awaitable[RetryResult])


class BaseCompassClient(ABC, Generic[T]):
    _API_METHODS: ClassVar[dict[str, _HttpMethods]] = {
        "create_index": "PUT",
        "list_indexes": "GET",
        "delete_index": "DELETE",
        "delete_document": "DELETE",
        "get_document": "GET",
        "put_documents": "PUT",
        "search_documents": "POST",
        "search_chunks": "POST",
        "add_attributes": "POST",
        "refresh": "POST",
        "upload_documents": "POST",
        "update_group_authorization": "POST",
        # Data Sources APIs
        "create_datasource": "POST",
        "list_datasources": "GET",
        "delete_datasources": "DELETE",
        "get_datasource": "GET",
        "sync_datasource": "POST",
        "list_datasources_objects_states": "GET",
    }
    _API_ENDPOINTS: ClassVar[dict[str, str]] = {
        "create_index": "/api/v1/indexes/{index_name}",
        "list_indexes": "/api/v1/indexes",
        "delete_index": "/api/v1/indexes/{index_name}",
        "delete_document": "/api/v1/indexes/{index_name}/documents/{document_id}",
        "get_document": "/api/v1/indexes/{index_name}/documents/{document_id}",
        "put_documents": "/api/v1/indexes/{index_name}/documents",
        "search_documents": "/api/v1/indexes/{index_name}/documents/_search",
        "search_chunks": "/api/v1/indexes/{index_name}/documents/_search_chunks",
        "add_attributes": "/api/v1/indexes/{index_name}/documents/{document_id}/_add_attributes",  # noqa: E501
        "refresh": "/api/v1/indexes/{index_name}/_refresh",
        "upload_documents": "/api/v1/indexes/{index_name}/documents/_upload",
        "update_group_authorization": "/api/v1/indexes/{index_name}/group_authorization",  # noqa: E501
        # Data Sources APIs
        "create_datasource": "/api/v1/datasources",
        "list_datasources": "/api/v1/datasources",
        "delete_datasources": "/api/v1/datasources/{datasource_id}",
        "get_datasource": "/api/v1/datasources/{datasource_id}",
        "sync_datasource": "/api/v1/datasources/{datasource_id}/_sync",
        "list_datasources_objects_states": "/api/v1/datasources/{datasource_id}/documents?skip={skip}&limit={limit}",  # noqa: E501
    }

    def __init__(
        self,
        *,
        index_url: str,
        bearer_token: Optional[str] = None,
        default_max_retries: int = DEFAULT_MAX_RETRIES,
        default_sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ):
        self.index_url = index_url
        self.bearer_token = bearer_token

        if default_max_retries < 0:
            raise ValueError("default_max_retries must be a non-negative integer.")
        if default_sleep_retry_seconds < 0:
            raise ValueError(
                "default_sleep_retry_seconds must be a non-negative integer."
            )
        self.default_max_retries = default_max_retries
        self.default_sleep_retry_seconds = default_sleep_retry_seconds

    def create_index(
        self,
        *,
        index_name: str,
        index_config: Optional[IndexConfig] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Create an index in Compass.

        :param index_name: the name of the index
        :param index_config: the optional configuration for the index
        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="create_index",
            index_name=index_name,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            data=index_config,
        )

    def refresh_index(
        self,
        *,
        index_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Refresh index.

        :param index_name: the name of the index
        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="refresh",
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

    def delete_index(
        self,
        *,
        index_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Delete an index from Compass.

        :param index_name: the name of the index
        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="delete_index",
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

    def delete_document(
        self,
        *,
        index_name: str,
        document_id: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Delete a document from Compass.

        :param index_name: the name of the index
        :param document_id: the id of the document

        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="delete_document",
            document_id=document_id,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

    def get_document(
        self,
        *,
        index_name: str,
        document_id: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Get a document from Compass.

        :param index_name: the name of the index
        :param document_id: the id of the document

        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="get_document",
            document_id=document_id,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

    def list_indexes(
        self,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        List all indexes in Compass.

        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="list_indexes",
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name="",
        )

    def _add_attributes(
        self,
        *,
        index_name: str,
        document_id: str,
        attributes: DocumentAttributes,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> T:
        """
        Update the content field of an existing document with additional context.

        :param index_name: the name of the index
        :param document_id: the document to modify
        :param attributes: the attributes to add to the document
        :param max_retries: the maximum number of times to retry a doc insertion
        :param sleep_retry_seconds: number of seconds to go to sleep before retrying a
            doc insertion

        :returns: an error message if the request failed, otherwise None
        """
        return self._send_request(
            api_name="add_attributes",
            document_id=document_id,
            data=attributes,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

    def _create_request(
        self,
        api_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
        data: Optional[BaseModel] = None,
        **url_params: str,
    ) -> _CompassRequest:
        max_retries = max_retries or self.default_max_retries
        sleep_retry_seconds = sleep_retry_seconds or self.default_sleep_retry_seconds
        if max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer.")
        if sleep_retry_seconds < 0:
            raise ValueError("sleep_retry_seconds must be a non-negative integer.")

        data_dict = data.model_dump(mode="json", exclude_none=True) if data else None

        headers = None
        if self.bearer_token:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

        method = self._API_METHODS[api_name]

        return {
            "max_retries": max_retries,
            "sleep_retry_seconds": sleep_retry_seconds,
            "data": data_dict,
            "headers": headers,
            "method": method,
        }

    def _get_api_path(self, api_name: str, **url_params: str) -> str:
        return self._API_ENDPOINTS[api_name].format(**url_params)

    def _handle_http_error(
        self,
        status_code: int,
        message: str,
        exc: Exception,
        api_name: str,
        target_path: str,
        sleep_retry_seconds: Optional[int] = None,
    ):
        if status_code == 401:
            error = "Unauthorized. Please check your bearer token."
            raise CompassAuthError(message=str(exc))
        elif 400 <= status_code < 500:
            error = f"Client error occurred: {message}"
            raise CompassClientError(message=error, code=status_code)
        else:
            error = str(exc) + " " + message
            logger.error(
                f"Failed to send request to {api_name} {target_path}: "
                f"{type(exc)} {error}. Going to sleep for "
                f"{sleep_retry_seconds} seconds and retrying."
            )
            raise exc

    def _handle_retry_result(
        self, result: RetryResult | None, error: str | None
    ) -> RetryResult:
        if result:
            return result
        return RetryResult(result=None, error=error)

    def _handle_retry_error(
        self, error: str | None, max_retries: int | None
    ) -> RetryResult:
        logger.error(f"Failed to send request after {max_retries} attempts. Aborting.")
        return RetryResult(result=None, error=error)

    @abstractmethod
    def _send_request(
        self,
        api_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
        data: Optional[BaseModel] = None,
        **url_params: str,
    ) -> T: ...
