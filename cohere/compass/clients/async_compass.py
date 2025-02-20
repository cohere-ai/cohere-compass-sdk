# Python imports

import logging


from typing import Any, Coroutine, Literal, Optional

from aiohttp.client_exceptions import ClientResponseError

# 3rd party imports
# TODO find stubs for joblib and remove "type: ignore"
from pydantic import BaseModel
from requests.exceptions import InvalidSchema
from tenacity import (
    RetryError,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_fixed,
)
import aiohttp

# Local imports

from cohere.compass.clients.base import BaseCompassClient, RetryResult
from cohere.compass.constants import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_RETRY_SECONDS,
)
from cohere.compass.exceptions import (
    CompassClientError,
    CompassError,
)
from cohere.compass.models import (
    SearchDocumentsResponse,
    SearchFilter,
    SearchInput,
)
from cohere.compass.models.documents import DocumentAttributes


logger = logging.getLogger(__name__)


class AsyncCompassClient(BaseCompassClient[Coroutine[Any, Any, RetryResult]]):
    def __init__(
        self,
        *,
        index_url: str,
        bearer_token: Optional[str] = None,
        http_session: Optional[aiohttp.ClientSession] = None,
        default_max_retries: int = DEFAULT_MAX_RETRIES,
        default_sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ):
        """
        Initialize the Compass client.

        :param index_url: The base URL for the index API.
        :param bearer_token (optional): The bearer token for authentication.
        :param http_session (optional): An optional HTTP session to use for requests.
        """
        super().__init__(
            index_url=index_url,
            bearer_token=bearer_token,
            default_max_retries=default_max_retries,
            default_sleep_retry_seconds=default_sleep_retry_seconds,
        )
        self.session = http_session or aiohttp.ClientSession()

    async def add_attributes(
        self,
        *,
        index_name: str,
        document_id: str,
        attributes: DocumentAttributes,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
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
        result = await self._add_attributes(
            index_name=index_name,
            document_id=document_id,
            attributes=attributes,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )
        if result.error:
            return result.error
        return None

    async def search_documents(
        self,
        *,
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: Optional[list[SearchFilter]] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> SearchDocumentsResponse:
        """
        Search documents in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of documents to return
        :param filters: the search filters to apply

        :returns: the search results
        """
        result = await self._search(
            api_name="search_documents",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            raise CompassError(result.error)

        return SearchDocumentsResponse.model_validate(result.result)

    async def _search(
        self,
        *,
        api_name: Literal["search_documents", "search_chunks"],
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: Optional[list[SearchFilter]] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> RetryResult:
        return await self._send_request(
            api_name=api_name,
            index_name=index_name,
            data=SearchInput(query=query, top_k=top_k, filters=filters),
            max_retries=1,
            sleep_retry_seconds=1,
        )

    async def _send_request(
        self,
        api_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
        data: Optional[BaseModel] = None,
        **url_params: str,
    ) -> RetryResult:
        """
        Send a request to the Compass API.

        :param function: the function to call
        :param index_name: the name of the index
        :param max_retries: the number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        :param data: the data to send
        :returns: An error message if the request failed, otherwise None.
        """

        compass_request = self._create_request(
            api_name=api_name,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            data=data,
            **url_params,
        )

        @retry(
            stop=stop_after_attempt(compass_request["max_retries"]),
            wait=wait_fixed(compass_request["sleep_retry_seconds"]),
            retry=retry_if_not_exception_type(
                (
                    CompassClientError,
                    InvalidSchema,
                )
            ),
        )
        async def _send_request_with_retry():
            nonlocal error

            try:
                response = await self.session.request(
                    method=compass_request["method"],
                    url=target_path,
                    json=compass_request["data"],
                    headers=compass_request["headers"],
                )

                if response.ok:
                    error = None
                    text = await response.text()
                    result = await response.json() if text else None
                    return RetryResult(result=result, error=None)
                else:
                    response.raise_for_status()

            except ClientResponseError as e:
                self._handle_http_error(
                    e.status,
                    e.message,
                    e,
                    api_name,
                    target_path,
                    sleep_retry_seconds,
                )

            except ConnectionAbortedError as e:
                raise CompassClientError(message=str(e), code=None)

            except Exception as e:
                error = str(e)
                logger.error(
                    f"Failed to send request to {api_name} {target_path}: {type(e)} "
                    f"{error}. Sleeping for {sleep_retry_seconds} before retrying..."
                )
                raise e

        error = None
        try:
            target_path = self._get_target_path(api_name, **url_params)
            res = await _send_request_with_retry()
            return self._handle_retry_result(res, error)
        except RetryError:
            logger.error(
                f"Failed to send request after {max_retries} attempts. Aborting."
            )
            return self._handle_retry_error(error, max_retries)
