# Python imports
import base64
import logging
import os
import threading
import uuid
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from statistics import mean
from typing import Any, Literal
from urllib.parse import urljoin

# 3rd party imports
import httpx
from joblib import Parallel, delayed  # type: ignore
from pydantic import BaseModel
from tenacity import retry, retry_if_not_exception_type, stop_after_attempt, wait_fixed

# Local imports
from cohere_compass import GroupAuthorizationInput
from cohere_compass.constants import (
    DEFAULT_COMPASS_CLIENT_TIMEOUT,
    DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES,
    DEFAULT_MAX_CHUNKS_PER_REQUEST,
    DEFAULT_MAX_ERROR_RATE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_WAIT,
)
from cohere_compass.exceptions import (
    CompassAuthError,
    CompassClientError,
    CompassError,
    CompassMaxErrorRateExceeded,
)
from cohere_compass.models import (
    CompassDocument,
    CompassSdkStage,
    CreateDataSource,
    DataSource,
    DirectSearchInput,
    DirectSearchResponse,
    DirectSearchScrollInput,
    Document,
    DocumentStatus,
    ParseableDocument,
    PutDocumentsInput,
    SearchChunksResponse,
    SearchDocumentsResponse,
    SearchFilter,
    SearchInput,
    UploadDocumentsInput,
)
from cohere_compass.models.config import IndexConfig
from cohere_compass.models.datasources import PaginatedList
from cohere_compass.models.documents import DocumentAttributes, PutDocumentsResponse
from cohere_compass.utils import partition_documents


@dataclass
class SendRequestResult:
    """
    A class to represent the result of a retryable operation.

    The class contains the following fields:
    - result: The result of the operation if successful, otherwise None.
    - error (Optional[str]): The error message if the operation failed, otherwise None.

    Notice that this is an internal class and should not be exposed to clients.
    """

    result: str | bytes | dict[str, Any] | None = None
    content_type: str | None = None


logger = logging.getLogger(__name__)


API_DEFINITIONS = {
    # Index APIs
    "create_index": (
        "PUT",
        "indexes/{index_name}",
    ),
    "list_indexes": (
        "GET",
        "indexes",
    ),
    "delete_index": (
        "DELETE",
        "indexes/{index_name}",
    ),
    "refresh": (
        "POST",
        "indexes/{index_name}/_refresh",
    ),
    "update_group_authorization": (
        "POST",
        "indexes/{index_name}/group_authorization",
    ),
    # Document APIs
    "delete_document": (
        "DELETE",
        "indexes/{index_name}/documents/{document_id}",
    ),
    "get_document": (
        "GET",
        "indexes/{index_name}/documents/{document_id}",
    ),
    "put_documents": (
        "PUT",
        "indexes/{index_name}/documents",
    ),
    "get_document_asset": (
        "GET",
        "indexes/{index_name}/documents/{document_id}/assets/{asset_id}",
    ),
    "add_attributes": (
        "POST",
        "indexes/{index_name}/documents/{document_id}/_add_attributes",
    ),
    "upload_documents": (
        "POST",
        "indexes/{index_name}/documents/_upload",
    ),
    # Search APIs
    "search_documents": (
        "POST",
        "indexes/{index_name}/documents/_search",
    ),
    "search_chunks": (
        "POST",
        "indexes/{index_name}/documents/_search_chunks",
    ),
    "direct_search": (
        "POST",
        "indexes/{index_name}/_direct_search",
    ),
    "direct_search_scroll": (
        "POST",
        "indexes/_direct_search/scroll",
    ),
    # Data Sources APIs
    "create_datasource": (
        "POST",
        "datasources",
    ),
    "list_datasources": (
        "GET",
        "datasources",
    ),
    "delete_datasources": (
        "DELETE",
        "datasources/{datasource_id}",
    ),
    "get_datasource": (
        "GET",
        "datasources/{datasource_id}",
    ),
    "sync_datasource": (
        "POST",
        "datasources/{datasource_id}/_sync",
    ),
    "list_datasources_objects_states": (
        "GET",
        "datasources/{datasource_id}/documents?skip={skip}&limit={limit}",
    ),
}


class CompassClient:
    """A compass client to interact with the Compass API."""

    def __init__(
        self,
        *,
        index_url: str,
        bearer_token: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_wait: timedelta = DEFAULT_RETRY_WAIT,
        include_api_in_url: bool = True,
    ):
        """
        Initialize the Compass client.

        IMPORTANT NOTE: If the user desires, a custom HTTP session can be passed. In
        this case, however, it is the responsibility of the user to manage thread
        safety. The user should ensure that the session is not shared among multiple
        threads. If in doubt, do not pass a custom session and let us handle the dirty
        work.

        :param index_url: The base URL for the index API.
        :param bearer_token (optional): The bearer token for authentication.
        :param http_session (optional): An optional HTTP session to use for requests.
        :param include_api_in_url: Whether to include '/api' in the base URL.
               Defaults to True.
        """
        self.index_url = index_url
        self.httpx_client = httpx.Client(timeout=DEFAULT_COMPASS_CLIENT_TIMEOUT)

        self.bearer_token = bearer_token

        if max_retries < 0:
            raise ValueError("default_max_retries must be a non-negative integer.")
        if retry_wait.total_seconds() < 0:
            raise ValueError(
                "default_sleep_retry_seconds must be a non-negative integer."
            )
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self.include_api_in_url = include_api_in_url

    def create_index(
        self,
        *,
        index_name: str,
        index_config: IndexConfig | None = None,
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
            data=index_config,
        )

    def refresh_index(
        self,
        *,
        index_name: str,
    ):
        """
        Refresh index.

        :param index_name: the name of the index
        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="refresh",
            index_name=index_name,
        )

    def delete_index(
        self,
        *,
        index_name: str,
    ):
        """
        Delete an index from Compass.

        :param index_name: the name of the index
        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="delete_index",
            index_name=index_name,
        )

    def delete_document(
        self,
        *,
        index_name: str,
        document_id: str,
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
            index_name=index_name,
        )

    def get_document(
        self,
        *,
        index_name: str,
        document_id: str,
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
            index_name=index_name,
        )

    def list_indexes(
        self,
    ):
        """
        List all indexes in Compass.

        :returns: the response from the Compass API
        """
        return self._send_request(
            api_name="list_indexes",
            index_name="",
        )

    def add_attributes(
        self,
        *,
        index_name: str,
        document_id: str,
        attributes: DocumentAttributes,
    ):
        """
        Update the content field of an existing document with additional context.

        :param index_name: the name of the index
        :param document_id: the document to modify
        :param attributes: the attributes to add to the document

        :returns: an error message if the request failed, otherwise None
        """
        self._send_request(
            api_name="add_attributes",
            document_id=document_id,
            data=attributes,
            index_name=index_name,
        )

    def insert_doc(
        self,
        *,
        index_name: str,
        doc: CompassDocument,
        authorized_groups: list[str] | None = None,
        merge_groups_on_conflict: bool = False,
    ) -> list[dict[str, str]] | None:
        """
        Insert a parsed document into an index in Compass.

        :param index_name: the name of the index
        :param doc: the parsed compass document
        """
        return self.insert_docs(
            index_name=index_name,
            docs=iter([doc]),
            authorized_groups=authorized_groups,
            merge_groups_on_conflict=merge_groups_on_conflict,
        )

    def upload_document(
        self,
        *,
        index_name: str,
        filename: str,
        filebytes: bytes,
        content_type: str,
        document_id: uuid.UUID,
        attributes: DocumentAttributes = DocumentAttributes(),
    ) -> str | dict[str, Any] | None:
        """
        Parse and insert a document into an index in Compass.

        :param index_name: the name of the index
        :param filename: the filename of the document
        :param filebytes: the bytes of the document
        :param content_type: the content type of the document
        :param document_id: the id of the document (optional)
        :param context: represents an additional information about the document

        :returns: an error message if the request failed, otherwise None
        """
        if len(filebytes) > DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES:
            max_file_size_mb = DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES / 1000_000
            err = f"File too large, supported file size is {max_file_size_mb} mb"
            logger.error(err)
            return err

        b64 = base64.b64encode(filebytes).decode("utf-8")
        doc = ParseableDocument(
            id=document_id,
            filename=filename,
            content_type=content_type,
            content_length_bytes=len(filebytes),
            content_encoded_bytes=b64,
            attributes=attributes,
        )

        result = self._send_request(
            api_name="upload_documents",
            data=UploadDocumentsInput(documents=[doc]),
            index_name=index_name,
        )

        return result.result  # type: ignore

    def insert_docs(
        self,
        *,
        index_name: str,
        docs: Iterator[CompassDocument],
        max_chunks_per_request: int = DEFAULT_MAX_CHUNKS_PER_REQUEST,
        max_error_rate: float = DEFAULT_MAX_ERROR_RATE,
        errors_sliding_window_size: int | None = 10,
        skip_first_n_docs: int = 0,
        num_jobs: int | None = None,
        authorized_groups: list[str] | None = None,
        merge_groups_on_conflict: bool = False,
    ) -> list[dict[str, str]] | None:
        """
        Insert multiple parsed documents into an index in Compass.

        :param index_name: the name of the index
        :param docs: the parsed documents
        :param max_chunks_per_request: the maximum number of chunks to send in a single
            API request
        :param num_jobs: the number of parallel jobs to use
        :param max_error_rate: the maximum error rate allowed
        :param max_retries: the maximum number of times to retry a request if it fails
        :param sleep_retry_seconds: the number of seconds to wait before retrying an API
            request
        :param errors_sliding_window_size: the size of the sliding window to keep track
            of errors
        :param skip_first_n_docs: number of docs to skip indexing. Useful when insertion
            failed after N documents
        :param authorized_groups: the groups that are authorized to access the
            documents. These groups should exist in RBAC. None passed will make the
            documents public
        :param merge_groups_on_conflict: when doc level security enable, allow upserting
            documents with static groups
        """

        def put_request(
            request_data: list[tuple[CompassDocument, Document]],
            previous_errors: list[dict[str, str]],
            num_doc: int,
        ) -> None:
            nonlocal num_succeeded, errors
            errors.extend(previous_errors)
            compass_docs: list[CompassDocument] = [
                compass_doc for compass_doc, _ in request_data
            ]
            put_docs_input = PutDocumentsInput(
                documents=[input_doc for _, input_doc in request_data],
                authorized_groups=authorized_groups,
                merge_groups_on_conflict=merge_groups_on_conflict,
            )

            # It could be that all documents have errors, in which case we should not
            # send a request to the Compass Server. This is a common case when the
            # parsing of the documents fails.  In this case, only errors will appear in
            # the insertion_docs response
            if not request_data:
                return

            try:
                self._send_request(
                    api_name="put_documents",
                    data=put_docs_input,
                    index_name=index_name,
                )
                num_succeeded += len(compass_docs)
            except CompassError as e:
                error = str(e)
                for doc in compass_docs:
                    filename = doc.metadata.filename
                    doc.errors.append(
                        {CompassSdkStage.Indexing: f"{filename}: {error}"}
                    )
                    errors.append({doc.metadata.document_id: f"{filename}: {error}"})

                # Keep track of the results of the last N API calls to calculate the
                # error rate If the error rate is higher than the threshold, stop the
                # insertion process
                error_window.append(error)

            error_rate = (
                mean([1 if x else 0 for x in error_window])
                if len(error_window) == error_window.maxlen
                else 0
            )
            if error_rate > max_error_rate:
                raise CompassMaxErrorRateExceeded(
                    f"[Thread {threading.get_native_id()}] {error_rate * 100}% of "
                    f"insertions failed in the last {errors_sliding_window_size} API "
                    "calls. Stopping the insertion process."
                )

        error_window: deque[str | None] = deque(
            maxlen=errors_sliding_window_size
        )  # Keep track of the results of the last N API calls
        num_succeeded = 0
        errors: list[dict[str, str]] = []
        requests_iter = partition_documents(docs, max_chunks_per_request)

        try:
            num_jobs = num_jobs or os.cpu_count()
            Parallel(n_jobs=num_jobs, backend="threading")(
                delayed(put_request)(
                    request_data=request_block,
                    previous_errors=previous_errors,
                    num_doc=i,
                )
                for i, (request_block, previous_errors) in enumerate(requests_iter, 1)
                if i > skip_first_n_docs
            )
        except CompassMaxErrorRateExceeded as e:
            logger.error(e.message)
        return errors if len(errors) > 0 else None

    def create_datasource(
        self,
        *,
        datasource: CreateDataSource,
    ) -> DataSource | str:
        """
        Create a new datasource in Compass.

        :param datasource: the datasource to create
        """
        result = self._send_request(
            api_name="create_datasource",
            data=datasource,
        )

        return DataSource.model_validate(result.result)

    def list_datasources(self) -> PaginatedList[DataSource] | str:
        """List all datasources in Compass."""
        result = self._send_request(api_name="list_datasources")

        return PaginatedList[DataSource].model_validate(result.result)

    def get_datasource(
        self,
        *,
        datasource_id: str,
    ):
        """
        Get a datasource in Compass.

        :param datasource_id: the id of the datasource
        """
        result = self._send_request(
            api_name="get_datasource",
            datasource_id=datasource_id,
        )

        return DataSource.model_validate(result.result)

    def delete_datasource(
        self,
        *,
        datasource_id: str,
    ):
        """
        Delete a datasource in Compass.

        :param datasource_id: the id of the datasource
        """
        result = self._send_request(
            api_name="delete_datasources",
            datasource_id=datasource_id,
        )

        return result.result

    def sync_datasource(
        self,
        *,
        datasource_id: str,
    ):
        """
        Sync a datasource in Compass.

        :param datasource_id: the id of the datasource
        """
        result = self._send_request(
            api_name="sync_datasource",
            datasource_id=datasource_id,
        )

        return result.result

    def list_datasources_objects_states(
        self,
        *,
        datasource_id: str,
        skip: int = 0,
        limit: int = 100,
    ) -> PaginatedList[DocumentStatus] | str:
        """
        List all objects states in a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param skip: the number of objects to skip
        :param limit: the number of objects to return
        """
        result = self._send_request(
            api_name="list_datasources_objects_states",
            datasource_id=datasource_id,
            skip=str(skip),
            limit=str(limit),
        )

        return PaginatedList[DocumentStatus].model_validate(result.result)

    def _search(
        self,
        *,
        api_name: Literal["search_documents", "search_chunks"],
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: list[SearchFilter] | None = None,
    ):
        return self._send_request(
            api_name=api_name,
            index_name=index_name,
            data=SearchInput(query=query, top_k=top_k, filters=filters),
        )

    def search_documents(
        self,
        *,
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: list[SearchFilter] | None = None,
    ) -> SearchDocumentsResponse:
        """
        Search documents in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of documents to return
        :param filters: the search filters to apply

        :returns: the search results
        """
        result = self._search(
            api_name="search_documents",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
        )

        return SearchDocumentsResponse.model_validate(result.result)

    def search_chunks(
        self,
        *,
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: list[SearchFilter] | None = None,
    ) -> SearchChunksResponse:
        """
        Search chunks in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of chunks to return
        :param filters: the search filters to apply

        :returns: the search results
        """
        result = self._search(
            api_name="search_chunks",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
        )

        return SearchChunksResponse.model_validate(result.result)

    def get_document_asset(
        self,
        *,
        index_name: str,
        document_id: str,
        asset_id: str,
    ) -> tuple[str | bytes | dict[str, Any], str]:
        """
        Get an asset from a document in Compass.

        :param index_name: the name of the index
        :param document_id: the id of the document
        :param asset_id: the id of the asset

        :returns: A tuple of the content and content type of the asset. The variable
        type of the content is either str, bytes, or dict[str, Any], depending on the
        asset type. For example, if the asset is an image, the content type will be
        bytes; if the asset is a markdown, the content type will be str; if the asset is
        a json, the content type will be dict[str, Any].

        :raises CompassError: if the asset cannot be retrieved, either because it
        doesn't exist or the user doesn't have permission to access it.
        """
        result = self._send_request(
            api_name="get_document_asset",
            index_name=index_name,
            document_id=document_id,
            asset_id=asset_id,
        )

        return result.result, result.content_type  # type: ignore

    def update_group_authorization(
        self,
        *,
        index_name: str,
        group_auth_input: GroupAuthorizationInput,
    ) -> PutDocumentsResponse:
        """
        Edit group authorization for an index.

        :param index_name: the name of the index
        :param group_auth_input: the group authorization input
        """
        result = self._send_request(
            api_name="update_group_authorization",
            index_name=index_name,
            data=group_auth_input,
        )
        return PutDocumentsResponse.model_validate(result.result)

    def direct_search(
        self,
        *,
        index_name: str,
        query: dict[str, Any],
        size: int = 100,
        scroll: str | None = None,
    ) -> DirectSearchResponse:
        """
        Perform a direct search query against the Compass API.

        :param index_name: the name of the index
        :param query: the direct search query (e.g. {"match_all": {}})
        :param size: the number of results to return
        :param scroll: the scroll duration (e.g. "1m" for 1 minute)

        :returns: the direct search results
        :raises CompassError: if the search fails
        """
        data = DirectSearchInput(query=query, size=size, scroll=scroll)

        result = self._send_request(
            api_name="direct_search",
            index_name=index_name,
            data=data,
        )

        return DirectSearchResponse.model_validate(result.result)

    def direct_search_scroll(
        self,
        *,
        scroll_id: str,
        scroll: str = "1m",
    ) -> DirectSearchResponse:
        """
        Continue a search using a scroll ID from a previous direct_search call.

        :param scroll_id: the scroll ID from a previous direct_search call
        :param scroll: the scroll duration (e.g. "1m" for 1 minute)

        :returns: the next batch of search results
        :raises CompassError: if the scroll search fails
        """
        data = DirectSearchScrollInput(scroll_id=scroll_id, scroll=scroll)

        result = self._send_request(
            api_name="direct_search_scroll",
            data=data,
        )

        return DirectSearchResponse.model_validate(result.result)

    def _send_http_request(
        self,
        http_method: str,
        target_path: str,
        data: BaseModel | None = None,
    ):
        data_dict = data.model_dump(mode="json", exclude_none=True) if data else None

        headers = None
        if self.bearer_token:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

        if http_method == "GET":
            response = self.httpx_client.get(target_path, headers=headers)
        elif http_method == "POST":
            response = self.httpx_client.post(
                target_path, json=data_dict, headers=headers
            )
        elif http_method == "PUT":
            response = self.httpx_client.put(
                target_path, json=data_dict, headers=headers
            )
        elif http_method == "DELETE":
            response = self.httpx_client.get(target_path, headers=headers)
        else:
            raise RuntimeError(f"Unsupported HTTP method: {http_method}")

        response.raise_for_status()

        content_type = response.headers.get("content-type")
        if content_type in ("image/jpeg", "image/png"):
            # To handle response from get_document_asset() when the asset
            # is an image.
            result = response.content
        elif content_type == "text/markdown":
            # To handle response from get_document_asset() when the asset
            # is a markdown.
            result = response.text
        else:
            # To handle response from other APIs.
            result = response.json() if response.text else None
        return SendRequestResult(
            result=result,
            content_type=content_type,
        )

    def _send_request(
        self,
        api_name: str,
        data: BaseModel | None = None,
        **url_params: str,
    ) -> SendRequestResult:
        """
        Send a request to the Compass API.

        :param function: the function to call
        :param index_name: the name of the index
        :param data: the data to send
        :returns: An error message if the request failed, otherwise None.
        """
        if api_name not in API_DEFINITIONS:
            raise CompassError(
                f"API name '{api_name}' is not defined in the API definitions."
            )
        http_method, api_path = API_DEFINITIONS[api_name]

        if self.include_api_in_url:
            target_path = urljoin(self.index_url, f"api/v1/{api_path}")
        else:
            target_path = urljoin(self.index_url, f"v1/{api_path}")
        target_path = target_path.format(**url_params)

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_fixed(self.retry_wait),
            reraise=True,  # re-raise last exception instead of wrapping in RetryError
            # todo find alternative to InvalidSchema
            retry=retry_if_not_exception_type((CompassClientError,)),
        )
        def _send_request_with_retry() -> SendRequestResult:
            try:
                return self._send_http_request(
                    http_method=http_method,
                    target_path=target_path,
                    data=data,
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 401:
                    error = "Unauthorized. Please check your bearer token."
                    raise CompassAuthError(message=str(e))
                elif 400 <= e.response.status_code < 500:
                    error = f"Client error occurred: {e.response.text}"
                    raise CompassClientError(message=error, code=e.response.status_code)
                else:
                    error = str(e) + " " + e.response.text
                    logger.warning(
                        f"Failed to send request to {api_name} {target_path}: "
                        f"{type(e)} {error}. Going to sleep for "
                        f"{self.retry_wait} seconds and retrying."
                    )
                    raise e
            except Exception as e:
                error = str(e)
                logger.warning(
                    f"Failed to send request to {api_name} {target_path}: {type(e)} "
                    f"{error}. Sleeping {self.retry_wait} seconds and retrying..."
                )
                raise e

        try:
            return _send_request_with_retry()
        except Exception as e:
            raise CompassError(f"Failed to send request for {api_name} API") from e
