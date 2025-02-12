# Python imports
import base64
import logging
import os
import threading
import uuid
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from statistics import mean
from typing import Any, Literal, Optional, Union, ClassVar, TypedDict

from aiohttp.client_exceptions import ClientResponseError
import requests
import aiohttp

# 3rd party imports
# TODO find stubs for joblib and remove "type: ignore"
from joblib import Parallel, delayed  # type: ignore
from pydantic import BaseModel
from requests.exceptions import InvalidSchema
from tenacity import (
    RetryError,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_fixed,
)

# Local imports
from cohere.compass import (
    GroupAuthorizationInput,
)
from cohere.compass.constants import (
    DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES,
    DEFAULT_MAX_CHUNKS_PER_REQUEST,
    DEFAULT_MAX_ERROR_RATE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_RETRY_SECONDS,
)
from cohere.compass.exceptions import (
    CompassAuthError,
    CompassClientError,
    CompassError,
    CompassMaxErrorRateExceeded,
)
from cohere.compass.models import (
    Chunk,
    CompassDocument,
    CompassDocumentStatus,
    CompassSdkStage,
    CreateDataSource,
    DataSource,
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
from cohere.compass.models.config import IndexConfig
from cohere.compass.models.datasources import PaginatedList
from cohere.compass.models.documents import DocumentAttributes, PutDocumentsResponse


@dataclass
class _RetryResult:
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


class BaseCompassClient:
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

    def _get_target_path(self, api_name: str, **url_params: str) -> str:
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
        self, result: _RetryResult | None, error: str | None
    ) -> _RetryResult:
        if result:
            return result
        return _RetryResult(result=None, error=error)

    def _handle_retry_error(
        self, error: str | None, max_retries: int | None
    ) -> _RetryResult:
        logger.error(f"Failed to send request after {max_retries} attempts. Aborting.")
        return _RetryResult(result=None, error=error)


class CompassClient(BaseCompassClient):
    """A compass client to interact with the Compass API."""

    def __init__(
        self,
        *,
        index_url: str,
        bearer_token: Optional[str] = None,
        http_session: Optional[requests.Session] = None,
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
        self.session = http_session or requests.Session()

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

    def add_attributes(
        self,
        *,
        index_name: str,
        document_id: str,
        attributes: DocumentAttributes,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Optional[str]:
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
        result = self._send_request(
            api_name="add_attributes",
            document_id=document_id,
            data=attributes,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )
        if result.error:
            return result.error
        return None

    def insert_doc(
        self,
        *,
        index_name: str,
        doc: CompassDocument,
        authorized_groups: Optional[list[str]] = None,
        merge_groups_on_conflict: bool = False,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Optional[list[dict[str, str]]]:
        """
        Insert a parsed document into an index in Compass.

        :param index_name: the name of the index
        :param doc: the parsed compass document
        :param max_retries: the maximum number of times to retry a doc insertion
        :param sleep_retry_seconds: interval between the document insertion retries.
        """
        return self.insert_docs(
            index_name=index_name,
            docs=iter([doc]),
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
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
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Optional[Union[str, dict[str, Any]]]:
        """
        Parse and insert a document into an index in Compass.

        :param index_name: the name of the index
        :param filename: the filename of the document
        :param filebytes: the bytes of the document
        :param content_type: the content type of the document
        :param document_id: the id of the document (optional)
        :param context: represents an additional information about the document
        :param max_retries: the maximum number of times to retry a request if it fails
        :param sleep_retry_seconds: interval between API request retries

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
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

        if result.error:
            return result.error
        return result.result

    def insert_docs(
        self,
        *,
        index_name: str,
        docs: Iterator[CompassDocument],
        max_chunks_per_request: int = DEFAULT_MAX_CHUNKS_PER_REQUEST,
        max_error_rate: float = DEFAULT_MAX_ERROR_RATE,
        errors_sliding_window_size: Optional[int] = 10,
        skip_first_n_docs: int = 0,
        num_jobs: Optional[int] = None,
        authorized_groups: Optional[list[str]] = None,
        merge_groups_on_conflict: bool = False,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Optional[list[dict[str, str]]]:
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

            results = self._send_request(
                api_name="put_documents",
                data=put_docs_input,
                max_retries=max_retries,
                sleep_retry_seconds=sleep_retry_seconds,
                index_name=index_name,
            )

            if results.error:
                for doc in compass_docs:
                    filename = doc.metadata.filename
                    error = results.error
                    doc.errors.append(
                        {CompassSdkStage.Indexing: f"{filename}: {error}"}
                    )
                    errors.append({doc.metadata.document_id: f"{filename}: {error}"})
            else:
                num_succeeded += len(compass_docs)

            # Keep track of the results of the last N API calls to calculate the error
            # rate If the error rate is higher than the threshold, stop the insertion
            # process
            error_window.append(results.error)
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

        error_window: deque[Optional[str]] = deque(
            maxlen=errors_sliding_window_size
        )  # Keep track of the results of the last N API calls
        num_succeeded = 0
        errors: list[dict[str, str]] = []
        requests_iter = self._get_request_blocks(docs, max_chunks_per_request)

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
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Union[DataSource, str]:
        """
        Create a new datasource in Compass.

        :param datasource: the datasource to create
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="create_datasource",
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            data=datasource,
        )

        if result.error:
            return result.error
        return DataSource.model_validate(result.result)

    def list_datasources(
        self,
        *,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Union[PaginatedList[DataSource], str]:
        """
        List all datasources in Compass.

        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="list_datasources",
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            return result.error
        return PaginatedList[DataSource].model_validate(result.result)

    def get_datasource(
        self,
        *,
        datasource_id: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Get a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="get_datasource",
            datasource_id=datasource_id,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            return result.error
        return DataSource.model_validate(result.result)

    def delete_datasource(
        self,
        *,
        datasource_id: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Delete a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="delete_datasources",
            datasource_id=datasource_id,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            return result.error
        return result.result

    def sync_datasource(
        self,
        *,
        datasource_id: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        """
        Sync a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="sync_datasource",
            datasource_id=datasource_id,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            return result.error
        return result.result

    def list_datasources_objects_states(
        self,
        *,
        datasource_id: str,
        skip: int = 0,
        limit: int = 100,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> Union[PaginatedList[DocumentStatus], str]:
        """
        List all objects states in a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param skip: the number of objects to skip
        :param limit: the number of objects to return
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="list_datasources_objects_states",
            datasource_id=datasource_id,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            skip=str(skip),
            limit=str(limit),
        )

        if result.error:
            return result.error
        return PaginatedList[DocumentStatus].model_validate(result.result)

    @staticmethod
    def _get_request_blocks(
        docs: Iterator[CompassDocument],
        max_chunks_per_request: int,
    ):
        """
        Create request blocks to send to the Compass API.

        :param docs: the documents to send
        :param max_chunks_per_request: the maximum number of chunks to send in a single
            API request
        :returns: an iterator over the request blocks
        """
        request_block: list[tuple[CompassDocument, Document]] = []
        errors: list[dict[str, str]] = []
        num_chunks = 0
        for _, doc in enumerate(docs, 1):
            if doc.status != CompassDocumentStatus.Success:
                logger.error(
                    f"Document {doc.metadata.document_id} has errors: {doc.errors}"
                )
                for error in doc.errors:
                    errors.append(
                        {doc.metadata.document_id: next(iter(error.values()))}
                    )
            else:
                num_chunks += (
                    len(doc.chunks)
                    if doc.status == CompassDocumentStatus.Success
                    else 0
                )
                if num_chunks > max_chunks_per_request:
                    yield request_block, errors
                    request_block, errors = [], []
                    num_chunks = 0

                request_block.append(
                    (
                        doc,
                        Document(
                            document_id=doc.metadata.document_id,
                            parent_document_id=doc.metadata.parent_document_id,
                            path=doc.metadata.filename,
                            content=doc.content,
                            chunks=[Chunk(**c.model_dump()) for c in doc.chunks],
                            index_fields=doc.index_fields,
                        ),
                    )
                )

        if len(request_block) > 0 or len(errors) > 0:
            yield request_block, errors

    def _search(
        self,
        *,
        api_name: Literal["search_documents", "search_chunks"],
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: Optional[list[SearchFilter]] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        return self._send_request(
            api_name=api_name,
            index_name=index_name,
            data=SearchInput(query=query, top_k=top_k, filters=filters),
            max_retries=1,
            sleep_retry_seconds=1,
        )

    def search_documents(
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
        result = self._search(
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

    def search_chunks(
        self,
        *,
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: Optional[list[SearchFilter]] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
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
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            raise CompassError(result.error)

        return SearchChunksResponse.model_validate(result.result)

    def update_group_authorization(
        self,
        *,
        index_name: str,
        group_auth_input: GroupAuthorizationInput,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
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
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )
        if result.error:
            raise CompassError(result.error)
        return PutDocumentsResponse.model_validate(result.result)

    # todo Simplify this method so we don't have to ignore the C901 complexity warning.
    def _send_request(
        self,
        api_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
        data: Optional[BaseModel] = None,
        **url_params: str,
    ) -> _RetryResult:
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
        def _send_request_with_retry():
            nonlocal error

            try:
                response = self.session.request(
                    method=compass_request["method"],
                    url=target_path,
                    json=compass_request["data"],
                    headers=compass_request["headers"],
                )

                if response.ok:
                    error = None
                    result = response.json() if response.text else None
                    return _RetryResult(result=result, error=None)
                else:
                    response.raise_for_status()

            except requests.exceptions.HTTPError as e:
                self._handle_http_error(
                    e.response.status_code,
                    e.response.text,
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
            res = _send_request_with_retry()
            return self._handle_retry_result(res, error)
        except RetryError:
            return self._handle_retry_error(error, max_retries)


class AsyncCompassClient(BaseCompassClient):
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
    ) -> _RetryResult:
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
    ) -> _RetryResult:
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
                    return _RetryResult(result=result, error=None)
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
