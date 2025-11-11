"""
Async Compass client for indexing and search operations.

This module provides the CompassAsyncClient class which handles asynchronous document
indexing, searching, and management operations with the Compass API.  Supports
concurrent operations and comprehensive error handling.
"""

# Python imports
import base64
import os
import uuid
from collections import deque
from collections.abc import AsyncIterable, Iterable
from datetime import timedelta
from statistics import mean
from typing import Any, Literal

# 3rd party imports
import httpx
from pydantic import BaseModel
from tenacity import retry, retry_if_not_exception_type, stop_after_attempt, wait_fixed

# Local imports
from cohere_compass import GroupAuthorizationInput
from cohere_compass.clients.compass import (
    API_DEFINITIONS,
    _SendRequestResult,  # pyright: ignore[reportPrivateUsage]
    logger,
)
from cohere_compass.constants import (
    DEFAULT_COMPASS_CLIENT_TIMEOUT,
    DEFAULT_MAX_CHUNKS_PER_REQUEST,
    DEFAULT_MAX_ERROR_RATE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_WAIT,
)
from cohere_compass.exceptions import (
    CompassAuthError,
    CompassClientError,
    CompassError,
    CompassInsertionError,
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
from cohere_compass.models.documents import (
    AssetPresignedUrlDetails,
    AssetPresignedUrlRequest,
    ContentTypeEnum,
    DocumentAttributes,
    GetAssetPresignedUrlsRequest,
    GetAssetPresignedUrlsResponse,
    ParseableDocumentConfig,
    ParsedDocumentResponse,
    PutDocumentsResponse,
    UploadDocumentsResult,
    UploadDocumentsStatus,
)
from cohere_compass.models.indexes import IndexDetails, ListIndexesResponse
from cohere_compass.models.search import GetDocumentResponse, RetrievedDocument, SortBy
from cohere_compass.utils import (
    async_apply,
    async_enumerate,
    partition_documents_async,
)


class CompassAsyncClient:
    """
    Async client for interacting with the Compass API.

    Provides asynchronous methods for document indexing, searching, and management
    operations. Supports concurrent operations with configurable retry logic.
    """

    def __init__(
        self,
        *,
        index_url: str,
        bearer_token: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_wait: timedelta = DEFAULT_RETRY_WAIT,
        timeout: timedelta | None = None,
        httpx_client: httpx.AsyncClient | None = None,
    ):
        """
        Initialize the async Compass client.

        :param index_url: The base URL for the Compass API.
        :param bearer_token: Optional bearer token for API authentication.
        :param max_retries: Maximum number of retries for failed requests.
        :param retry_wait: Time to wait between retries.
        :param timeout: Request timeout duration. If not specified, it defaults to
            DEFAULT_COMPASS_CLIENT_TIMEOUT.
        :param httpx_client: The httpx client to use for making requests. If not
            provided, a new httpx client will be created with the timeout set when
            creating the client.

        Raises:
            ValueError: If max_retries is negative or retry_wait is negative.

        """
        self.index_url = index_url if index_url.endswith("/") else f"{index_url}/"
        self.timeout = (
            timeout
            if timeout is not None
            else DEFAULT_COMPASS_CLIENT_TIMEOUT
            if httpx_client is None
            else timedelta(seconds=httpx_client.timeout.read)
            if httpx_client.timeout.read
            else DEFAULT_COMPASS_CLIENT_TIMEOUT
        )
        self.httpx_client = httpx_client or httpx.AsyncClient(
            timeout=self.timeout.total_seconds()
        )

        self.bearer_token = bearer_token

        if max_retries < 0:
            raise ValueError("default_max_retries must be a non-negative integer.")
        if retry_wait.total_seconds() < 0:
            raise ValueError(
                "default_sleep_retry_seconds must be a non-negative integer."
            )
        self.max_retries = max_retries
        self.retry_wait = retry_wait

    async def aclose(self):
        """Close the HTTP client."""
        await self.httpx_client.aclose()

    async def get_models(
        self,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> dict[str, list[str]]:
        """
        Get the models available in Compass.

        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        :return: Dictionary with the models available in Compass, where the keys are the
            model roles ("dense", "rerank", "sparse") and the values are lists of model
            versions for each role.
        """
        result = await self._send_request(
            api_name="get_models",
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )
        if not isinstance(result.result, dict):
            raise ValueError("Invalid response from Compass API")

        return result.result

    async def create_index(
        self,
        *,
        index_name: str,
        index_config: IndexConfig | None = None,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Create an index in Compass.

        :param index_name: the name of the index
        :param index_config: the optional configuration for the index
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        await self._send_request(
            api_name="create_index",
            index_name=index_name,
            data=index_config,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def get_index_details(
        self,
        *,
        index_name: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> IndexDetails:
        """
        Get the details of an index in Compass.

        :param index_name: the name of the index
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        :return: IndexDetails object containing the details of the index
        """
        result = await self._send_request(
            api_name="get_index_details",
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return IndexDetails.model_validate(result.result)

    async def refresh_index(
        self,
        *,
        index_name: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Refresh index.

        :param index_name: the name of the index
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.
        """
        await self._send_request(
            api_name="refresh",
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def delete_index(
        self,
        *,
        index_name: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Delete an index from Compass.

        :param index_name: the name of the index
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        await self._send_request(
            api_name="delete_index",
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def delete_document(
        self,
        *,
        index_name: str,
        document_id: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Delete a document from Compass.

        :param index_name: the name of the index
        :param document_id: the id of the document
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        await self._send_request(
            api_name="delete_document",
            document_id=document_id,
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def get_document(
        self,
        *,
        index_name: str,
        document_id: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> RetrievedDocument:
        """
        Get a document from Compass.

        :param index_name: the name of the index
        :param document_id: the id of the document
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        :return: RetrievedDocument object containing the document
        """
        result = await self._send_request(
            api_name="get_document",
            document_id=document_id,
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )
        response = GetDocumentResponse.model_validate(result.result)
        return response.document

    async def list_indexes(
        self,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> ListIndexesResponse:
        """
        List all indexes in Compass.

        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            ListIndexesResponse object containing the list of indexes

        """
        result = await self._send_request(
            api_name="list_indexes",
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )
        return ListIndexesResponse.model_validate(result.result)

    async def add_attributes(
        self,
        *,
        index_name: str,
        document_id: str,
        attributes: DocumentAttributes,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Update the content field of an existing document with additional context.

        :param index_name: the name of the index
        :param document_id: the document to modify
        :param attributes: the attributes to add to the document
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        await self._send_request(
            api_name="add_attributes",
            document_id=document_id,
            data=attributes,
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def insert_doc(
        self,
        *,
        index_name: str,
        doc: CompassDocument,
        authorized_groups: list[str] | None = None,
        merge_groups_on_conflict: bool = False,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Insert a parsed document into an index in Compass.

        :param index_name: the name of the index
        :param doc: the parsed compass document
        :param authorized_groups: Optional list of groups authorized to access this
            document.
        :param merge_groups_on_conflict: Whether to merge groups on conflict.
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        await self.insert_docs(
            index_name=index_name,
            docs=[doc],
            authorized_groups=authorized_groups,
            merge_groups_on_conflict=merge_groups_on_conflict,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def upload_document(
        self,
        *,
        index_name: str,
        filename: str,
        filebytes: bytes,
        content_type: ContentTypeEnum,
        document_id: str,
        attributes: DocumentAttributes = DocumentAttributes(),
        config: ParseableDocumentConfig = ParseableDocumentConfig(),
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> UploadDocumentsResult:
        """
        Parse and insert a document into an index in Compass.

        :param index_name: the name of the index
        :param filename: the filename of the document
        :param filebytes: the bytes of the document
        :param content_type: the content type of the document
        :param document_id: the id of the document (optional)
        :param attributes: Additional attributes to add to the document.
        :param config: Configuration for the document parsing.
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            UploadDocumentsResult object containing the result of the upload

        """
        b64 = base64.b64encode(filebytes).decode("utf-8")
        doc = ParseableDocument(
            id=document_id,
            filename=filename,
            content_type=content_type,
            content_length_bytes=len(filebytes),
            content_encoded_bytes=b64,
            attributes=attributes,
            config=config,
        )

        result = await self._send_request(
            api_name="upload_documents",
            data=UploadDocumentsInput(documents=[doc]),
            index_name=index_name,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return UploadDocumentsResult.model_validate(result.result)

    async def upload_document_status(
        self,
        *,
        index_name: str,
        upload_id: uuid.UUID,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> list[UploadDocumentsStatus] | None:
        """
        Status of the document upload.

        :param index_name: the name of the index
        :param upload_id: the upload id returned when uploading the document
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.


        Returns:
            List of UploadDocumentsStatus objects containing the status of the upload
            of the various documents in the upload batch.

        """
        result = await self._send_request(
            api_name="upload_documents_status",
            index_name=index_name,
            upload_id=str(upload_id),
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return [UploadDocumentsStatus(**r) for r in result.result]  # type: ignore

    async def download_parsed_document(
        self,
        *,
        index_name: str,
        upload_id: uuid.UUID,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> list[ParsedDocumentResponse] | None:
        """
        Download the parsed document from Compass.

        :param index_name: the name of the index
        :param upload_id: the upload id returned when uploading the document
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.


        Returns:
            List of ParsedDocumentResponse objects

        """
        result = await self._send_request(
            api_name="download_parsed_document",
            index_name=index_name,
            upload_id=str(upload_id),
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        if not isinstance(result.result, list):
            raise ValueError("Invalid response from Compass API")

        return [ParsedDocumentResponse.convert(data=r) for r in result.result]

    async def insert_docs(
        self,
        *,
        index_name: str,
        docs: Iterable[CompassDocument] | AsyncIterable[CompassDocument],
        max_chunks_per_request: int = DEFAULT_MAX_CHUNKS_PER_REQUEST,
        max_error_rate: float = DEFAULT_MAX_ERROR_RATE,
        errors_sliding_window_size: int | None = 10,
        skip_first_n_docs: int = 0,
        num_jobs: int | None = None,
        authorized_groups: list[str] | None = None,
        merge_groups_on_conflict: bool = False,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Insert multiple parsed documents into an index in Compass.

        :param index_name: the name of the index
        :param docs: the parsed documents
        :param max_chunks_per_request: the maximum number of chunks to send in a single
            API request
        :param max_error_rate: the maximum error rate allowed
        :param errors_sliding_window_size: the size of the sliding window to keep track
            of errors
        :param skip_first_n_docs: number of docs to skip indexing. Useful when insertion
            failed after N documents
        :param num_jobs: the number of parallel jobs to use
        :param authorized_groups: the groups that are authorized to access the
            documents. These groups should exist in RBAC. None passed will make the
            documents public
        :param merge_groups_on_conflict: when doc level security enable, allow upserting
            documents with static groups
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """

        async def put_request(
            data: tuple[
                list[tuple[CompassDocument, Document]], list[dict[str, str]], int
            ],
        ) -> None:
            nonlocal num_succeeded, errors
            request_data, previous_errors, _num_doc = data
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
                await self._send_request(
                    api_name="put_documents",
                    data=put_docs_input,
                    index_name=index_name,
                    max_retries=max_retries,
                    retry_wait=retry_wait,
                    timeout=timeout,
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
                    f"{error_rate * 100}% of insertions failed in the last "
                    f"{errors_sliding_window_size} API calls. Stopping insertion."
                )

        error_window: deque[str | None] = deque(
            maxlen=errors_sliding_window_size
        )  # Keep track of the results of the last N API calls
        num_succeeded = 0
        errors: list[dict[str, str]] = []

        async def docs_async_iter():
            if isinstance(docs, AsyncIterable):
                async for doc in docs:
                    yield doc
            else:
                for doc in docs:
                    yield doc

        requests_iter = partition_documents_async(
            docs_async_iter(), max_chunks_per_request
        )

        try:
            num_jobs = num_jobs or os.cpu_count()
            args = (
                (
                    request_block,
                    previous_errors,
                    i,
                )
                async for i, (request_block, previous_errors) in async_enumerate(
                    requests_iter, 1
                )
                if i > skip_first_n_docs
            )
            await async_apply(put_request, args, num_jobs)
        except CompassMaxErrorRateExceeded as e:
            logger.error(e.message)

        if errors:
            raise CompassInsertionError(errors=errors)

    async def create_datasource(
        self,
        *,
        datasource: CreateDataSource,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> DataSource | str:
        """
        Create a new datasource in Compass.

        :param datasource: the datasource to create
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        result = await self._send_request(
            api_name="create_datasource",
            data=datasource,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return DataSource.model_validate(result.result)

    async def list_datasources(
        self,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> PaginatedList[DataSource] | str:
        """
        List all datasources in Compass.

        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        result = await self._send_request(
            api_name="list_datasources",
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return PaginatedList[DataSource].model_validate(result.result)

    async def get_datasource(
        self,
        *,
        datasource_id: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Get a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        result = await self._send_request(
            api_name="get_datasource",
            datasource_id=datasource_id,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return DataSource.model_validate(result.result)

    async def delete_datasource(
        self,
        *,
        datasource_id: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Delete a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.
        """
        await self._send_request(
            api_name="delete_datasources",
            datasource_id=datasource_id,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def sync_datasource(
        self,
        *,
        datasource_id: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        """
        Sync a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        """
        await self._send_request(
            api_name="sync_datasource",
            datasource_id=datasource_id,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def list_datasources_objects_states(
        self,
        *,
        datasource_id: str,
        skip: int = 0,
        limit: int = 100,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> PaginatedList[DocumentStatus] | str:
        """
        List all objects states in a datasource in Compass.

        :param datasource_id: the id of the datasource
        :param skip: the number of objects to skip
        :param limit: the number of objects to return
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.


        """
        result = await self._send_request(
            api_name="list_datasources_objects_states",
            datasource_id=datasource_id,
            skip=str(skip),
            limit=str(limit),
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return PaginatedList[DocumentStatus].model_validate(result.result)

    async def _search(
        self,
        *,
        api_name: Literal["search_documents", "search_chunks"],
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: list[SearchFilter] | None = None,
        rerank_model: str | None = None,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ):
        return await self._send_request(
            api_name=api_name,
            index_name=index_name,
            data=SearchInput(
                query=query, top_k=top_k, filters=filters, rerank_model=rerank_model
            ),
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

    async def search_documents(
        self,
        *,
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: list[SearchFilter] | None = None,
        rerank_model: str | None = None,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> SearchDocumentsResponse:
        """
        Search documents in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of documents to return
        :param filters: the search filters to apply
        :param rerank_model: the model to use for reranking the results
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            SearchDocumentsResponse object containing the search results

        """
        result = await self._search(
            api_name="search_documents",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
            rerank_model=rerank_model,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return SearchDocumentsResponse.model_validate(result.result)

    async def search_chunks(
        self,
        *,
        index_name: str,
        query: str,
        top_k: int = 10,
        filters: list[SearchFilter] | None = None,
        rerank_model: str | None = None,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> SearchChunksResponse:
        """
        Search chunks in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of chunks to return
        :param filters: the search filters to apply
        :param rerank_model: the model to use for reranking the results
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            SearchChunksResponse object containing the search results

        """
        result = await self._search(
            api_name="search_chunks",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
            rerank_model=rerank_model,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return SearchChunksResponse.model_validate(result.result)

    async def get_document_asset(
        self,
        *,
        index_name: str,
        document_id: str,
        asset_id: str,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> tuple[str | bytes | dict[str, Any], str]:
        """
        Get an asset from a document in Compass.

        :param index_name: the name of the index
        :param document_id: the id of the document
        :param asset_id: the id of the asset
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.


        Returns:
            A tuple of the content and content type of the asset. The variable type of
            the content is either str, bytes, or dict[str, Any], depending on the asset
            type. For example, if the asset is an image, the content type will be bytes;
            if the asset is a markdown, the content type will be str; if the asset is a
            json, the content type will be dict[str, Any].

        Raises:
            CompassError: if the asset cannot be retrieved, either because it doesn't
            exist or the user doesn't have permission to access it.

        """
        result = await self._send_request(
            api_name="get_document_asset",
            index_name=index_name,
            document_id=document_id,
            asset_id=asset_id,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return result.result, result.content_type  # type: ignore

    async def get_asset_presigned_urls(
        self,
        *,
        index_name: str,
        assets: list[AssetPresignedUrlRequest],
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> list[AssetPresignedUrlDetails]:
        """
        Get presigned URLs for assets in documents.

        Retrieves presigned URLs for the specified assets. Each URL has a 1-hour TTL.
        This operation is all-or-nothing: if any requested asset pair is invalid or
        generation fails, the entire request fails.

        :param index_name: the name of the index
        :param assets: list of AssetPresignedUrlRequest objects containing document_id
            and asset_id pairs.
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            A list of AssetPresignedUrlDetails objects in the same order as the input
            assets.

        Raises:
            CompassError: if any asset cannot be found or the user doesn't have READ
                permission on the target documents.
            CompassClientError: if the request body is malformed.

        """
        request_data = GetAssetPresignedUrlsRequest(assets=assets)

        result = await self._send_request(
            api_name="get_asset_presigned_urls",
            index_name=index_name,
            data=request_data,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        response = GetAssetPresignedUrlsResponse.model_validate(result.result)
        return response.asset_urls

    async def update_group_authorization(
        self,
        *,
        index_name: str,
        group_auth_input: GroupAuthorizationInput,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> PutDocumentsResponse:
        """
        Edit group authorization for an index.

        :param index_name: the name of the index
        :param group_auth_input: the group authorization input
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            PutDocumentsResponse object containing the response from the Compass API.

        """
        result = await self._send_request(
            api_name="update_group_authorization",
            index_name=index_name,
            data=group_auth_input,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )
        return PutDocumentsResponse.model_validate(result.result)

    async def direct_search(
        self,
        *,
        index_name: str,
        query: dict[str, Any],
        sort_by: list[SortBy] | None = None,
        size: int = 100,
        scroll: str | None = None,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> DirectSearchResponse:
        """
        Perform a direct search query against the Compass API.

        :pram index_name: the name of the index
        :pram query: the direct search query (e.g. {"match_all": {}})
        :pram sort_by: the sort by criteria
        :pram size: the number of results to return
        :pram scroll: the scroll duration (e.g. "1m" for 1 minute)
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.


        Returns:
            DirectSearchResponse object containing the direct search results

        """
        data = DirectSearchInput(query=query, size=size, scroll=scroll, sort_by=sort_by)

        result = await self._send_request(
            api_name="direct_search",
            index_name=index_name,
            data=data,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return DirectSearchResponse.model_validate(result.result)

    async def direct_search_scroll(
        self,
        *,
        index_name: str,
        scroll_id: str,
        scroll: str = "1m",
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> DirectSearchResponse:
        """
        Continue a search using a scroll ID from a previous direct_search call.

        :param scroll_id: the scroll ID from a previous direct_search call
        :param index_name: the name of the index same as used in direct_search
        :param scroll: the scroll duration (e.g. "1m" for 1 minute)
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        Returns:
            DirectSearchResponse object containing the next batch of search results

        """
        data = DirectSearchScrollInput(scroll_id=scroll_id, scroll=scroll)
        result = await self._send_request(
            api_name="direct_search_scroll",
            index_name=index_name,
            data=data,
            max_retries=max_retries,
            retry_wait=retry_wait,
            timeout=timeout,
        )

        return DirectSearchResponse.model_validate(result.result)

    async def _send_http_request(
        self,
        http_method: str,
        target_path: str,
        data: BaseModel | None = None,
        timeout: timedelta | None = None,
    ):
        timeout = timeout or self.timeout

        data_dict = data.model_dump(mode="json", exclude_none=True) if data else None

        headers = None
        if self.bearer_token:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

        if http_method == "GET":
            response = await self.httpx_client.get(
                target_path,
                headers=headers,
                timeout=timeout.total_seconds(),
            )
        elif http_method == "POST":
            response = await self.httpx_client.post(
                target_path,
                json=data_dict,
                headers=headers,
                timeout=timeout.total_seconds(),
            )
        elif http_method == "PUT":
            response = await self.httpx_client.put(
                target_path,
                json=data_dict,
                headers=headers,
                timeout=timeout.total_seconds(),
            )
        elif http_method == "DELETE":
            response = await self.httpx_client.delete(
                target_path,
                headers=headers,
                timeout=timeout.total_seconds(),
            )
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
        return _SendRequestResult(
            result=result,
            content_type=content_type,
        )

    # todo Simplify this method so we don't have to ignore the C901 complexity warning.
    async def _send_request(
        self,
        api_name: str,
        data: BaseModel | None = None,
        max_retries: int | None = None,
        retry_wait: timedelta | None = None,
        timeout: timedelta | None = None,
        **url_params: str,
    ) -> _SendRequestResult:
        """
        Send a request to the Compass API.

        :param function: the function to call
        :param index_name: the name of the index
        :param data: the data to send
        :param max_retries: Maximum number of retries for failed requests. If not
            provided, the default from the client will be used.
        :param retry_wait: Time to wait between retries. If not provided, the default
            from the client will be used.
        :param timeout: Request timeout duration. If not provided, the default from the
            client will be used.

        :return: An error message if the request failed, otherwise None.
        """
        max_retries = max_retries or self.max_retries
        retry_wait = retry_wait or self.retry_wait
        timeout = timeout or self.timeout

        if api_name not in API_DEFINITIONS:
            raise CompassError(
                f"API name '{api_name}' is not defined in the API definitions."
            )
        http_method, api_path = API_DEFINITIONS[api_name]

        target_path = f"{self.index_url}v1/{api_path}"
        target_path = target_path.format(**url_params)

        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_fixed(retry_wait),
            reraise=True,  # re-raise last exception instead of wrapping in RetryError
            # todo find alternative to InvalidSchema
            retry=retry_if_not_exception_type((CompassClientError,)),
        )
        async def _send_request_with_retry() -> _SendRequestResult:
            try:
                return await self._send_http_request(
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
            return await _send_request_with_retry()
        except Exception as e:
            raise CompassError(f"Failed to send request for {api_name} API") from e
