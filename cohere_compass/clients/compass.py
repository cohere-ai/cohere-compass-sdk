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
from typing import Any, Literal, Optional, Union

import requests
from deprecated import deprecated

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
from cohere_compass import (
    GroupAuthorizationInput,
)
from cohere_compass.constants import (
    DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES,
    DEFAULT_MAX_CHUNKS_PER_REQUEST,
    DEFAULT_MAX_ERROR_RATE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_RETRY_SECONDS,
)
from cohere_compass.exceptions import (
    CompassAuthError,
    CompassClientError,
    CompassError,
    CompassMaxErrorRateExceeded,
)
from cohere_compass.models import (
    Chunk,
    CompassDocument,
    CompassDocumentStatus,
    CompassSdkStage,
    CreateDataSource,
    DataSource,
    DirectSearchInput,
    DirectSearchResponse,
    DirectSearchScrollInput,
    Document,
    DocumentStatus,
    IndexDetails,
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


@dataclass
class _RetryResult:
    """
    A class to represent the result of a retryable operation.

    The class contains the following fields:
    - result: The result of the operation if successful, otherwise None.
    - error (Optional[str]): The error message if the operation failed, otherwise None.

    Notice that this is an internal class and should not be exposed to clients.
    """

    result: Optional[Union[str, bytes, dict[str, Any]]] = None
    content_type: Optional[str] = None
    error: Optional[str] = None


logger = logging.getLogger(__name__)


class CompassClient:
    """A compass client to interact with the Compass API."""

    def __init__(
        self,
        *,
        index_url: str,
        bearer_token: Optional[str] = None,
        http_session: Optional[requests.Session] = None,
        default_max_retries: int = DEFAULT_MAX_RETRIES,
        default_sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
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
        self._thread_local = threading.local()

        self.index_url = index_url
        self.session = http_session or requests.Session()

        self.bearer_token = bearer_token

        if default_max_retries < 0:
            raise ValueError("default_max_retries must be a non-negative integer.")
        if default_sleep_retry_seconds < 0:
            raise ValueError(
                "default_sleep_retry_seconds must be a non-negative integer."
            )
        self.default_max_retries = default_max_retries
        self.default_sleep_retry_seconds = default_sleep_retry_seconds
        self.api_method = {
            "create_index": self._put,
            "get_index_details": self._get,
            "list_indexes": self._get,
            "delete_index": self._delete,
            "delete_document": self._delete,
            "get_document": self._get,
            "put_documents": self._put,
            "search_documents": self._post,
            "search_chunks": self._post,
            "get_document_asset": self._get,
            "add_attributes": self._post,
            "refresh": self._post,
            "upload_documents": self._post,
            "update_group_authorization": self._post,
            "direct_search": self._post,
            "direct_search_scroll": self._post,
            "direct_search_scroll_with_index": self._post,
            # Data Sources APIs
            "create_datasource": self._post,
            "list_datasources": self._get,
            "delete_datasources": self._delete,
            "get_datasource": self._get,
            "sync_datasource": self._post,
            "list_datasources_objects_states": self._get,
            "get_models": self._get,
        }
        base_api = "/api" if include_api_in_url else ""
        self.api_endpoint = {
            "create_index": f"{base_api}/v1/indexes/{{index_name}}",
            "get_index_details": f"{base_api}/v1/indexes/{{index_name}}",
            "list_indexes": f"{base_api}/v1/indexes",
            "delete_index": f"{base_api}/v1/indexes/{{index_name}}",
            "delete_document": f"{base_api}/v1/indexes/{{index_name}}/documents/{{document_id}}",  # noqa: E501
            "get_document": f"{base_api}/v1/indexes/{{index_name}}/documents/{{document_id}}",  # noqa: E501
            "put_documents": f"{base_api}/v1/indexes/{{index_name}}/documents",
            "search_documents": f"{base_api}/v1/indexes/{{index_name}}/documents/_search",  # noqa: E501
            "search_chunks": f"{base_api}/v1/indexes/{{index_name}}/documents/_search_chunks",  # noqa: E501
            "get_document_asset": f"{base_api}/v1/indexes/{{index_name}}/documents/{{document_id}}/assets/{{asset_id}}",  # noqa: E501
            "add_attributes": f"{base_api}/v1/indexes/{{index_name}}/documents/{{document_id}}/_add_attributes",  # noqa: E501
            "refresh": f"{base_api}/v1/indexes/{{index_name}}/_refresh",
            "upload_documents": f"{base_api}/v1/indexes/{{index_name}}/documents/_upload",  # noqa: E501
            "update_group_authorization": f"{base_api}/v1/indexes/{{index_name}}/group_authorization",  # noqa: E501
            "direct_search": f"{base_api}/v1/indexes/{{index_name}}/_direct_search",
            "direct_search_scroll": f"{base_api}/v1/indexes/_direct_search/scroll",
            "direct_search_scroll_with_index": f"{base_api}/v1/indexes/{{index_name}}/_direct_search/scroll",  # noqa: E501
            # Data Sources APIs
            "create_datasource": f"{base_api}/v1/datasources",
            "list_datasources": f"{base_api}/v1/datasources",
            "delete_datasources": f"{base_api}/v1/datasources/{{datasource_id}}",
            "get_datasource": f"{base_api}/v1/datasources/{{datasource_id}}",
            "sync_datasource": f"{base_api}/v1/datasources/{{datasource_id}}/_sync",
            "list_datasources_objects_states": f"{base_api}/v1/datasources/{{datasource_id}}/documents?skip={{skip}}&limit={{limit}}",  # noqa: E501
            "get_models": f"{base_api}/v1/config/models",
        }

    def _get(self, *args: Any, **kwargs: Any):
        return self._get_session().get(*args, **kwargs)

    def _post(self, *args: Any, **kwargs: Any):
        return self._get_session().post(*args, **kwargs)

    def _put(self, *args: Any, **kwargs: Any):
        return self._get_session().put(*args, **kwargs)

    def _delete(self, *args: Any, **kwargs: Any):
        return self._get_session().delete(*args, **kwargs)

    def _get_session(self) -> requests.Session:
        if not hasattr(self._thread_local, "session"):
            self._thread_local.session = requests.Session()
        return self._thread_local.session

    def get_models(
        self,
        *,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> dict[str, list[str]]:
        """
        Get the models available in Compass.

        :returns: a dictionary with the models available in Compass, where the keys are
            the model roles ("dense", "rerank", "sparse") and the values are lists of
            model versions for each role.

        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        """
        result = self._send_request(
            api_name="get_models",
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )
        if result.error:
            raise CompassError(result.error)
        return result.result  # type: ignore

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

    def get_index_details(
        self,
        *,
        index_name: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> IndexDetails:
        """
        Get the details of an index in Compass.

        :param index_name: the name of the index
        :returns: the response from the Compass API
        """
        result = self._send_request(
            api_name="get_index_details",
            index_name=index_name,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )
        if result.error:
            raise CompassError(result.error)
        return result.result  # type: ignore

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

        return result.result  # type: ignore

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
            # Parser returns a tuple[str, Exception] in case of a error
            # Example, if service is not reachable or document times out when parsing
            if isinstance(doc, tuple):
                logger.error(f"Document has error when parsing: {doc[0]}, {doc[1]}")
                errors.append({doc[0]: doc[1]})
            elif doc.status != CompassDocumentStatus.Success:
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
        rerank_model: Optional[str] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ):
        return self._send_request(
            api_name=api_name,
            index_name=index_name,
            data=SearchInput(
                query=query, top_k=top_k, filters=filters, rerank_model=rerank_model
            ),
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
        rerank_model: Optional[str] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> SearchDocumentsResponse:
        """
        Search documents in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of documents to return
        :param filters: the search filters to apply
        :param rerank_model: the model to use for reranking the results

        :returns: the search results
        """
        result = self._search(
            api_name="search_documents",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
            rerank_model=rerank_model,
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
        rerank_model: Optional[str] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> SearchChunksResponse:
        """
        Search chunks in an index.

        :param index_name: the name of the index
        :param query: the search query
        :param top_k: the number of chunks to return
        :param filters: the search filters to apply
        :param rerank_model: the model to use for reranking the results

        :returns: the search results
        """
        result = self._search(
            api_name="search_chunks",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
            rerank_model=rerank_model,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            raise CompassError(result.error)

        return SearchChunksResponse.model_validate(result.result)

    def get_document_asset(
        self,
        *,
        index_name: str,
        document_id: str,
        asset_id: str,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> tuple[Union[str, bytes, dict[str, Any]], str]:
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
            max_retries=1,
            sleep_retry_seconds=1,
        )

        if result.error:
            raise CompassError(result.error)

        return result.result, result.content_type  # type: ignore

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

    def direct_search(
        self,
        *,
        index_name: str,
        query: dict[str, Any],
        size: int = 100,
        scroll: Optional[str] = None,
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> DirectSearchResponse:
        """
        Perform a direct search query against the Compass API.

        :param index_name: the name of the index
        :param query: the direct search query (e.g. {"match_all": {}})
        :param size: the number of results to return
        :param scroll: the scroll duration (e.g. "1m" for 1 minute)
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries

        :returns: the direct search results
        :raises CompassError: if the search fails
        """
        data = DirectSearchInput(query=query, size=size, scroll=scroll)

        result = self._send_request(
            api_name="direct_search",
            index_name=index_name,
            data=data,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            raise CompassError(result.error)

        return DirectSearchResponse.model_validate(result.result)

    @deprecated(
        "Direct search scroll is deprecated, "
        "use direct_search_scroll_with_index instead"
    )
    def direct_search_scroll(
        self,
        *,
        scroll_id: str,
        scroll: str = "1m",
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> DirectSearchResponse:
        """
        Continue a search using a scroll ID from a previous direct_search call.

        :param scroll_id: the scroll ID from a previous direct_search call
        :param scroll: the scroll duration (e.g. "1m" for 1 minute)
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries

        :returns: the next batch of search results
        :raises CompassError: if the scroll search fails
        """
        data = DirectSearchScrollInput(scroll_id=scroll_id, scroll=scroll)

        result = self._send_request(
            api_name="direct_search_scroll",
            data=data,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )

        if result.error:
            raise CompassError(result.error)

        return DirectSearchResponse.model_validate(result.result)

    def direct_search_scroll_with_index(
        self,
        *,
        scroll_id: str,
        index_name: str,
        scroll: str = "1m",
        max_retries: Optional[int] = None,
        sleep_retry_seconds: Optional[int] = None,
    ) -> DirectSearchResponse:
        """
        Continue a search using a scroll ID from a previous direct_search call.

        :param scroll_id: the scroll ID from a previous direct_search call
        :param index_name: the name of the index same as used in direct_search
        :param scroll: the scroll duration (e.g. "1m" for 1 minute)
        :param max_retries: the maximum number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries

        :returns: the next batch of search results
        :raises CompassError: if the scroll search fails
        """
        data = DirectSearchScrollInput(scroll_id=scroll_id, scroll=scroll)
        result = self._send_request(
            api_name="direct_search_scroll_with_index",
            index_name=index_name,
            data=data,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
        )
        if result.error:
            raise CompassError(result.error)
        return DirectSearchResponse.model_validate(result.result)

    # todo Simplify this method so we don't have to ignore the C901 complexity warning.
    def _send_request(  # noqa: C901
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
        if not max_retries:
            max_retries = self.default_max_retries
        if not sleep_retry_seconds:
            sleep_retry_seconds = self.default_sleep_retry_seconds
        if max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer.")
        if sleep_retry_seconds < 0:
            raise ValueError("sleep_retry_seconds must be a non-negative integer.")

        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_fixed(sleep_retry_seconds),
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
                data_dict = (
                    data.model_dump(mode="json", exclude_none=True) if data else None
                )

                headers = None
                if self.bearer_token:
                    headers = {"Authorization": f"Bearer {self.bearer_token}"}

                response = self.api_method[api_name](
                    target_path, json=data_dict, headers=headers
                )

                if response.ok:
                    error = None
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
                    return _RetryResult(
                        result=result,
                        content_type=content_type,
                        error=None,
                    )
                else:
                    response.raise_for_status()

            except requests.exceptions.HTTPError as e:
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
                        f"{sleep_retry_seconds} seconds and retrying."
                    )
                    raise e

            except ConnectionAbortedError as e:
                raise CompassClientError(message=str(e), code=None)

            except Exception as e:
                error = str(e)
                logger.warning(
                    f"Failed to send request to {api_name} {target_path}: {type(e)} "
                    f"{error}. Sleeping for {sleep_retry_seconds} before retrying..."
                )
                raise e

        error = None
        try:
            target_path = self.index_url + self.api_endpoint[api_name].format(
                **url_params
            )
            res = _send_request_with_retry()
            if res:
                return res
            else:
                return _RetryResult(result=None, error=error)
        except RetryError:
            logger.error(
                f"Failed to send request after {max_retries} attempts. Aborting."
            )
            return _RetryResult(result=None, error=error)
