# Python imports
from collections import deque
from dataclasses import dataclass
from statistics import mean
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple, Union
import base64
import logging
import os
import threading
import uuid

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
import requests

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
    PaginatedList,
    ParseableDocument,
    PushDocumentsInput,
    PutDocumentsInput,
    SearchChunksResponse,
    SearchDocumentsResponse,
    SearchFilter,
    SearchInput,
)


@dataclass
class RetryResult:
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None


_DEFAULT_TIMEOUT = 30


logger = logging.getLogger(__name__)


class SessionWithDefaultTimeout(requests.Session):
    def __init__(self, timeout: int):
        self._timeout = timeout
        super().__init__()

    def request(self, *args: Any, **kwargs: Any):
        kwargs.setdefault("timeout", self._timeout)
        return super().request(*args, **kwargs)


class CompassClient:
    def __init__(
        self,
        *,
        index_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        bearer_token: Optional[str] = None,
        default_timeout: int = _DEFAULT_TIMEOUT,
    ):
        """
        A compass client to interact with the Compass API
        :param index_url: the url of the Compass instance
        :param username: the username for the Compass instance
        :param password: the password for the Compass instance
        """
        self.index_url = index_url
        self.username = username or os.getenv("COHERE_COMPASS_USERNAME")
        self.password = password or os.getenv("COHERE_COMPASS_PASSWORD")
        self.session = SessionWithDefaultTimeout(default_timeout)
        self.bearer_token = bearer_token

        self.api_method = {
            "create_index": self.session.put,
            "list_indexes": self.session.get,
            "delete_index": self.session.delete,
            "delete_document": self.session.delete,
            "get_document": self.session.get,
            "put_documents": self.session.put,
            "search_documents": self.session.post,
            "search_chunks": self.session.post,
            "add_attributes": self.session.post,
            "refresh": self.session.post,
            "upload_documents": self.session.post,
            "edit_group_authorization": self.session.post,
            # Data Sources APIs
            "create_datasource": self.session.post,
            "list_datasources": self.session.get,
            "delete_datasources": self.session.delete,
            "get_datasource": self.session.get,
            "sync_datasource": self.session.post,
            "list_datasources_objects_states": self.session.get,
        }
        self.api_endpoint = {
            "create_index": "/api/v1/indexes/{index_name}",
            "list_indexes": "/api/v1/indexes",
            "delete_index": "/api/v1/indexes/{index_name}",
            "delete_document": "/api/v1/indexes/{index_name}/documents/{document_id}",
            "get_document": "/api/v1/indexes/{index_name}/documents/{document_id}",
            "put_documents": "/api/v1/indexes/{index_name}/documents",
            "search_documents": "/api/v1/indexes/{index_name}/documents/_search",
            "search_chunks": "/api/v1/indexes/{index_name}/documents/_search_chunks",
            "add_attributes": "/api/v1/indexes/{index_name}/documents/{document_id}/_add_attributes",
            "refresh": "/api/v1/indexes/{index_name}/_refresh",
            "upload_documents": "/api/v1/indexes/{index_name}/documents/_upload",
            "edit_group_authorization": "/api/v1/indexes/{index_name}/group_authorization",
            # Data Sources APIs
            "create_datasource": "/api/v1/datasources",
            "list_datasources": "/api/v1/datasources",
            "delete_datasources": "/api/v1/datasources/{datasource_id}",
            "get_datasource": "/api/v1/datasources/{datasource_id}",
            "sync_datasource": "/api/v1/datasources/{datasource_id}/_sync",
            "list_datasources_objects_states": "/api/v1/datasources/{datasource_id}/documents?skip={skip}&limit={limit}",
        }

    def create_index(self, *, index_name: str):
        """
        Create an index in Compass
        :param index_name: the name of the index
        :return: the response from the Compass API
        """
        return self._send_request(
            api_name="create_index",
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
            index_name=index_name,
        )

    def refresh(self, *, index_name: str):
        """
        Refresh index
        :param index_name: the name of the index
        :return: the response from the Compass API
        """
        return self._send_request(
            api_name="refresh",
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
            index_name=index_name,
        )

    def delete_index(self, *, index_name: str):
        """
        Delete an index from Compass
        :param index_name: the name of the index
        :return: the response from the Compass API
        """
        return self._send_request(
            api_name="delete_index",
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
            index_name=index_name,
        )

    def delete_document(self, *, index_name: str, document_id: str):
        """
        Delete a document from Compass
        :param index_name: the name of the index
        :document_id: the id of the document
        :return: the response from the Compass API
        """
        return self._send_request(
            api_name="delete_document",
            document_id=document_id,
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
            index_name=index_name,
        )

    def get_document(self, *, index_name: str, document_id: str):
        """
        Get a document from Compass
        :param index_name: the name of the index
        :document_id: the id of the document
        :return: the response from the Compass API
        """
        return self._send_request(
            api_name="get_document",
            document_id=document_id,
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
            index_name=index_name,
        )

    def list_indexes(self):
        """
        List all indexes in Compass
        :return: the response from the Compass API
        """
        return self._send_request(
            api_name="list_indexes",
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
            index_name="",
        )

    def add_attributes(
        self,
        *,
        index_name: str,
        document_id: str,
        context: dict[str, Any],
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ) -> Optional[RetryResult]:
        """
        Update the content field of an existing document with additional context

        :param index_name: the name of the index
        :param document_id: the document to modify
        :param context: A dictionary of key:value pairs to insert into the content field of a document
        :param max_retries: the maximum number of times to retry a doc insertion
        :param sleep_retry_seconds: number of seconds to go to sleep before retrying a doc insertion
        """

        return self._send_request(
            api_name="add_attributes",
            document_id=document_id,
            data=context,
            max_retries=max_retries,
            sleep_retry_seconds=sleep_retry_seconds,
            index_name=index_name,
        )

    def insert_doc(
        self,
        *,
        index_name: str,
        doc: CompassDocument,
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
        authorized_groups: Optional[List[str]] = None,
        merge_groups_on_conflict: bool = False,
    ) -> Optional[List[Dict[str, str]]]:
        """
        Insert a parsed document into an index in Compass
        :param index_name: the name of the index
        :param doc: the parsed compass document
        :param max_retries: the maximum number of times to retry a doc insertion
        :param sleep_retry_seconds: number of seconds to go to sleep before retrying a doc insertion
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
        attributes: Dict[str, Any] = {},
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ) -> Optional[Union[str, Dict[str, Any]]]:
        """
        Parse and insert a document into an index in Compass
        :param index_name: the name of the index
        :param filename: the filename of the document
        :param filebytes: the bytes of the document
        :param content_type: the content type of the document
        :param document_id: the id of the document (optional)
        :param context: represents an additional information about the document
        :param max_retries: the maximum number of times to retry a request if it fails
        :param sleep_retry_seconds: the number of seconds to wait before retrying an API request
        :return: an error message if the request failed, otherwise None
        """
        if len(filebytes) > DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES:
            err = f"File too large, supported file size is {DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES / 1000_000} mb"
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
            data=PushDocumentsInput(documents=[doc]),
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
        errors_sliding_window_size: Optional[int] = 10,
        skip_first_n_docs: int = 0,
        num_jobs: Optional[int] = None,
        authorized_groups: Optional[List[str]] = None,
        merge_groups_on_conflict: bool = False,
    ) -> Optional[List[Dict[str, str]]]:
        """
        Insert multiple parsed documents into an index in Compass
        :param index_name: the name of the index
        :param docs: the parsed documents
        :param max_chunks_per_request: the maximum number of chunks to send in a single API request
        :param num_jobs: the number of parallel jobs to use
        :param max_error_rate: the maximum error rate allowed
        :param max_retries: the maximum number of times to retry a request if it fails
        :param sleep_retry_seconds: the number of seconds to wait before retrying an API request
        :param errors_sliding_window_size: the size of the sliding window to keep track of errors
        :param skip_first_n_docs: number of docs to skip indexing. Useful when insertion failed after N documents
        :param authorized_groups: the groups that are authorized to access the documents. These groups should exist in RBAC. None passed will make the documents public
        :param merge_groups_on_conflict: when doc level security enable, allow upserting documents with static groups
        """

        def put_request(
            request_data: list[Tuple[CompassDocument, Document]],
            previous_errors: list[dict[str, str]],
            num_doc: int,
        ) -> None:
            nonlocal num_succeeded, errors
            errors.extend(previous_errors)
            compass_docs: List[CompassDocument] = [
                compass_doc for compass_doc, _ in request_data
            ]
            put_docs_input = PutDocumentsInput(
                documents=[input_doc for _, input_doc in request_data],
                authorized_groups=authorized_groups,
                merge_groups_on_conflict=merge_groups_on_conflict,
            )

            # It could be that all documents have errors, in which case we should not send a request
            # to the Compass Server. This is a common case when the parsing of the documents fails.
            # In this case, only errors will appear in the insertion_docs response
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
                    doc.errors.append(
                        {
                            CompassSdkStage.Indexing: f"{doc.metadata.filename}: {results.error}"
                        }
                    )
                    errors.append(
                        {
                            doc.metadata.document_id: f"{doc.metadata.filename}: {results.error}"
                        }
                    )
            else:
                num_succeeded += len(compass_docs)

            # Keep track of the results of the last N API calls to calculate the error rate
            # If the error rate is higher than the threshold, stop the insertion process
            error_window.append(results.error)
            error_rate = (
                mean([1 if x else 0 for x in error_window])
                if len(error_window) == error_window.maxlen
                else 0
            )
            if error_rate > max_error_rate:
                raise CompassMaxErrorRateExceeded(
                    f"[Thread {threading.get_native_id()}]{error_rate * 100}% of insertions failed "
                    f"in the last {errors_sliding_window_size} API calls. Stopping the insertion process."
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ):
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ) -> Union[PaginatedList[DataSource], str]:
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ):
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ):
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ):
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
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep_retry_seconds: int = DEFAULT_SLEEP_RETRY_SECONDS,
    ) -> Union[PaginatedList[DocumentStatus], str]:
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
        Create request blocks to send to the Compass API
        :param docs: the documents to send
        :param max_chunks_per_request: the maximum number of chunks to send in a single API request
        :return: an iterator over the request blocks
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
                    errors.append({doc.metadata.document_id: list(error.values())[0]})
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
        filters: Optional[List[SearchFilter]] = None,
    ):
        """
        Search your Compass index
        :param index_name: the name of the index
        :param query: query to search for
        :param top_k: number of documents to return
        """
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
        filters: Optional[List[SearchFilter]] = None,
    ) -> SearchDocumentsResponse:
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
        filters: Optional[List[SearchFilter]] = None,
    ) -> SearchChunksResponse:
        result = self._search(
            api_name="search_chunks",
            index_name=index_name,
            query=query,
            top_k=top_k,
            filters=filters,
        )

        return SearchChunksResponse.model_validate(result.result)

    def edit_group_authorization(
        self, *, index_name: str, group_auth_input: GroupAuthorizationInput
    ):
        """
        Edit group authorization for an index
        :param index_name: the name of the index
        :param group_auth_input: the group authorization input
        """
        return self._send_request(
            api_name="edit_group_authorization",
            index_name=index_name,
            data=group_auth_input,
            max_retries=DEFAULT_MAX_RETRIES,
            sleep_retry_seconds=DEFAULT_SLEEP_RETRY_SECONDS,
        )

    def _send_request(
        self,
        api_name: str,
        max_retries: int,
        sleep_retry_seconds: int,
        data: Optional[Union[Dict[str, Any], BaseModel]] = None,
        **url_params: str,
    ) -> RetryResult:
        """
        Send a request to the Compass API
        :param function: the function to call
        :param index_name: the name of the index
        :param max_retries: the number of times to retry the request
        :param sleep_retry_seconds: the number of seconds to sleep between retries
        :param data: the data to send
        :return: An error message if the request failed, otherwise None
        """

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
                data_dict = None
                if data:
                    if isinstance(data, BaseModel):
                        data_dict = data.model_dump(mode="json")
                    else:
                        data_dict = data

                headers = None
                auth = None
                if self.username and self.password:
                    auth = (self.username, self.password)
                if self.bearer_token:
                    headers = {"Authorization": f"Bearer {self.bearer_token}"}
                    auth = None

                response = self.api_method[api_name](
                    target_path, json=data_dict, auth=auth, headers=headers
                )

                if response.ok:
                    error = None
                    result = response.json() if response.text else None
                    return RetryResult(result=result, error=None)
                else:
                    response.raise_for_status()

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    error = "Unauthorized. Please check your username and password."
                    raise CompassAuthError(message=str(e))
                elif 400 <= e.response.status_code < 500:
                    error = f"Client error occurred: {e.response.text}"
                    raise CompassClientError(message=error)
                else:
                    error = str(e) + " " + e.response.text
                    logger.error(
                        f"Failed to send request to {api_name} {target_path}: {type(e)} {error}. Going to sleep for "
                        f"{sleep_retry_seconds} seconds and retrying."
                    )
                    raise e

            except Exception as e:
                error = str(e)
                logger.error(
                    f"Failed to send request to {api_name} {target_path}: {type(e)} {error}. Going to sleep for "
                    f"{sleep_retry_seconds} seconds and retrying."
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
                return RetryResult(result=None, error=error)
        except RetryError:
            logger.error(
                f"Failed to send request after {max_retries} attempts. Aborting."
            )
            return RetryResult(result=None, error=error)
