"""
Async parser client for document parsing operations.

This module provides the CompassParserAsyncClient for asynchronous document
parsing operations with support for multiple file formats and filesystems.
"""

# Python imports
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any

# 3rd party imports
import httpx
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_fixed,
)

# Local imports
from cohere_compass import (
    ProcessFileParameters,
)
from cohere_compass.constants import (
    DEFAULT_COMPASS_PARSER_CLIENT_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_WAIT,
)
from cohere_compass.exceptions import CompassClientError, CompassError
from cohere_compass.models import (
    CompassDocument,
    MetadataConfig,
    ParserConfig,
)
from cohere_compass.utils import async_map, open_document, scan_folder

Fn_or_Dict = dict[str, Any] | Callable[[CompassDocument], dict[str, Any]]


logger = logging.getLogger(__name__)


class CompassParserAsyncClient:
    """
    Client to interact with the CompassParser API.

    It allows to process files using the parser and metadata configurations specified in
    the parameters. The client is stateful, that is, it can be initialized with parser
    and metadata configurations that will be used for all subsequent files processed by
    the client.  Also, independently of the default configurations, the client allows to
    pass specific configurations for each file when calling the process_file or
    process_files methods.  The client is responsible for opening the files and sending
    them to the CompassParser API for processing. The resulting documents are returned
    as CompassDocument objects.

    :param parser_url: URL of the CompassParser API
    :param parser_config: Default parser configuration to use when processing files
    :param metadata_config: Default metadata configuration to use when processing files
    """

    def __init__(
        self,
        *,
        parser_url: str,
        parser_config: ParserConfig = ParserConfig(),
        metadata_config: MetadataConfig = MetadataConfig(),
        bearer_token: str | None = None,
        num_workers: int = 1,
        timeout: timedelta | None = None,
        httpx_client: httpx.AsyncClient | None = None,
    ):
        """
        Initialize the CompassParserClient.

        The parser_config and metadata_config are optional, and if not provided, the
        default configurations will be used. If the parser/metadata configs are
        provided, they will be used for all subsequent files processed by the client
        unless specific configs are passed when calling the process_file or
        process_files methods.

        :param parser_url: the URL of the CompassParser API
        :param parser_config: the parser configuration to use when processing files if
            no parser configuration is specified in the method calls (process_file or
            process_files)
        :param metadata_config: the metadata configuration to use when processing files
            if no metadata configuration is specified in the method calls (process_file
            or process_files)
        :param bearer_token (optional): The bearer token for authentication.
        :param num_workers (optional): The number of workers to use for processing
            files.
        :param timeout (optional): The timeout to use for the httpx client. Default is
            DEFAULT_COMPASS_PARSER_CLIENT_TIMEOUT.
        :param httpx_client (optional): The httpx client to use for making requests.
            If not provided, a new httpx client will be created with the timeout set
            when creating the client. If an httpx client is provided, the timeout will
            be ignored.
        """
        self.parser_url = (
            parser_url if not parser_url.endswith("/") else parser_url[:-1]
        )
        self.parser_config = parser_config
        self.bearer_token = bearer_token
        self.thread_pool = ThreadPoolExecutor(num_workers)
        self.num_workers = num_workers
        self.timeout = (
            timeout
            if timeout is not None
            else DEFAULT_COMPASS_PARSER_CLIENT_TIMEOUT
            if httpx_client is None
            else timedelta(seconds=httpx_client.timeout.read)
            if httpx_client.timeout.read
            else DEFAULT_COMPASS_PARSER_CLIENT_TIMEOUT
        )
        self.httpx = httpx_client or httpx.AsyncClient(
            timeout=self.timeout.total_seconds()
        )

        self.metadata_config = metadata_config
        logger.info(
            f"CompassParserClient initialized with parser_url: {self.parser_url}"
        )

    def process_folder(
        self,
        *,
        folder_path: str,
        allowed_extensions: list[str] | None = None,
        recursive: bool = False,
        parser_config: ParserConfig | None = None,
        metadata_config: MetadataConfig | None = None,
        custom_context: Fn_or_Dict | None = None,
    ):
        """
        Process all the files in the specified folder.

        The files are processed using the default parser and metadata configurations
        passed when creating the client. The method iterates over all the files in the
        folder and processes them using the process_file method. The resulting documents
        are returned as a list of CompassDocument objects.

        :param folder_path: the folder to process
        :param allowed_extensions: the list of allowed extensions to process
        :param recursive: whether to process the folder recursively
        :param parser_config: the parser configuration to use when processing files if
            no parser configuration is specified in the method calls (process_file or
            process_files)
        :param metadata_config: the metadata configuration to use when processing files
            if no metadata configuration is specified in the method calls (process_file
            or process_files)
        :param custom_context: Additional data to add to compass document. Fields will
            be filterable but not semantically searchable.  Can either be a dictionary
            or a callable that takes a CompassDocument and returns a dictionary.

        :return: the list of processed documents
        """
        filenames = scan_folder(
            folder_path=folder_path,
            allowed_extensions=allowed_extensions,
            recursive=recursive,
        )
        return self.process_files(
            filenames=filenames,
            parser_config=parser_config,
            metadata_config=metadata_config,
            custom_context=custom_context if custom_context else None,
        )

    async def process_files(
        self,
        *,
        filenames: list[str],
        file_ids: list[str] | None = None,
        parser_config: ParserConfig | None = None,
        metadata_config: MetadataConfig | None = None,
        custom_context: Fn_or_Dict | None = None,
    ):
        """
        Process a list of files.

        If the parser/metadata configs are not provided, then the default configs passed
        by parameter when creating the client will be used. This makes the
        CompassParserClient stateful. That is, we can set the parser/metadata configs
        only once when creating the parser client, and process all subsequent files
        without having to pass the config every time.

        All the documents passed as filenames and opened to obtain their bytes. Then,
        they are packed into a ProcessFilesParameters object that contains a list of
        ProcessFileParameters, each contain a file, its id, and the parser/metadata
        config.

        :param filenames: List of filenames to process
        :param file_ids: List of ids for the files
        :param parser_config: ParserConfig object (applies the same config to all docs)
        :param metadata_config: MetadataConfig object (applies the same config to all
            docs)
        :param custom_context: Additional data to add to compass document. Fields will
            be filterable but not semantically searchable.  Can either be a dictionary
            or a callable that takes a CompassDocument and returns a dictionary.

        :return: List of processed documents
        """

        async def process_file(i: int) -> list[CompassDocument] | tuple[str, Exception]:
            filename = filenames[i]
            try:
                return await self.process_file(
                    filename=filename,
                    file_id=file_ids[i] if file_ids else None,
                    parser_config=parser_config,
                    metadata_config=metadata_config,
                    custom_context=custom_context,
                )
            except Exception as e:
                return filename, e

        for results in await async_map(
            process_file,
            range(len(filenames)),
            self.num_workers,
        ):
            if isinstance(results, list):
                for r in results:
                    yield r
            else:
                yield results

    @staticmethod
    def _get_metadata(
        doc: CompassDocument, custom_context: Fn_or_Dict | None = None
    ) -> dict[str, Any]:
        if custom_context is None:
            return {}
        elif callable(custom_context):
            return custom_context(doc)
        else:
            return custom_context

    @retry(
        stop=stop_after_attempt(DEFAULT_MAX_RETRIES),
        wait=wait_fixed(DEFAULT_RETRY_WAIT),
        # todo find alternative to InvalidSchema
        retry=retry_if_not_exception_type((CompassClientError,)),
        reraise=True,
    )
    async def process_file(
        self,
        *,
        filename: str,
        file_id: str | None = None,
        content_type: str | None = None,
        parser_config: ParserConfig | None = None,
        metadata_config: MetadataConfig | None = None,
        custom_context: Fn_or_Dict | None = None,
    ) -> list[CompassDocument]:
        """
        Process a file.

        The method takes in a file, its id, and the parser/metadata config. If the
        config is None, then it uses the default configs passed by parameter when
        creating the client.  This makes the CompassParserClient stateful for
        convenience, that is, one can pass in the parser/metadata config only once when
        creating the CompassParserClient, and process files without having to pass the
        config every time.

        :param filename: Filename to process.
        :param file_id: Id for the file.
        :param content_type: Content type of the file.
        :param parser_config: ParserConfig object with the config to use for parsing the
            file.
        :param metadata_config: MetadataConfig object with the config to use for
            extracting metadata for each document.
        :param custom_context: Additional data to add to compass document. Fields will
            be filterable but not semantically searchable.  Can either be a dictionary
            or a callable that takes a CompassDocument and returns a dictionary.

        :return: List of resulting documents
        """
        doc = open_document(filename)
        if doc.errors:
            logger.error(f"Error opening document: {doc.errors}")
            return []

        return await self._process_file_bytes(
            params=self._get_file_params(
                parser_config=parser_config,
                metadata_config=metadata_config,
                file_id=file_id,
                content_type=content_type,
            ),
            filename=filename,
            file_bytes=doc.filebytes,
            custom_context=custom_context,
        )

    @retry(
        stop=stop_after_attempt(DEFAULT_MAX_RETRIES),
        wait=wait_fixed(DEFAULT_RETRY_WAIT),
        # todo find alternative to InvalidSchema
        retry=retry_if_not_exception_type((CompassClientError,)),
        reraise=True,
    )
    async def process_file_bytes(
        self,
        *,
        filename: str,
        file_bytes: bytes,
        file_id: str | None = None,
        content_type: str | None = None,
        parser_config: ParserConfig | None = None,
        metadata_config: MetadataConfig | None = None,
        custom_context: Fn_or_Dict | None = None,
        timeout: timedelta | None = None,
    ) -> list[CompassDocument]:
        """
        Process a file.

        The method takes in a file, its id, its byte array,
        and the parser/metadata config.
        If the config is None, then it uses the default configs passed by parameter when
        creating the client.  This makes the CompassParserClient stateful for
        convenience, that is, one can pass in the parser/metadata config only once when
        creating the CompassParserClient, and process files without having to pass the
        config every time.

        :param filename: filename to process.
        :param file_bytes: byte content of the file
        :param file_id: ID for the file.
        :param content_type: Content type of the file.
        :param parser_config: ParserConfig object with the config to use for parsing the
            file.
        :param metadata_config: MetadataConfig object with the config to use for
            extracting metadata for each document.
        :param custom_context: Additional data to add to compass document. Fields will
            be filterable but not semantically searchable.  Can either be a dictionary
            or a callable that takes a CompassDocument and returns a dictionary.
        :param timeout: Timeout in seconds for the process_file request. If None, uses
            the timeout set when creating the client.

        :return: List of resulting documents
        """
        return await self._process_file_bytes(
            params=self._get_file_params(
                parser_config=parser_config,
                metadata_config=metadata_config,
                file_id=file_id,
                content_type=content_type,
            ),
            filename=filename,
            file_bytes=file_bytes,
            custom_context=custom_context,
            timeout=timeout,
        )

    def _get_file_params(
        self,
        *,
        parser_config: ParserConfig | None = None,
        metadata_config: MetadataConfig | None = None,
        file_id: str | None = None,
        content_type: str | None = None,
    ):
        parser_config = parser_config or self.parser_config
        metadata_config = metadata_config or self.metadata_config
        return ProcessFileParameters(
            parser_config=parser_config,
            metadata_config=metadata_config,
            doc_id=file_id,
            content_type=content_type,
        )

    async def _process_file_bytes(
        self,
        *,
        params: ProcessFileParameters,
        filename: str,
        file_bytes: bytes,
        custom_context: Fn_or_Dict | None = None,
        timeout: timedelta | None = None,
    ) -> list[CompassDocument]:
        headers = None
        if self.bearer_token:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

        res = await self.httpx.post(
            url=f"{self.parser_url}/v1/process_file",
            data={"data": json.dumps(params.model_dump())},
            files={"file": (filename, file_bytes)},
            headers=headers,
            timeout=(timeout or self.timeout).total_seconds(),
        )

        if res.is_error:
            if res.status_code >= 400 and res.status_code < 500:
                raise CompassClientError(
                    f"Error processing file: {res.status_code} {res.text}"
                )
            else:
                raise CompassError(
                    f"Error processing file: {res.status_code} {res.text}"
                )

        docs: list[CompassDocument] = []
        for doc in res.json()["docs"]:
            compass_doc = self._adapt_doc_id_compass_doc(doc)
            if compass_doc.errors:
                doc_id = compass_doc.metadata.document_id
                logger.warning(f"Document {doc_id} has errors: {compass_doc.errors}")
            additional_metadata = CompassParserAsyncClient._get_metadata(
                doc=compass_doc, custom_context=custom_context
            )
            compass_doc.content = {**compass_doc.content, **additional_metadata}
            compass_doc.errors = doc.get("errors", [])
            docs.append(compass_doc)

        return docs

    @staticmethod
    def _adapt_doc_id_compass_doc(doc: dict[Any, Any]) -> CompassDocument:
        metadata = doc["metadata"]
        if "document_id" not in metadata:
            metadata["document_id"] = metadata.pop("doc_id")
            metadata["parent_document_id"] = metadata.pop("parent_doc_id")

        chunks = doc["chunks"]
        for chunk in chunks:
            if "parent_document_id" not in chunk:
                chunk["parent_document_id"] = chunk.pop("parent_doc_id")
            if "document_id" not in chunk:
                chunk["document_id"] = chunk.pop("doc_id")
            if "path" not in chunk:
                chunk["path"] = doc["metadata"]["filename"]

        res = CompassDocument(
            filebytes=doc["filebytes"],
            metadata=metadata,
            content=doc["content"],
            content_type=doc["content_type"],
            elements=doc["elements"],
            chunks=chunks,
            index_fields=doc["index_fields"],
            errors=doc["errors"],
            ignore_metadata_errors=doc["ignore_metadata_errors"],
            markdown=doc["markdown"],
        )

        return res
