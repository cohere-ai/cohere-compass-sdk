# Python imports
import json
import logging
import threading
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, Union

# 3rd party imports
import requests
from requests.exceptions import InvalidSchema
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
    DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES,
    DEFAULT_MAX_RETRIES,
    DEFAULT_SLEEP_RETRY_SECONDS,
)
from cohere_compass.exceptions import CompassClientError, CompassError
from cohere_compass.models import (
    CompassDocument,
    MetadataConfig,
    ParserConfig,
)
from cohere_compass.utils import imap_queued, open_document, scan_folder

Fn_or_Dict = Union[dict[str, Any], Callable[[CompassDocument], dict[str, Any]]]


logger = logging.getLogger(__name__)


class CompassParserClient:
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
        bearer_token: Optional[str] = None,
        num_workers: int = 1,
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
        """
        self._thread_local = threading.local()
        self.parser_url = (
            parser_url if not parser_url.endswith("/") else parser_url[:-1]
        )
        self.parser_config = parser_config
        self.bearer_token = bearer_token
        self.thread_pool = ThreadPoolExecutor(num_workers)
        self.num_workers = num_workers

        self.metadata_config = metadata_config
        logger.info(
            f"CompassParserClient initialized with parser_url: {self.parser_url}"
        )

    def _get_session(self) -> requests.Session:
        if not hasattr(self._thread_local, "session"):
            self._thread_local.session = requests.Session()
        return self._thread_local.session

    def process_folder(
        self,
        *,
        folder_path: str,
        allowed_extensions: Optional[list[str]] = None,
        recursive: bool = False,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
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

        :returns: the list of processed documents
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

    def process_files(
        self,
        *,
        filenames: list[str],
        file_ids: Optional[list[str]] = None,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
    ) -> Iterable[Union[CompassDocument, tuple[str, Exception]]]:
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

        :returns: List of processed documents
        """

        def process_file(i: int) -> Union[list[CompassDocument], tuple[str, Exception]]:
            filename = filenames[i]
            try:
                return self.process_file(
                    filename=filename,
                    file_id=file_ids[i] if file_ids else None,
                    parser_config=parser_config,
                    metadata_config=metadata_config,
                    custom_context=custom_context,
                )
            except Exception as e:
                return filename, e

        for results in imap_queued(
            self.thread_pool,
            process_file,
            range(len(filenames)),
            max_queued=self.num_workers,
        ):
            if isinstance(results, list):
                yield from results
            else:
                yield results

    @staticmethod
    def _get_metadata(
        doc: CompassDocument, custom_context: Optional[Fn_or_Dict] = None
    ) -> dict[str, Any]:
        if custom_context is None:
            return {}
        elif callable(custom_context):
            return custom_context(doc)
        else:
            return custom_context

    @retry(
        stop=stop_after_attempt(DEFAULT_MAX_RETRIES),
        wait=wait_fixed(DEFAULT_SLEEP_RETRY_SECONDS),
        retry=retry_if_not_exception_type((InvalidSchema, CompassClientError)),
    )
    def process_file(
        self,
        *,
        filename: str,
        file_id: Optional[str] = None,
        content_type: Optional[str] = None,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
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

        :returns: List of resulting documents
        """
        doc = open_document(filename)
        if doc.errors:
            logger.error(f"Error opening document: {doc.errors}")
            return []

        return self._process_file_bytes(
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
        wait=wait_fixed(DEFAULT_SLEEP_RETRY_SECONDS),
        retry=retry_if_not_exception_type((InvalidSchema, CompassClientError)),
    )
    def process_file_bytes(
        self,
        *,
        filename: str,
        file_bytes: bytes,
        file_id: Optional[str] = None,
        content_type: Optional[str] = None,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
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

        :returns: List of resulting documents
        """
        return self._process_file_bytes(
            params=self._get_file_params(
                parser_config=parser_config,
                metadata_config=metadata_config,
                file_id=file_id,
                content_type=content_type,
            ),
            filename=filename,
            file_bytes=file_bytes,
            custom_context=custom_context,
        )

    def _get_file_params(
        self,
        *,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        file_id: Optional[str] = None,
        content_type: Optional[str] = None,
    ):
        parser_config = parser_config or self.parser_config
        metadata_config = metadata_config or self.metadata_config
        return ProcessFileParameters(
            parser_config=parser_config,
            metadata_config=metadata_config,
            doc_id=file_id,
            content_type=content_type,
        )

    def _process_file_bytes(
        self,
        *,
        params: ProcessFileParameters,
        filename: str,
        file_bytes: bytes,
        custom_context: Optional[Fn_or_Dict] = None,
    ) -> list[CompassDocument]:
        if len(file_bytes) > DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES:
            max_size_mb = DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES / 1000_000
            logger.error(
                f"File too large, supported file size is {max_size_mb} mb"
                + f"filename {filename}"
            )
            return []
        headers = None
        if self.bearer_token:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

        res = self._get_session().post(
            url=f"{self.parser_url}/v1/process_file",
            data={"data": json.dumps(params.model_dump())},
            files={"file": (filename, file_bytes)},
            headers=headers,
        )

        if not res.ok:
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
            if not doc.get("errors", []):
                compass_doc = self._adapt_doc_id_compass_doc(doc)
                additional_metadata = CompassParserClient._get_metadata(
                    doc=compass_doc, custom_context=custom_context
                )
                compass_doc.content = {**compass_doc.content, **additional_metadata}
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
