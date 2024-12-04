# Python imports
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
import json
import logging
import os

# 3rd party imports
import requests

# Local imports
from cohere.compass import (
    ProcessFileParameters,
)
from cohere.compass.constants import DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES
from cohere.compass.models import (
    CompassDocument,
    MetadataConfig,
    ParserConfig,
)
from cohere.compass.utils import imap_queued, open_document, scan_folder

Fn_or_Dict = Union[Dict[str, Any], Callable[[CompassDocument], Dict[str, Any]]]


logger = logging.getLogger(__name__)


class CompassParserClient:
    """
    Client to interact with the CompassParser API. It allows to process files using the
    parser and metadata configurations specified in the parameters. The client is
    stateful, that is, it can be initialized with parser and metadata configurations
    that will be used for all subsequent files processed by the client.  Also,
    independently of the default configurations, the client allows to pass specific
    configurations for each file when calling the process_file or process_files methods.
    The client is responsible for opening the files and sending them to the
    CompassParser API for processing. The resulting documents are returned as
    CompassDocument objects.

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
        username: Optional[str] = None,
        password: Optional[str] = None,
        num_workers: int = 4,
    ):
        """
        Initializes the CompassParserClient with the specified parser_url,
        parser_config, and metadata_config.  The parser_config and metadata_config are
        optional, and if not provided, the default configurations will be used.  If the
        parser/metadata configs are provided, they will be used for all subsequent files
        processed by the client unless specific configs are passed when calling the
        process_file or process_files methods.

        :param parser_url: the URL of the CompassParser API
        :param parser_config: the parser configuration to use when processing files if
            no parser configuration is specified in the method calls (process_file or
            process_files)
        :param metadata_config: the metadata configuration to use when processing files
            if no metadata configuration is specified in the method calls (process_file
            or process_files)
        """
        self.parser_url = (
            parser_url if not parser_url.endswith("/") else parser_url[:-1]
        )
        self.parser_config = parser_config
        self.username = username or os.getenv("COHERE_COMPASS_USERNAME")
        self.password = password or os.getenv("COHERE_COMPASS_PASSWORD")
        self.session = requests.Session()
        self.thread_pool = ThreadPoolExecutor(num_workers)
        self.num_workers = num_workers

        self.metadata_config = metadata_config
        logger.info(
            f"CompassParserClient initialized with parser_url: {self.parser_url}"
        )

    def process_folder(
        self,
        *,
        folder_path: str,
        allowed_extensions: Optional[List[str]] = None,
        recursive: bool = False,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
    ):
        """
        Processes all the files in the specified folder using the default parser and
        metadata configurations passed when creating the client. The method iterates
        over all the files in the folder and processes them using the process_file
        method. The resulting documents are returned as a list of CompassDocument
        objects.

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

    def process_files(
        self,
        *,
        filenames: List[str],
        file_ids: Optional[List[str]] = None,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
    ) -> Iterable[CompassDocument]:
        """
        Processes a list of files provided as filenames, using the specified parser and
        metadata configurations.

        If the parser/metadata configs are not provided, then the default configs passed
        by parameter when creating the client will be used. This makes the
        CompassParserClient stateful. That is, we can set the parser/metadata configs
        only once when creating the parser client, and process all subsequent files
        without having to pass the config every time.

        All the documents passed as filenames and opened to obtain their bytes. Then,
        they are packed into a ProcessFilesParameters object that contains a list of
        ProcessFileParameters, each contain a file, its id, and the parser/metadata
        config

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

        def process_file(i: int) -> List[CompassDocument]:
            return self.process_file(
                filename=filenames[i],
                file_id=file_ids[i] if file_ids else None,
                parser_config=parser_config,
                metadata_config=metadata_config,
                custom_context=custom_context,
            )

        for results in imap_queued(
            self.thread_pool,
            process_file,
            range(len(filenames)),
            max_queued=self.num_workers,
        ):
            yield from results

    @staticmethod
    def _get_metadata(
        doc: CompassDocument, custom_context: Optional[Fn_or_Dict] = None
    ) -> Dict[str, Any]:
        if custom_context is None:
            return {}
        elif callable(custom_context):
            return custom_context(doc)
        else:
            return custom_context

    def process_file(
        self,
        *,
        filename: str,
        file_id: Optional[str] = None,
        content_type: Optional[str] = None,
        parser_config: Optional[ParserConfig] = None,
        metadata_config: Optional[MetadataConfig] = None,
        custom_context: Optional[Fn_or_Dict] = None,
    ) -> List[CompassDocument]:
        """
        Takes in a file, its id, and the parser/metadata config. If the config is None,
        then it uses the default configs passed by parameter when creating the client.
        This makes the CompassParserClient stateful for convenience, that is, one can
        pass in the parser/metadata config only once when creating the
        CompassParserClient, and process files without having to pass the config every
        time

        :param filename: Filename to process
        :param file_id: Id for the file
        :param content_type: Content type of the file
        :param parser_config: ParserConfig object with the config to use for parsing the file
        :param metadata_config: MetadataConfig object with the config to use for
            extracting metadata for each document
        :param custom_context: Additional data to add to compass document. Fields will
            be filterable but not semantically searchable.  Can either be a dictionary
            or a callable that takes a CompassDocument and returns a dictionary.

        :return: List of resulting documents
        """
        doc = open_document(filename)
        if doc.errors:
            logger.error(f"Error opening document: {doc.errors}")
            return []
        if len(doc.filebytes) > DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES:
            logger.error(
                f"File too large, supported file size is {DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES / 1000_000} "
                f"mb, filename {doc.metadata.filename}"
            )
            return []

        parser_config = parser_config or self.parser_config
        metadata_config = metadata_config or self.metadata_config

        params = ProcessFileParameters(
            parser_config=parser_config,
            metadata_config=metadata_config,
            doc_id=file_id,
            content_type=content_type,
        )
        auth = (
            (self.username, self.password) if self.username and self.password else None
        )
        res = self.session.post(
            url=f"{self.parser_url}/v1/process_file",
            data={"data": json.dumps(params.model_dump())},
            files={"file": (filename, doc.filebytes)},
            auth=auth,
        )

        if res.ok:
            docs: list[CompassDocument] = []
            for doc in res.json()["docs"]:
                if not doc.get("errors", []):
                    compass_doc = CompassDocument(**doc)
                    additional_metadata = CompassParserClient._get_metadata(
                        doc=compass_doc, custom_context=custom_context
                    )
                    compass_doc.content = {**compass_doc.content, **additional_metadata}
                    docs.append(compass_doc)
        else:
            docs = []
            logger.error(f"Error processing file: {res.text}")

        return docs
