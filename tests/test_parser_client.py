import os
from typing import List

import pytest

from compass_sdk import (
    CompassDocumentMetadata,
    MetadataConfig,
    MetadataStrategy,
    ParserConfig,
    ParsingModel,
    ParsingStrategy,
)
from compass_sdk.constants import DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES
from compass_sdk.parser import CompassDocument, CompassParserClient


@pytest.mark.parametrize("dataset_path", ["empty_dataset.json"])
def test_parse_empty_dataset(dataset_path):
    parser = CompassParserClient()
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")

    doc = parser.process_file(os.path.join(folder, "empty_dataset.json"))
    assert len(doc) == 1
    assert list(doc[0].errors[0].values())[0].startswith("Document has no bytes")


@pytest.mark.parametrize(
    """dataset_path, metadata_strategy, metadata_keywords, expected_num_chunks, expected_index_fields, 
    expected_metadata, expected_num_docs""",
    [
        ("tmdb_tiny.csv", MetadataStrategy.No_Metadata, [], 1, ["text"], "", 5),
        (
            "tmdb_tiny.csv",
            MetadataStrategy.KeywordSearch,
            ["title"],
            1,
            ["text", "meta"],
            '[{"title": "Inception"}]',
            5,
        ),
        ("tmdb_tiny.json", MetadataStrategy.No_Metadata, [], 12, ["text"], "", 5),
        (
            "tmdb_tiny.json",
            MetadataStrategy.KeywordSearch,
            ["title", "genres"],
            12,
            ["text", "meta"],
            '[{"title": "Four Rooms"}, {"genres": [{"id": 80, "name": "Crime"}, {"id": 35, "name": "Comedy"}]}]',
            5,
        ),
        (
            "tmdb_tiny.parquet",
            MetadataStrategy.KeywordSearch,
            ["title", "genres.name"],
            12,
            ["text", "meta"],
            '[{"title": "Four Rooms"}, {"genres.name": "Crime"}, {"genres.name": "Comedy"}]',
            5,
        ),
    ],
)
def test_parse_tmdb_dataset(
    dataset_path: str,
    metadata_strategy: MetadataStrategy,
    metadata_keywords: List[str],
    expected_num_chunks: int,
    expected_index_fields: List[str],
    expected_metadata: str,
    expected_num_docs: int,
):
    parser_config = ParserConfig(num_tokens_per_chunk=500)
    metadata_config = MetadataConfig(metadata_strategy=metadata_strategy, keyword_search_attributes=metadata_keywords)
    parser = CompassParserClient()
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")
    docs = parser.process_file(
        filename=os.path.join(folder, dataset_path), parser_config=parser_config, metadata_config=metadata_config
    )
    assert len(docs) == expected_num_docs
    doc = docs[0]
    assert len(doc.metadata.doc_id) > 0
    assert len(doc.chunks) == expected_num_chunks
    assert doc.index_fields == expected_index_fields
    assert ("meta" in doc.content) == (metadata_strategy != MetadataStrategy.No_Metadata)
    if metadata_strategy != MetadataStrategy.No_Metadata:
        assert doc.content["meta"] == expected_metadata


@pytest.mark.parametrize(
    (
        "filepath",
        "is_dataset",
        "expected_num_chunks",
        "expected_num_docs",
        "expected_index_fields",
        "page_numbers",
    ),
    [
        ("sample.xls", True, 1, 9, ["text"], [None]),
        ("sample.xls", False, 1, 1, ["text"], [1]),
        ("sample.doc", False, 8, 1, ["text"], [None] * 8),
        ("sample.doc", True, 0, 1, [], []),
        ("sample.docx", False, 6, 1, ["text"], [None] * 6),
        ("sample.epub", False, 38, 1, ["text"], [None] * 38),
        ("sample.odt", False, 6, 1, ["text"], [None] * 6),
        ("sample.pdf", False, 68, 1, ["text"], None),
        ("sample.ppt", False, 3, 1, ["text"], [1, 3, 4]),
        ("sample.pptx", False, 3, 1, ["text"], [1, 4, 4]),
    ],
)
def test_parse_documents(
    filepath: str,
    is_dataset: bool,
    expected_num_chunks: int,
    expected_num_docs: int,
    expected_index_fields: List[str],
    page_numbers: List[int],
):
    parser_config = ParserConfig(num_tokens_per_chunk=500)
    parser = CompassParserClient()
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")
    docs = parser.process_file(
        filename=os.path.join(folder, filepath), is_dataset=is_dataset, parser_config=parser_config
    )
    assert len(docs) == expected_num_docs
    doc = docs[0]

    actual_num_chunks = len(doc.chunks) if doc.chunks else 0
    assert actual_num_chunks == expected_num_chunks
    assert doc.index_fields == expected_index_fields
    if page_numbers is not None:
        assert [chunk.origin and chunk.origin.page_number for chunk in doc.chunks] == page_numbers


@pytest.mark.parametrize(
    "filepath, is_dataset, expected_num_chunks, expected_num_docs, expected_index_fields",
    [
        ("sample.pdf", False, 75, 1, ["text"]),
    ],
)
def test_parse_hi_res(
    filepath: str, is_dataset: bool, expected_num_chunks: int, expected_num_docs: int, expected_index_fields: List[str]
):
    parser_config = ParserConfig(parsing_strategy=ParsingStrategy.Hi_Res, parsing_model=ParsingModel.YoloX_Quantized)
    parser = CompassParserClient()
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")
    docs = parser.process_file(
        filename=os.path.join(folder, filepath), is_dataset=is_dataset, parser_config=parser_config
    )
    assert len(docs) == expected_num_docs
    doc = docs[0]

    actual_num_chunks = len(doc.chunks) if doc.chunks else 0
    assert actual_num_chunks == expected_num_chunks
    assert doc.index_fields == expected_index_fields


@pytest.mark.parametrize(
    "filepaths, are_datasets, expected_num_chunks, expected_num_docs, expected_index_fields",
    [
        (
            [
                "sample.xls",
                "sample.xls",
                "sample.doc",
                "sample.doc",
                "sample.docx",
                "sample.epub",
                "sample.odt",
                "sample.pdf",
                "sample.ppt",
                "sample.pptx",
                "json_example.json",
            ],
            [True, False, False, True, False, False, False, False, False, False, False],
            [1, 1, 8, 0, 6, 37, 6, 66, 2, 2, 1],
            [9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            [["text"], ["text"], ["text"], [], ["text"], ["text"], ["text"], ["text"], ["text"], ["text"], ["text"]],
        )
    ],
)
def test_parse_documents_in_block(
    filepaths: List[str],
    are_datasets: List[bool],
    expected_num_chunks: List[int],
    expected_num_docs: List[int],
    expected_index_fields: List[List[str]],
):
    parser_config = ParserConfig()
    parser = CompassParserClient()
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")

    for i, filepath in enumerate(filepaths):
        docs = parser.process_file(
            filename=os.path.join(folder, filepath), is_dataset=are_datasets[i], parser_config=parser_config
        )
        assert len(docs) == expected_num_docs[i]
        doc = docs[0]

        assert len(doc.chunks) if doc.chunks else 0 == expected_num_chunks[i]
        assert doc.index_fields == expected_index_fields[i]


def test_passed_cohere_key_overrides_env():
    env_key = "12356"
    passed_key = "abcdef"
    os.environ["COHERE_API_ENV"] = env_key
    metadata_config = MetadataConfig(cohere_api_key=passed_key)
    assert metadata_config.cohere_api_key == passed_key


def test_large_files_fail_fast(mocker):
    parser = CompassParserClient()
    large_file_doc = CompassDocument(
        filebytes=bytearray(DEFAULT_MAX_ACCEPTED_FILE_SIZE_BYTES + 1000),
        metadata=CompassDocumentMetadata(filename="mocked_file.md"),
    )
    mocker.patch("compass_sdk.parser.open_document", return_value=large_file_doc)
    doc = parser.process_file("mocked_file.md")
    assert len(doc) == 0


def test_custom_context_with_fn_has_value():
    parser = CompassParserClient()
    parser_config = ParserConfig(num_tokens_per_chunk=500)
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")

    def _custom_context(doc):
        return {"test_key_found": "Darn right!"}

    docs = parser.process_folder(folder, parser_config=parser_config, custom_context=_custom_context)

    for doc in docs:
        assert doc.content["test_key_found"] == "Darn right!"


def test_custom_context_with_dict_has_value():
    parser = CompassParserClient()
    parser_config = ParserConfig(num_tokens_per_chunk=500)
    folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")

    test_dict = {"static_key": "Darn right!"}

    docs = parser.process_folder(folder, parser_config=parser_config, custom_context=test_dict)

    for doc in docs:
        assert doc.content["static_key"] == "Darn right!"
