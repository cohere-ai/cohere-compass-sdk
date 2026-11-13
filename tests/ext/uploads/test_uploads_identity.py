import re

from cohere_compass.ext.uploads.identity import content_identity
from cohere_compass.models.config import ParserConfig


def test_content_identity_is_deterministic_and_url_safe() -> None:
    first = content_identity(b"hello", ParserConfig())
    assert first == content_identity(b"hello", ParserConfig())
    assert re.fullmatch(r"[A-Za-z0-9_-]+", first[1])


def test_content_identity_uses_canonical_full_parser_config() -> None:
    quick = ParserConfig.model_validate({"pdf_parsing_strategy": "QuickText"})
    reordered_quick = ParserConfig.model_validate({"num_tokens_per_chunk": 250, "pdf_parsing_strategy": "QuickText"})
    reordered_quick_again = ParserConfig.model_validate(
        {"pdf_parsing_strategy": "QuickText", "num_tokens_per_chunk": 250}
    )

    assert content_identity(b"same", quick) != content_identity(b"same", reordered_quick)
    assert content_identity(b"same", reordered_quick) == content_identity(b"same", reordered_quick_again)
