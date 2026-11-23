import json
import re
from hashlib import sha256
from uuid import uuid5

from cohere_compass.ext.uploads.identity import CONTENT_ID_NAMESPACE, content_identity
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


def test_content_identity_without_path_or_attributes_is_unchanged() -> None:
    # Documents uploaded by earlier versions must keep their IDs, or every
    # upload would re-upload them and `prune` would delete the old ones.
    options = json.dumps(ParserConfig().model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    digest = sha256(b"same").hexdigest()
    legacy_id = uuid5(CONTENT_ID_NAMESPACE, f"{digest}:{options}").hex

    assert content_identity(b"same", ParserConfig()) == (digest, legacy_id)
    assert content_identity(b"same", ParserConfig(), attributes={}) == (digest, legacy_id)


def test_content_identity_includes_path_and_attributes_when_given() -> None:
    plain = content_identity(b"same", ParserConfig())[1]
    in_a = content_identity(b"same", ParserConfig(), path="a/README.md")[1]
    in_b = content_identity(b"same", ParserConfig(), path="b/README.md")[1]
    with_url = content_identity(b"same", ParserConfig(), attributes={"url": "https://example.com/a"})[1]

    assert len({plain, in_a, in_b, with_url}) == 4
    assert content_identity(b"same", ParserConfig(), path="a/README.md")[1] == in_a
    assert content_identity(b"same", ParserConfig(), attributes={"a": "1", "b": "2"}) == content_identity(
        b"same", ParserConfig(), attributes={"b": "2", "a": "1"}
    )
    # The content digest does not depend on the path or attributes.
    assert content_identity(b"same", ParserConfig(), path="a/README.md")[0] == sha256(b"same").hexdigest()
