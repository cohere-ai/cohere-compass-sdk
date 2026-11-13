"""Deterministic identities for uploaded content."""

import json
from hashlib import sha256
from uuid import UUID, uuid5

from cohere_compass.models.config import ParserConfig

CONTENT_ID_NAMESPACE = UUID("cb8a9e51-cbe1-583e-a941-c0dc2e68acfb")


def content_identity(file_bytes: bytes, parser_config: ParserConfig) -> tuple[str, str]:
    """
    Return the SHA-256 digest and deterministic document ID for content.

    The ID couples content with parser options: unchanged content and options
    reproduce the same ID (so uploads can skip an already-indexed document),
    while changed options mint a new ID and leave the previous version intact.
    `parser_config` is required because it is part of the identity.
    """
    content_sha256 = sha256(file_bytes).hexdigest()
    canonical_options = json.dumps(parser_config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return content_sha256, uuid5(CONTENT_ID_NAMESPACE, f"{content_sha256}:{canonical_options}").hex
