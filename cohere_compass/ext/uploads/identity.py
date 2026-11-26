"""Deterministic identities for uploaded content."""

import json
from collections.abc import Mapping
from hashlib import sha256
from uuid import UUID, uuid5

from cohere_compass.models.config import ParserConfig

CONTENT_ID_NAMESPACE = UUID("cb8a9e51-cbe1-583e-a941-c0dc2e68acfb")


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def content_identity(
    file_bytes: bytes,
    parser_config: ParserConfig,
    *,
    path: str | None = None,
    attributes: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """
    Return the SHA-256 digest and deterministic document ID for content.

    The ID couples content with parser options: unchanged content and options
    reproduce the same ID (so uploads can skip an already-indexed document),
    while changed options mint a new ID and leave the previous version intact.
    `parser_config` is required because it is part of the identity.

    `path` and `attributes` join the identity only when given, so the same
    content under two paths, or with two sets of attributes, becomes two
    documents. Without them, the ID is the content-and-options ID.
    """
    content_sha256 = sha256(file_bytes).hexdigest()
    name = f"{content_sha256}:{_canonical_json(parser_config.model_dump(mode='json'))}"
    descriptor: dict[str, object] = {}
    if path is not None:
        descriptor["path"] = path
    if attributes:
        descriptor["attributes"] = dict(attributes)
    if descriptor:
        name = f"{name}:{_canonical_json(descriptor)}"
    return content_sha256, uuid5(CONTENT_ID_NAMESPACE, name).hex
