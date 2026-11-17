"""Models for indexes functionality in the Cohere Compass SDK."""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class RetentionType(str, Enum):
    """Enum for specifying the retention policy type."""

    Fixed = "fixed"
    Sliding = "sliding"


class RetentionPolicy(BaseModel):
    """
    Retention policy configuration for an index.

    Defines how documents are automatically expired and deleted based on their age
    or access patterns.

    :param retention_type: The type of retention policy - 'fixed' (based on creation
        time) or 'sliding' (based on last access time).
    :param ttl_days: Time-to-live in days. Documents older than this will be
        soft-deleted.
    :param grace_period_days: Number of days between soft-delete and hard-delete.
        During the grace period, documents are hidden from search but not yet
        permanently removed.
    :param enabled: Whether the retention policy is currently active.
    """

    retention_type: RetentionType = Field(
        description="The type of retention policy: 'fixed' (expires based on "
        "created_at + ttl_days) or 'sliding' (expires based on accessed_at + ttl_days)."
    )
    ttl_days: int = Field(
        gt=0,
        description="Time-to-live in days. Documents will be soft-deleted after this period.",
    )
    grace_period_days: int = Field(
        default=7,
        ge=0,
        description="Days between soft-delete and hard-delete. Default is 7 days.",
    )
    enabled: bool = Field(
        default=True,
        description="Whether the retention policy is active.",
    )


class GlossaryEntry(BaseModel):
    """
    One term group in an index glossary.

    With ``expansions``, a query that contains any of ``terms`` is also searched as
    each expansion (``SPB`` => ``Star Pubs & Bars``). Without ``expansions`` the group
    is an equivalence set, which needs at least two terms: a query containing any
    term is also searched as every other term.
    """

    terms: list[str] = Field(min_length=1, description="Terms to look for in the query, matched as whole words.")
    expansions: list[str] = Field(
        default_factory=list,
        description="What each term also searches for. Empty makes the terms an equivalence set.",
    )


class Glossary(BaseModel):
    """
    Search glossary of an index.

    The server limits a glossary to 5,000 entries, 20 terms and 20 expansions per
    entry, and 256 characters per term.
    """

    entries: list[GlossaryEntry] = Field(min_length=1)
    expand_semantic_query: bool = Field(
        default=True,
        description="When a term matches, also add a short list of its expansions to the query that is embedded "
        "and reranked. Keyword search always uses the expansions.",
    )


class PrefixSearchSettings(BaseModel):
    """
    Prefix search settings of an index.

    When a query of at least ``min_prefix_length`` characters is the start of a word
    in one of ``fields`` in a document the caller can read, search is limited to
    those documents. Otherwise search runs as usual.
    """

    fields: list[str] = Field(
        default_factory=lambda: ["text"],
        min_length=1,
        description='Fields to check: "text" or "content.<name>". At most 5.',
    )
    min_prefix_length: int = Field(default=3, ge=1, le=20, description="Shortest query that is checked as a prefix.")


class GlossaryResponse(BaseModel):
    """Response object for get_glossary API."""

    glossary: Glossary | None = None


class PrefixSearchResponse(BaseModel):
    """Response object for get_prefix_search API."""

    prefix_search: PrefixSearchSettings | None = None


class IndexInfo(BaseModel):
    """Information about an index."""

    name: str = Field(description="The name of the index - this has to be unique")
    count: int = Field(description="The total number of chunks in the index")
    parent_doc_count: int | None = Field(
        description="The total number of files provided for the index - files can be broken into 1..* chunks"
    )
    description: str | None = Field(
        default=None,
        description="What the index contains, as set by its owner. None when no description is set.",
    )


class IndexDetails(IndexInfo):
    """Model representing the details of an index."""

    dense_model: str = Field(
        description="The name of the dense model used for embedding documents in the index.",
    )
    sparse_model: str = Field(
        description="The name of the sparse model used for embedding documents in the index.",
    )
    analyzer: str = Field(
        description="The name of the analyzer used for the index.",
    )
    store_size_bytes: int | None = Field(
        default=None,
        description="Total index size on disk in bytes, including replica shards.",
    )
    primary_store_size_bytes: int | None = Field(
        default=None,
        description="Primary shard size on disk in bytes, excluding replicas.",
    )
    primary_shard_count: int | None = Field(
        default=None,
        description="Number of primary shards.",
    )
    replica_count: int | None = Field(
        default=None,
        description="Number of replicas configured (not total replica shards).",
    )
    health: Literal["green", "yellow", "red"] | None = Field(
        default=None,
        description='Index health status: "green", "yellow", or "red".',
    )
    retention_policy: RetentionPolicy | None = Field(
        default=None,
        description="The retention policy configured for this index, if any.",
    )


class IndexUpdate(BaseModel):
    """
    Request body for update_index API.

    Only the fields that are explicitly set are sent, so an unset field is left
    unchanged on the server while an explicit None clears it.
    """

    description: str | None = Field(
        default=None,
        max_length=1000,
        description="What the index contains. None or an empty string removes the description.",
    )


class IndexUpdateResponse(BaseModel):
    """Response object for update_index API."""

    description: str | None = Field(default=None, description="The index's description after the update.")


class ListIndexesResponse(BaseModel):
    """Response object for list_indexes API."""

    indexes: list[IndexInfo]
