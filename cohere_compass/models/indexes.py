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


class IndexInfo(BaseModel):
    """Information about an index."""

    name: str = Field(description="The name of the index - this has to be unique")
    count: int = Field(description="The total number of chunks in the index")
    parent_doc_count: int | None = Field(
        description="The total number of files provided for the index - files can be broken into 1..* chunks"
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


class ListIndexesResponse(BaseModel):
    """Response object for list_indexes API."""

    indexes: list[IndexInfo]
