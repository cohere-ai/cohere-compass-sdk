"""
Models for synchronizer functionality in the Cohere Compass SDK.

Synchronizers bind a Compass index to an external data origin (e.g. SharePoint,
OneDrive) and let Atlas keep the index in sync. See the Compass synchronizer API
for the access model and credential-owner semantics.
"""

import datetime
from typing import Any

from pydantic import BaseModel, Field


class Selector(BaseModel):
    """A set of items to include or exclude when syncing a data origin."""

    data_ids: list[str] | None = Field(
        default=None,
        description="Explicit data item IDs to match.",
    )
    patterns: list[str] | None = Field(
        default=None,
        description="Glob-style patterns to match against item paths.",
    )


class DataSelector(BaseModel):
    """What to sync from a data origin, expressed as include/exclude selectors."""

    include_selector: Selector | None = Field(
        default=None,
        description="Items to include. When omitted, the whole origin is eligible.",
    )
    exclude_selector: Selector | None = Field(
        default=None,
        description="Items to exclude from the included set.",
    )


class UpsertSynchronizerRequest(BaseModel):
    """Request body for creating or updating a synchronizer."""

    data_origin_id: str = Field(
        description="Identifier of the data origin to sync (e.g. 'sharepoint', 'onedrive').",
    )
    data_selector: DataSelector | None = Field(
        default=None,
        description="What to sync from the data origin. Syncs the whole origin when omitted.",
    )
    credential_bundle_id: str = Field(
        default="user-default",
        description="Identifier for shared credentials. Synchronizers with the same "
        "credential_bundle_id share a single upstream auth (Atlas role). Use a different "
        "value to authenticate as a separate upstream identity.",
    )


class SynchronizerResponse(BaseModel):
    """A synchronizer and its current authentication status."""

    name: str = Field(description="The synchronizer name (unique within the index).")
    credential_bundle_user_id: str | None = Field(
        default=None,
        description="User who owns the credential bundle in use; None when no bundle is assigned.",
    )
    index_name: str = Field(description="The index this synchronizer feeds.")
    credential_bundle_id: str = Field(description="Identifier of the credential bundle in use.")
    data_origin_id: str = Field(description="Identifier of the data origin being synced.")
    data_selector: DataSelector | None = Field(
        default=None,
        description="What is being synced from the data origin.",
    )
    atlas_data_connection_id: str | None = Field(
        default=None,
        description="Atlas data connection backing this synchronizer.",
    )
    auth_status: str | None = Field(
        default=None,
        description="Authentication status of the credential owner: 'authenticated', "
        "'not_authenticated', or None when unknown.",
    )
    created_at: datetime.datetime = Field(description="When the synchronizer was created.")
    updated_at: datetime.datetime | None = Field(
        default=None,
        description="When the synchronizer was last updated.",
    )


class ListSynchronizerResponse(BaseModel):
    """Response object for list_synchronizers."""

    synchronizers: list[SynchronizerResponse]


class DataOrigin(BaseModel):
    """A supported data origin (e.g. SharePoint, OneDrive)."""

    id: str = Field(description="Identifier used as data_origin_id when creating a synchronizer.")
    name: str = Field(default="", description="Human-readable name of the data origin.")
    description: str = Field(default="", description="Description of the data origin.")
    uri: str | None = Field(default=None, description="Optional URI associated with the data origin.")


class DataOriginResponse(BaseModel):
    """Response object for list_data_origins."""

    data_origins: list[DataOrigin]


class TreeRequest(BaseModel):
    """Request body for browsing a data origin's tree (e.g. folders)."""

    root: str | None = Field(default=None, description="Selector path to browse from. None for the root.")
    depth: int = Field(default=0, description="How many levels to descend. 0 returns the immediate children.")
    page_size: int = Field(default=0, description="Maximum number of entries to return. 0 uses the server default.")
    page_token: str = Field(default="", description="Pagination token from a previous response.")


class TreeEntryResponse(BaseModel):
    """A single entry (folder or item) in a data origin's tree."""

    display_name: str = Field(description="Human-readable name of the entry.")
    selector_path: str = Field(description="Path to use in a data selector to target this entry.")
    type: str = Field(description="Entry type (e.g. folder or file).")
    data_id: str | None = Field(default=None, description="Data item ID, when the entry is a syncable item.")
    properties: dict[str, str] | None = Field(default=None, description="Additional entry properties.")


class TreeResponse(BaseModel):
    """Response object for get_synchronizer_tree."""

    tree_entries: list[TreeEntryResponse]
    page_token: str = Field(default="", description="Token to fetch the next page, empty when exhausted.")


class FailedDataItemResponse(BaseModel):
    """A data item that failed (or was skipped) during a sync."""

    data_id: str = Field(description="Identifier of the failed item.")
    error_message: str = Field(default="", description="Why the item failed.")
    display_name: str = Field(default="", description="Human-readable name of the item.")
    url: str = Field(default="", description="URL of the item, when available.")
    is_skipped: bool = Field(default=False, description="Whether the item was skipped rather than errored.")


class SyncStatusResponse(BaseModel):
    """Cumulative status of a synchronizer's syncing."""

    total_count: int = Field(default=0, description="Total number of items considered.")
    done_count: int = Field(default=0, description="Number of items successfully synced.")
    failed_items: list[FailedDataItemResponse] = Field(
        default=[],
        description="Items that failed or were skipped.",
    )
    last_sync_time: datetime.datetime | None = Field(default=None, description="When the last sync ran.")
    last_sync_state: str | None = Field(default=None, description="State of the last sync.")


class GetSyncStatusResponse(BaseModel):
    """Response object for get_sync_status."""

    sync_status: SyncStatusResponse | None = None


class GetLatestSyncJobResponse(BaseModel):
    """Response object for get_latest_sync_job."""

    latest_sync_job: dict[str, Any] | None = None


class OAuthResponse(BaseModel):
    """Response object for get_synchronizer_oauth_url."""

    auth_url: str = Field(description="URL the credential owner visits to authorize the data origin.")
