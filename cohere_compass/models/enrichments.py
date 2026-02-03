"""Models for webhook enrichment request/response contracts."""

from typing import Any

from pydantic import BaseModel, Field


class WebhookEnrichmentRequest(BaseModel):
    """
    Request payload sent to webhook enrichers.

    The parsed document (JSON CompassDocument) can be fetched from parsed_doc_url.
    """

    parsed_doc_url: str = Field(description="URL to fetch the parsed CompassDocument JSON.")
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary parameters passed through from WebhookEnricherConfig.",
    )


class WebhookEnrichmentItem(BaseModel):
    """A single enrichment to apply to document chunks."""

    field: str = Field(description="Enrichment field name (e.g., 'sentiment_score').")
    value: Any = Field(description="JSON-serializable enrichment value.")
    chunk_ids: list[str] | None = Field(
        default=None,
        description="Chunk IDs to enrich. If None, applies to all chunks.",
    )


class WebhookEnrichmentResponse(BaseModel):
    """Response expected from webhook enrichers."""

    enrichments: list[WebhookEnrichmentItem] = []
