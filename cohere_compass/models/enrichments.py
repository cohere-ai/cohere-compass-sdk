from typing import Any

from pydantic import BaseModel, Field


class WebhookEnrichmentRequest(BaseModel):
    """
    Request parameters sent to webhook enrichers.

    The file downloaded from parsed_doc_url will be a JSON CompassDocument.
    """

    parsed_doc_url: str = Field(
        description="URL where the parsed document can be accessed. "
    )


class WebhookEnrichmentItem(BaseModel):

    field: str = Field(
        description="The enrichment field name (e.g., 'sentiment_v1_score')."
    )
    value: Any = Field(
        description="The enrichment value. Can be any JSON-serializable value."
    )
    chunk_ids: list[str] | None = Field(
        default=None,
        description="Optional list of specific chunk IDs to apply this enrichment to. "
        "If omitted, the enrichment will be applied to all chunks in the document.",
    )


# Your webhook must return a WebhookEnrichmentResponse in JSON
WebhookEnrichmentResponse = list[WebhookEnrichmentItem]

