# Python imports
from enum import Enum
from importlib import metadata
from typing import Optional

# 3rd party imports
from pydantic import BaseModel

# Local imports
from cohere_compass.models import (
    MetadataConfig,
    ParserConfig,
    ValidatedModel,
)

__version__ = metadata.version("cohere-compass-sdk")


class ProcessFileParameters(ValidatedModel):
    """Model for use with the process_file parser API."""

    parser_config: ParserConfig
    metadata_config: MetadataConfig
    doc_id: Optional[str] = None
    content_type: Optional[str] = None


class ProcessFilesParameters(ValidatedModel):
    """Model for use with the process_files parser API."""

    doc_ids: Optional[list[str]] = None
    parser_config: ParserConfig
    metadata_config: MetadataConfig


class GroupAuthorizationActions(str, Enum):
    """Enum for use with the update_group_authorization API to specify the edit type."""

    ADD = "add"
    REMOVE = "remove"


class GroupAuthorizationInput(BaseModel):
    """Model for use with the update_group_authorization API."""

    document_ids: list[str]
    authorized_groups: list[str]
    action: GroupAuthorizationActions
