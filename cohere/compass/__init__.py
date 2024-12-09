# Python imports
from enum import Enum
from typing import List, Optional

# 3rd party imports
from pydantic import BaseModel

# Local imports
from cohere.compass.models import (
    MetadataConfig,
    ParserConfig,
    ValidatedModel,
)

__version__ = "0.8.0"


class ProcessFileParameters(ValidatedModel):
    parser_config: ParserConfig
    metadata_config: MetadataConfig
    doc_id: Optional[str] = None
    content_type: Optional[str] = None


class ProcessFilesParameters(ValidatedModel):
    doc_ids: Optional[List[str]] = None
    parser_config: ParserConfig
    metadata_config: MetadataConfig


class GroupAuthorizationActions(str, Enum):
    ADD = "add"
    REMOVE = "remove"


class GroupAuthorizationInput(BaseModel):
    document_ids: List[str]
    authorized_groups: List[str]
    action: GroupAuthorizationActions
