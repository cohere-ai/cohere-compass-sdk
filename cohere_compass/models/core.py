from pydantic import BaseModel


class IndexDetails(BaseModel):
    """Model representing the details of an index."""

    name: str
    count: str
    parent_doc_count: int
    dense_model: int
    sparse_model: str
    analyzer: str
