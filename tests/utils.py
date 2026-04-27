from cohere_compass.models import (
    CompassDocument,
    CompassDocumentMetadata,
    CompassSdkStage,
)
from cohere_compass.models.documents import CompassDocumentChunk


def create_test_doc(doc_id: str, num_chunks: int = 1, has_errors: bool = False) -> CompassDocument:
    doc = CompassDocument(
        metadata=CompassDocumentMetadata(
            document_id=doc_id,
            parent_document_id="parent_" + doc_id,
            filename=f"{doc_id}.txt",
        ),
        content={"test": "content"},
        index_fields=["field1", "field2"],
    )

    if not has_errors:
        doc.chunks = [
            CompassDocumentChunk(
                chunk_id=f"{doc_id}_chunk_{i}",
                sort_id=str(i),
                document_id=f"{doc_id}_chunk_{i}",
                parent_document_id="parent_" + doc_id,
                content={"text": f"chunk {i}"},
                path=f"{doc_id}.txt",
            )
            for i in range(num_chunks)
        ]
    else:
        doc.errors = [{CompassSdkStage.Parsing: "Test error"}]

    return doc
