"""
Docling Document Processing Module.

Provides document processing with Docling OCR and Qdrant vector storage.
"""

from app.docling.processor import AsyncDocumentProcessor
from app.docling.qdrant_service import DoclingQdrantService
from app.docling.qdrant_adapter import DoclingQdrantAdapter, DoclingChunkMetadata

__all__ = [
    "AsyncDocumentProcessor",
    "DoclingQdrantService",
    "DoclingQdrantAdapter",
    "DoclingChunkMetadata",
]
