"""
Vector and embedding related schemas.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime


@dataclass
class VectorMetaData:
    """Metadata for vectors stored in Qdrant."""
    file_id: str
    filename: str
    chunk_id: str
    chunk_index: int
    page_number: int
    text: str
    token_count: int
    has_image: bool = False
    heading_text: Optional[str] = None
    context_text: Optional[str] = None
    doc_items_refs: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class DocumentMetadata:
    """Metadata for a document."""
    file_id: str
    filename: str
    total_pages: int = 0
    total_chunks: int = 0
    processed_at: Optional[datetime] = None
