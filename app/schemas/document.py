"""
Document schema models for MongoDB storage.
"""

from enum import Enum
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
import uuid


class DocumentStatus(str, Enum):
    """Processing status for documents."""
    PENDING = "pending"           # Queued for chunking/vectorization
    PROCESSING = "processing"     # Currently being processed
    VECTORIZED = "vectorized"     # Successfully chunked and stored in Qdrant
    FAILED = "failed"             # Processing failed
    STORED = "stored"             # Stored only, no chunking needed (direct PDF downloads)


class CrawledDocument(BaseModel):
    """
    Schema for crawled PDF documents stored in MongoDB.
    
    Tracks document metadata, processing status, and audit trail.
    """
    file_id: str = Field(default_factory=lambda: f"{uuid.uuid4()}.pdf")
    original_file: str = Field(..., description="Original filename")
    source_url: str = Field(..., description="URL where PDF was crawled from")
    file_path: str = Field(..., description="Storage path on filesystem")
    
    # Processing state
    status: DocumentStatus = Field(default=DocumentStatus.PENDING)
    vector_count: int = Field(default=0, description="Number of chunks stored in Qdrant")
    error_message: Optional[str] = Field(default=None, description="Error details if failed")
    
    # File metadata
    file_size: int = Field(default=0, description="File size in bytes")
    page_count: int = Field(default=0, description="Number of pages")
    mime_type: str = Field(default="application/pdf")
    
    # Crawl context
    crawl_session_id: str = Field(..., description="Groups PDFs by crawl run")
    crawl_depth: int = Field(default=0, description="Depth in crawl tree")
    
    # Soft delete
    is_deleted: bool = Field(default=False)
    
    # Custom flags
    is_vectorized: str = Field(default="0", description="Legacy flag: 0=pending, 1=vectorized")
    
    # Audit trail
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = Field(default="crawler")
    updated_by: str = Field(default="crawler")
    
    class Config:
        use_enum_values = True
    
    def to_mongo_dict(self) -> dict:
        """Convert to MongoDB-compatible dictionary."""
        data = self.model_dump()
        # Rename file_id to fileId for MongoDB convention
        data["fileId"] = data.pop("file_id")
        data["originalFile"] = data.pop("original_file")
        data["sourceUrl"] = data.pop("source_url")
        data["filePath"] = data.pop("file_path")
        data["vectorCount"] = data.pop("vector_count")
        data["errorMessage"] = data.pop("error_message")
        data["fileSize"] = data.pop("file_size")
        data["pageCount"] = data.pop("page_count")
        data["mimeType"] = data.pop("mime_type")
        data["crawlSessionId"] = data.pop("crawl_session_id")
        data["crawlSessionId"] = data.pop("crawl_session_id")
        data["crawlDepth"] = data.pop("crawl_depth")
        data["isDeleted"] = data.pop("is_deleted")
        data["isVectorized"] = data.pop("is_vectorized")
        data["createdAt"] = data.pop("created_at")
        data["updatedAt"] = data.pop("updated_at")
        data["createdBy"] = data.pop("created_by")
        data["updatedBy"] = data.pop("updated_by")
        return data
    
    @classmethod
    def from_mongo_dict(cls, data: dict) -> "CrawledDocument":
        """Create from MongoDB document."""
        return cls(
            file_id=data.get("fileId", ""),
            original_file=data.get("originalFile", ""),
            source_url=data.get("sourceUrl", ""),
            file_path=data.get("filePath", ""),
            status=data.get("status", DocumentStatus.PENDING),
            vector_count=data.get("vectorCount", 0),
            error_message=data.get("errorMessage"),
            file_size=data.get("fileSize", 0),
            page_count=data.get("pageCount", 0),
            mime_type=data.get("mimeType", "application/pdf"),
            crawl_session_id=data.get("crawlSessionId", ""),
            crawl_depth=data.get("crawlDepth", 0),
            is_deleted=data.get("isDeleted", False),
            is_vectorized=data.get("isVectorized", "0"),
            created_at=data.get("createdAt", datetime.utcnow()),
            updated_at=data.get("updatedAt", datetime.utcnow()),
            created_by=data.get("createdBy", "crawler"),
            updated_by=data.get("updatedBy", "crawler"),
        )
