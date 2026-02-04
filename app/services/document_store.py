"""
MongoDB Document Store Service.

Provides CRUD operations for crawled PDF documents.
"""

from datetime import datetime
from typing import List, Optional
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from app.config import get_logger
from app.schemas.document import CrawledDocument, DocumentStatus

logger = get_logger(__name__)


class DocumentStore:
    """
    MongoDB document store for crawled PDFs.
    
    Manages document metadata, processing status, and queries.
    Uses connection pooling via singleton pattern.
    
    Usage:
        store = DocumentStore.from_config()
        doc = store.create_document(...)
        pending = store.get_pending_documents()
    """
    
    _instance: Optional["DocumentStore"] = None
    _client: Optional[MongoClient] = None
    
    def __init__(self, mongodb_url: str, database_name: str):
        """
        Initialize document store.
        
        Args:
            mongodb_url: MongoDB connection URL
            database_name: Database name to use
        """
        self._mongodb_url = mongodb_url
        self._database_name = database_name
        self._db: Optional[Database] = None
        self._collection: Optional[Collection] = None
    
    def _get_client(self) -> MongoClient:
        """Get or create MongoDB client with connection pooling."""
        if DocumentStore._client is None:
            DocumentStore._client = MongoClient(
                self._mongodb_url,
                maxPoolSize=10,
                minPoolSize=1,
                serverSelectionTimeoutMS=5000,
            )
            logger.info(f"Connected to MongoDB: {self._database_name}")
        return DocumentStore._client
    
    def _get_collection(self) -> Collection:
        """Get the documents collection."""
        if self._collection is None:
            client = self._get_client()
            self._db = client[self._database_name]
            self._collection = self._db["documents"]
            self._ensure_indexes()
        return self._collection
    
    def _ensure_indexes(self) -> None:
        """Create indexes for efficient queries."""
        collection = self._collection
        if collection is not None:
            collection.create_index([("status", ASCENDING), ("createdAt", ASCENDING)])
            collection.create_index([("crawlSessionId", ASCENDING)])
            collection.create_index([("fileId", ASCENDING)], unique=True)
            collection.create_index([("sourceUrl", ASCENDING)])
            logger.info("MongoDB indexes ensured")
    
    @classmethod
    def from_config(cls) -> "DocumentStore":
        """Create DocumentStore from app configuration."""
        if cls._instance is None:
            from app.config import MONGODB_URL, MONGODB_DATABASE
            cls._instance = cls(MONGODB_URL, MONGODB_DATABASE)
        return cls._instance
    
    def create_document(
        self,
        original_file: str,
        source_url: str,
        file_path: str,
        crawl_session_id: str,
        file_size: int = 0,
        crawl_depth: int = 0,
        status: DocumentStatus = DocumentStatus.PENDING,
    ) -> CrawledDocument:
        """
        Create a new document record.
        
        Args:
            original_file: Original filename
            source_url: URL where PDF was found
            file_path: Local storage path
            crawl_session_id: Session ID for this crawl
            file_size: File size in bytes
            crawl_depth: Depth in crawl tree
            status: Initial status (PENDING for chunking, STORED for storage only)
            
        Returns:
            Created document
        """
        doc = CrawledDocument(
            original_file=original_file,
            source_url=source_url,
            file_path=file_path,
            crawl_session_id=crawl_session_id,
            file_size=file_size,
            crawl_depth=crawl_depth,
            status=status,
        )
        
        collection = self._get_collection()
        mongo_doc = doc.to_mongo_dict()
        collection.insert_one(mongo_doc)
        
        logger.info(f"Created document: {doc.file_id} (status={status.value})")
        return doc
    
    def get_by_file_id(self, file_id: str) -> Optional[CrawledDocument]:
        """Get document by file ID."""
        collection = self._get_collection()
        result = collection.find_one({"fileId": file_id})
        if result:
            return CrawledDocument.from_mongo_dict(result)
        return None
    
    def get_by_source_url(self, source_url: str) -> Optional[CrawledDocument]:
        """Get document by source URL (check for duplicates)."""
        collection = self._get_collection()
        result = collection.find_one({"sourceUrl": source_url, "isDeleted": False})
        if result:
            return CrawledDocument.from_mongo_dict(result)
        return None
    
    def get_pending_documents(self, limit: int = 10) -> List[CrawledDocument]:
        """
        Get documents pending processing.
        
        Args:
            limit: Maximum documents to return
            
        Returns:
            List of pending documents
        """
        collection = self._get_collection()
        cursor = collection.find(
            {"status": DocumentStatus.PENDING.value, "isDeleted": False}
        ).sort("createdAt", ASCENDING).limit(limit)
        
        return [CrawledDocument.from_mongo_dict(doc) for doc in cursor]
    
    def get_by_session(self, crawl_session_id: str) -> List[CrawledDocument]:
        """Get all documents from a crawl session."""
        collection = self._get_collection()
        cursor = collection.find(
            {"crawlSessionId": crawl_session_id, "isDeleted": False}
        ).sort("createdAt", ASCENDING)
        
        return [CrawledDocument.from_mongo_dict(doc) for doc in cursor]
    
    def update_status(
        self,
        file_id: str,
        status: DocumentStatus,
        error_message: Optional[str] = None,
        vector_count: int = 0,
        page_count: int = 0,
        updated_by: str = "system",
    ) -> bool:
        """
        Update document processing status.
        
        Args:
            file_id: Document file ID
            status: New status
            error_message: Error message if failed
            vector_count: Number of vectors stored
            page_count: Number of pages processed
            updated_by: Who made the update
            
        Returns:
            True if updated, False if not found
        """
        collection = self._get_collection()
        
        update_data = {
            "status": status.value,
            "updatedAt": datetime.utcnow(),
            "updatedBy": updated_by,
        }
        
        if error_message is not None:
            update_data["errorMessage"] = error_message
        if vector_count > 0:
            update_data["vectorCount"] = vector_count
        if page_count > 0:
            update_data["pageCount"] = page_count
        
        result = collection.update_one(
            {"fileId": file_id},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            logger.info(f"Updated document {file_id} status to {status.value}")
            return True
        return False
    
    def mark_processing(self, file_id: str) -> bool:
        """Mark document as being processed."""
        return self.update_status(file_id, DocumentStatus.PROCESSING, updated_by="worker")
    
    def mark_vectorized(
        self, file_id: str, vector_count: int, page_count: int = 0
    ) -> bool:
        """Mark document as successfully vectorized."""
        return self.update_status(
            file_id,
            DocumentStatus.VECTORIZED,
            vector_count=vector_count,
            page_count=page_count,
            updated_by="worker",
        )
    
    def mark_failed(self, file_id: str, error_message: str) -> bool:
        """Mark document as failed."""
        return self.update_status(
            file_id,
            DocumentStatus.FAILED,
            error_message=error_message,
            updated_by="worker",
        )
    
    def soft_delete(self, file_id: str, deleted_by: str = "admin") -> bool:
        """Soft delete a document."""
        collection = self._get_collection()
        result = collection.update_one(
            {"fileId": file_id},
            {"$set": {
                "isDeleted": True,
                "updatedAt": datetime.utcnow(),
                "updatedBy": deleted_by,
            }}
        )
        return result.modified_count > 0
    
    def get_stats(self, crawl_session_id: Optional[str] = None) -> dict:
        """Get document statistics."""
        collection = self._get_collection()
        
        match_filter = {"isDeleted": False}
        if crawl_session_id:
            match_filter["crawlSessionId"] = crawl_session_id
        
        pipeline = [
            {"$match": match_filter},
            {"$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "totalVectors": {"$sum": "$vectorCount"},
            }}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        stats = {
            "total": 0,
            "pending": 0,
            "processing": 0,
            "vectorized": 0,
            "failed": 0,
            "stored": 0,
            "total_vectors": 0,
        }
        
        for r in results:
            status = r["_id"]
            count = r["count"]
            stats["total"] += count
            stats[status] = count
            if status == "vectorized":
                stats["total_vectors"] = r["totalVectors"]
        
        return stats
    
    def close(self) -> None:
        """Close the MongoDB connection."""
        if DocumentStore._client:
            DocumentStore._client.close()
            DocumentStore._client = None
            DocumentStore._instance = None
            logger.info("Closed MongoDB connection")
