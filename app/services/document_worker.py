"""
Document Processing Worker.

Background worker that picks up pending documents from MongoDB,
processes them with Docling, and stores vectors in Qdrant.
"""

import asyncio
import time
from pathlib import Path
from typing import Optional

from app.config import get_logger, PDF_STORAGE_PATH
from app.services.document_store import DocumentStore
from app.schemas.document import DocumentStatus, CrawledDocument

logger = get_logger(__name__)


class DocumentWorker:
    """
    Background worker for processing documents.
    
    Polls MongoDB for pending documents, processes them with Docling,
    and updates their status.
    
    Usage:
        worker = DocumentWorker()
        await worker.run()  # Run continuously
        
        # Or process one batch
        await worker.process_pending(limit=10)
    """
    
    def __init__(
        self,
        poll_interval: float = 5.0,
        batch_size: int = 5,
    ):
        """
        Initialize worker.
        
        Args:
            poll_interval: Seconds between polling for new documents
            batch_size: Number of documents to process per batch
        """
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._store: Optional[DocumentStore] = None
        self._running = False
    
    @property
    def store(self) -> DocumentStore:
        """Get document store (lazy initialization)."""
        if self._store is None:
            self._store = DocumentStore.from_config()
        return self._store
    
    async def run(self, max_iterations: Optional[int] = None) -> None:
        """
        Run worker continuously.
        
        Args:
            max_iterations: Maximum iterations (None for infinite)
        """
        self._running = True
        iteration = 0
        
        logger.info(f"Starting document worker (poll_interval={self.poll_interval}s)")
        
        while self._running:
            if max_iterations is not None and iteration >= max_iterations:
                break
            
            try:
                processed = await self.process_pending(limit=self.batch_size)
                
                if processed == 0:
                    # No work to do, wait before polling again
                    await asyncio.sleep(self.poll_interval)
                
                iteration += 1
                
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(self.poll_interval)
        
        logger.info("Document worker stopped")
    
    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
    
    async def process_pending(self, limit: int = 10) -> int:
        """
        Process pending documents.
        
        Args:
            limit: Maximum documents to process
            
        Returns:
            Number of documents processed
        """
        pending = self.store.get_pending_documents(limit=limit)
        
        if not pending:
            return 0
        
        logger.info(f"Processing {len(pending)} pending documents")
        
        processed = 0
        for doc in pending:
            try:
                await self._process_document(doc)
                processed += 1
            except Exception as e:
                logger.error(f"Failed to process {doc.file_id}: {e}")
                self.store.mark_failed(doc.file_id, str(e))
        
        return processed
    
    async def _process_document(self, doc: CrawledDocument) -> None:
        """
        Process a single document.
        
        Args:
            doc: Document to process
        """
        file_id = doc.file_id
        file_path = Path(doc.file_path)
        
        logger.info(f"Processing document: {file_id}")
        
        # Mark as processing
        self.store.mark_processing(file_id)
        
        # Check file exists
        if not file_path.exists():
            raise FileNotFoundError(f"PDF file not found: {file_path}")
        
        try:
            # Import Docling processor here to avoid circular imports
            from app.docling import AsyncDocumentProcessor
            
            # Initialize processor
            processor = await AsyncDocumentProcessor.from_config()
            
            # Process document
            result = await processor.process_document_main(
                file_id=file_id.replace(".pdf", ""),
                file_name=doc.original_file,
                pdfpath=str(file_path),
            )
            
            if result:
                vector_count = result.get("chunks", 0)
                page_count = result.get("pages", 0)
                
                # Mark as vectorized
                self.store.mark_vectorized(
                    file_id,
                    vector_count=vector_count,
                    page_count=page_count,
                )
                
                logger.info(
                    f"Vectorized {file_id}: {vector_count} chunks, {page_count} pages"
                )
            else:
                raise RuntimeError("Docling returned no result")
                
        except Exception as e:
            logger.error(f"Processing failed for {file_id}: {e}")
            self.store.mark_failed(file_id, str(e))
            raise
    
    async def process_by_session(self, crawl_session_id: str) -> dict:
        """
        Process all pending documents from a crawl session.
        
        Args:
            crawl_session_id: Session ID to process
            
        Returns:
            Processing statistics
        """
        documents = self.store.get_by_session(crawl_session_id)
        pending = [d for d in documents if d.status == DocumentStatus.PENDING]
        
        logger.info(
            f"Processing session {crawl_session_id}: "
            f"{len(pending)}/{len(documents)} pending"
        )
        
        stats = {
            "total": len(documents),
            "pending": len(pending),
            "processed": 0,
            "failed": 0,
        }
        
        for doc in pending:
            try:
                await self._process_document(doc)
                stats["processed"] += 1
            except Exception as e:
                logger.error(f"Failed to process {doc.file_id}: {e}")
                stats["failed"] += 1
        
        return stats


async def run_worker(
    poll_interval: float = 5.0,
    batch_size: int = 5,
    max_iterations: Optional[int] = None,
) -> None:
    """
    Run the document worker.
    
    This is a convenience function to start the worker.
    
    Args:
        poll_interval: Seconds between polling
        batch_size: Documents per batch
        max_iterations: Max iterations (None for infinite)
    """
    worker = DocumentWorker(
        poll_interval=poll_interval,
        batch_size=batch_size,
    )
    await worker.run(max_iterations=max_iterations)


if __name__ == "__main__":
    # Run worker when executed directly
    import argparse
    
    parser = argparse.ArgumentParser(description="Document processing worker")
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval")
    parser.add_argument("--batch", type=int, default=5, help="Batch size")
    parser.add_argument("--once", action="store_true", help="Process once and exit")
    
    args = parser.parse_args()
    
    if args.once:
        asyncio.run(run_worker(
            poll_interval=args.interval,
            batch_size=args.batch,
            max_iterations=1,
        ))
    else:
        asyncio.run(run_worker(
            poll_interval=args.interval,
            batch_size=args.batch,
        ))
