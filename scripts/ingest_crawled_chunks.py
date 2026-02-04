
import asyncio
import json
import logging
from pathlib import Path
from typing import List
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.docling.qdrant_service import DoclingQdrantService
from app.docling.qdrant_adapter import DoclingChunkMetadata
from app.config import get_logger

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = get_logger("ingest_script")

async def ingest_chunks():
    """Ingest chunks from all_chunks.json into Qdrant."""
    
    chunks_file = Path("outputs/scraped/all_chunks.json")
    if not chunks_file.exists():
        logger.error(f"Chunks file not found: {chunks_file}")
        return

    logger.info(f"Reading chunks from {chunks_file}...")
    data = json.loads(chunks_file.read_text(encoding="utf-8"))
    
    parents = data.get("parents", [])
    children = data.get("children", [])
    
    all_chunks = parents + children
    logger.info(f"Found {len(all_chunks)} chunks to ingest.")

    # Initialize Qdrant service
    try:
        service = await DoclingQdrantService.from_config()
    except Exception as e:
        logger.error(f"Failed to initialize Qdrant service: {e}")
        return

    # Group chunks by URL (to act as pseudo-file)
    chunks_by_url = {}
    for chunk in all_chunks:
        url = chunk.get("url", "unknown")
        if url not in chunks_by_url:
            chunks_by_url[url] = []
        chunks_by_url[url].append(chunk)

    total_inserted = 0

    for url, chunks in chunks_by_url.items():
        logger.info(f"Processing {len(chunks)} chunks for URL: {url}")
        
        # Convert to DoclingChunkMetadata
        metadata_list: List[DoclingChunkMetadata] = []
        
        for chunk in chunks:
            text = chunk.get("text", "")
            if not text:
                continue
                
            # Map fields
            metadata = DoclingChunkMetadata(
                chunk_id=chunk.get("id"),
                text=text,
                page_number=1, # Web pages treated as single page
                chunk_index=chunk.get("chunk_index", 0),
                doc_items_refs=[],
                has_image=False, # Could parse text for ![]()
                token_count=int(len(text.split()) * 1.3), # Rough estimate if not present
                heading_text=None,
                context_text=text[:500] 
            )
            metadata_list.append(metadata)

        if not metadata_list:
            continue

        # Use URL as filename and a derived ID as file_id
        import hashlib
        file_id = hashlib.md5(url.encode()).hexdigest()
        
        try:
            inserted = await service.insert_docling_chunks(
                chunks_metadata=metadata_list,
                file_id=file_id,
                filename=url,
                check_duplicates=True
            )
            total_inserted += inserted
        except Exception as e:
            logger.error(f"Failed to ingest chunks for {url}: {e}")

    logger.info(f"Ingestion complete. Total vectors inserted: {total_inserted}")

if __name__ == "__main__":
    asyncio.run(ingest_chunks())
