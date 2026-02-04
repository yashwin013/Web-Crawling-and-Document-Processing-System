"""
Process all pending PDFs from MongoDB files collection.
Run this after crawling to vectorize all downloaded PDFs.
"""
import asyncio
from app.docling.pipeline import create_vector_pipeline

async def main():
    print("=" * 60)
    print("  PROCESSING PENDING FILES")
    print("=" * 60)
    
    # Process all files for 'system_crawler' user (used during crawling)
    await create_vector_pipeline(createdby="system_crawler")
    
    print("\n" + "=" * 60)
    print("  PROCESSING COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
