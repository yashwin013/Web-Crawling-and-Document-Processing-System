"""
Web Crawl RAG Pipeline Application Package.

This package provides a complete pipeline for:
- Web crawling and PDF extraction
- Document processing with Docling OCR
- Vector storage in Qdrant
- RAG-based querying
"""

from app.config import settings

__version__ = "1.0.0"
__all__ = ["settings"]
