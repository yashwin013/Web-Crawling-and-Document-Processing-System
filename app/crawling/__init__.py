"""
Web Crawling Pipeline Package.

A modular, stage-based pipeline for web scraping with smart OCR decisions.
"""

from app.crawling.pipeline import WebScrapingPipeline, run_pipeline
from app.crawling.models.document import Document, Page, PageContent
from app.crawling.models.config import PipelineConfig, CrawlConfig

__all__ = [
    "WebScrapingPipeline",
    "run_pipeline",
    "Document",
    "Page", 
    "PageContent",
    "PipelineConfig",
    "CrawlConfig",
]
