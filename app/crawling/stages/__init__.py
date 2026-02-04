"""Pipeline stages package."""

from app.crawling.stages.base import PipelineStage
from app.crawling.stages.text_extractor import TextExtractorStage
from app.crawling.stages.ocr_decision import OCRDecisionStage
from app.crawling.stages.ocr_processor import OCRProcessorStage
from app.crawling.stages.chunker import ChunkerStage

# Lazy import for CrawlerStage (requires playwright)
def get_crawler_stage():
    """Get CrawlerStage (imports playwright on demand)."""
    from app.crawling.stages.crawler import CrawlerStage
    return CrawlerStage

__all__ = [
    "PipelineStage",
    "TextExtractorStage",
    "OCRDecisionStage",
    "OCRProcessorStage",
    "ChunkerStage",
    "get_crawler_stage",
]
