"""
Web Scraping Pipeline Orchestrator.

Chains pipeline stages together for end-to-end web scraping.
"""

import time
import json
import aiofiles
from pathlib import Path
from typing import Optional

from app.crawling.stages.base import PipelineStage
from app.crawling.stages.text_extractor import TextExtractorStage
from app.crawling.stages.ocr_decision import OCRDecisionStage
from app.crawling.stages.ocr_processor import OCRProcessorStage
from app.crawling.stages.language_filter import LanguageFilterStage
from app.crawling.stages.chunker import ChunkerStage
from app.crawling.models.document import Document
from app.crawling.models.config import PipelineConfig

from app.config import get_logger

logger = get_logger(__name__)


class WebScrapingPipeline:
    """
    Orchestrates the web scraping pipeline.
    
    Chains multiple stages together and executes them in sequence.
    Each stage receives a Document and returns a modified Document.
    
    Usage:
        config = PipelineConfig(output_dir=Path("outputs"))
        
        pipeline = (
            WebScrapingPipeline(config)
            .add_stage(CrawlerStage())
            .add_stage(TextExtractorStage())
            .add_stage(OCRDecisionStage())
            .add_stage(OCRProcessorStage())
            .add_stage(ChunkerStage())
        )
        
        result = await pipeline.run("https://example.com")
        print(f"Processed {result.total_pages} pages")
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize pipeline.
        
        Args:
            config: Pipeline configuration (uses defaults if not provided)
        """
        self.config = config or PipelineConfig()
        self.stages: list[PipelineStage] = []
    
    def add_stage(self, stage: PipelineStage) -> "WebScrapingPipeline":
        """
        Add a stage to the pipeline.
        
        Args:
            stage: Pipeline stage to add
            
        Returns:
            Self for fluent chaining
        """
        stage.config = self.config
        self.stages.append(stage)
        return self
    
    async def run(self, start_url: str) -> Document:
        """
        Execute the pipeline on a URL.
        
        Args:
            start_url: URL to start crawling from
            
        Returns:
            Document with all processed data
        """
        logger.info(f"Starting pipeline for {start_url}")
        logger.info(f"Stages: {[s.name for s in self.stages]}")
        
        start_time = time.time()
        
        # Initialize document
        document = Document(
            start_url=start_url,
            output_dir=self.config.output_dir,
            crawl_depth=self.config.crawl.max_depth,
            max_pages=self.config.crawl.max_pages,
        )
        
        # Execute each stage
        for stage in self.stages:
            try:
                document = await stage.run(document)
                
                # Save intermediate results if configured
                if self.config.save_intermediate:
                    await self._save_intermediate(document, stage.name)
                    
            except Exception as e:
                logger.error(f"Pipeline failed at stage {stage.name}: {e}")
                raise
        
        elapsed = time.time() - start_time
        logger.info(f"Pipeline complete in {elapsed:.1f}s")
        logger.info(
            f"Results: {document.total_pages} pages, "
            f"{document.total_chunks} chunks"
        )
        
        # Save final results
        await self._save_results(document)
        
        return document
    
    async def _save_intermediate(self, document: Document, stage_name: str) -> None:
        """Save intermediate results after each stage."""
        output_dir = self.config.output_dir
        intermediate_dir = output_dir / "intermediate"
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        
        state = {
            "stage": stage_name,
            "pages_count": len(document.pages),
            "pages_scraped": document.pages_scraped,
            "pdfs_downloaded": document.pdfs_downloaded,
            "pages_failed": document.pages_failed,
        }
        
        state_file = intermediate_dir / f"after_{stage_name}.json"
        async with aiofiles.open(state_file, 'w') as f:
            await f.write(json.dumps(state, indent=2))
    
    async def _save_results(self, document: Document) -> None:
        """Save final pipeline results."""
        output_dir = self.config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save chunks
        parents, children = document.get_all_chunks()
        
        chunks_file = output_dir / "all_chunks.json"
        chunks_data = json.dumps({
            "parents": parents,
            "children": children,
            "metadata": {
                "start_url": document.start_url,
                "total_pages": document.total_pages,
                "total_parents": len(parents),
                "total_children": len(children),
                "duration_seconds": document.duration_seconds,
            }
        }, indent=2)
        
        async with aiofiles.open(chunks_file, 'w') as f:
            await f.write(chunks_data)
        
        logger.info(f"Saved {len(parents)} parent chunks, {len(children)} child chunks")
    
    @classmethod
    def create_default(
        cls, 
        output_dir: Optional[Path] = None,
        config: Optional[PipelineConfig] = None
    ) -> "WebScrapingPipeline":
        """
        Create a pipeline with all default stages.
        
        Args:
            output_dir: Output directory (optional)
            config: Pipeline configuration (optional)
            
        Returns:
            Configured pipeline ready to run
        """
        from app.crawling.stages.crawler import CrawlerStage
        
        if config is None:
            config = PipelineConfig()
            
        if output_dir:
            config.output_dir = output_dir
        
        return (
            cls(config)
            .add_stage(CrawlerStage())
            .add_stage(TextExtractorStage())
            .add_stage(OCRDecisionStage())
            .add_stage(OCRProcessorStage())
            .add_stage(LanguageFilterStage())
            .add_stage(ChunkerStage())
        )
    
    def __repr__(self) -> str:
        stages = ", ".join(s.name for s in self.stages)
        return f"<WebScrapingPipeline(stages=[{stages}])>"


# Convenience function for simple usage
async def run_pipeline(
    start_url: str,
    output_dir: Optional[Path] = None,
    config: Optional[PipelineConfig] = None,
) -> Document:
    """
    Run the web scraping pipeline on a URL.
    
    This is a convenience function for simple usage.
    
    Args:
        start_url: URL to start crawling
        output_dir: Output directory
        config: Pipeline configuration
        
    Returns:
        Processed document
    """
    if config is None:
        config = PipelineConfig()
    
    if output_dir:
        config.output_dir = output_dir
    
    pipeline = WebScrapingPipeline.create_default(config=config)
    return await pipeline.run(start_url)
