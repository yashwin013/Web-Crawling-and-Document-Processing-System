"""
Language filtering stage.
Detects language of extracted text and filters out non-English content.
"""

from typing import Optional
from langdetect import detect, LangDetectException

from app.crawling.stages.base import PipelineStage
from app.crawling.models.document import Document, Page

from app.config import get_logger

logger = get_logger(__name__)


class LanguageFilterStage(PipelineStage):
    """
    Filters out pages that are not in English.
    """
    
    @property
    def name(self) -> str:
        return "language_filter"
    
    async def process(self, document: Document) -> Document:
        """
        Process document and filter non-English content.
        
        Args:
            document: Document to process
            
        Returns:
            Document with filtered pages
        """
        logger.info("Starting language filtering...")
        
        pages_to_keep = []
        filtered_count = 0
        
        for page in document.pages:
            # If no content, keep it
            if not page.content or not page.content.text:
                pages_to_keep.append(page)
                continue
                
            original_text = page.content.text
            # Split into paragraphs (assume double newline is paragraph break)
            paragraphs = original_text.split('\n\n')
            
            english_paragraphs = []
            removed_chars = 0
            
            for p in paragraphs:
                cleaned_p = p.strip()
                if not cleaned_p:
                    continue
                    
                # Keep very short lines (headers, nav items, numbers) - reduced risk of false positive
                if len(cleaned_p) < 30:
                    english_paragraphs.append(p)
                    continue
                    
                try:
                    # Detect language of the paragraph
                    # Fast check: if it has no latin characters, it's definitely not English
                    # But langdetect handles this well.
                    lang = detect(cleaned_p)
                    
                    if lang == 'en':
                        english_paragraphs.append(p)
                    else:
                        # Log only if substantial text is removed
                        if len(cleaned_p) > 50:
                            logger.debug(f"Removing non-English paragraph ({lang}): {cleaned_p[:50]}...")
                        removed_chars += len(p)
                except LangDetectException:
                    # If uncertain, safe to keep (often numbers/symbols)
                    english_paragraphs.append(p)
            
            # Reconstruct the page text
            # Only keep the page if it still has meaningful content
            if english_paragraphs:
                new_text = "\n\n".join(english_paragraphs)
                if removed_chars > 0:
                     logger.info(f"Filtered {removed_chars} chars of non-English text from {page.url}")
                     # Update the page content
                     page.content.text = new_text
                
                pages_to_keep.append(page)
            else:
                 logger.info(f"Dropped page {page.url} - No English content remaining.")
                 filtered_count += 1
        
        document.pages = pages_to_keep
        
        logger.info(f"Language filter complete. Removed {filtered_count} fully non-English pages.")
        return document
