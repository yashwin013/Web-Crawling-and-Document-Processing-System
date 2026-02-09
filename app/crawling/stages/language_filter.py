"""
Language filtering stage.
Detects language of extracted text and filters out non-English content.
Uses sentence-level detection for better granularity while maintaining accuracy.
"""

import re
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
        Process document and filter non-English content at sentence level.
        
        Args:
            document: Document to process
            
        Returns:
            Document with filtered pages
        """
        logger.info("Starting language filtering (sentence-level)...")
        
        pages_to_keep = []
        filtered_count = 0
        
        for page in document.pages:
            # If no content, keep it
            if not page.content or not page.content.text:
                pages_to_keep.append(page)
                continue
                
            original_text = page.content.text
            
            # Split into sentences using regex (better than simple split)
            # Handles: periods, question marks, exclamation marks, newlines
            sentences = re.split(r'(?<=[.!?])\s+|\n+', original_text)
            
            english_sentences = []
            removed_chars = 0
            
            for sentence in sentences:
                cleaned_sentence = sentence.strip()
                if not cleaned_sentence:
                    continue
                
                # Keep very short text (likely headers, numbers, nav items)
                # Reduced threshold from 30 to 15 for better sentence handling
                if len(cleaned_sentence) < 15:
                    english_sentences.append(sentence)
                    continue
                
                # Skip if mostly numbers/symbols (keep without detection)
                alpha_ratio = sum(c.isalpha() for c in cleaned_sentence) / len(cleaned_sentence)
                if alpha_ratio < 0.5:  # Less than 50% alphabetic characters
                    english_sentences.append(sentence)
                    continue
                
                try:
                    # Detect language of the sentence
                    lang = detect(cleaned_sentence)
                    
                    if lang == 'en':
                        english_sentences.append(sentence)
                    else:
                        # Log only if substantial text is removed
                        if len(cleaned_sentence) > 30:
                            logger.debug(f"Removing non-English sentence ({lang}): {cleaned_sentence[:50]}...")
                        removed_chars += len(sentence)
                except LangDetectException:
                    # If uncertain, safe to keep (often mixed content, symbols, etc.)
                    english_sentences.append(sentence)
            
            # Reconstruct the page text
            # Join sentences with appropriate spacing
            if english_sentences:
                # Preserve original spacing as much as possible
                new_text = ' '.join(english_sentences)
                # Clean up multiple spaces
                new_text = re.sub(r'\s+', ' ', new_text).strip()
                
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
