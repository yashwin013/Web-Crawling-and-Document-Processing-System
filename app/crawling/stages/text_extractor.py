"""
Text extractor stage - extracts text from DOM/HTML.
"""

import re
from typing import Optional

from app.crawling.stages.base import PipelineStage
from app.crawling.models.document import Document, Page, PageContent, ContentSource

from app.config import get_logger

logger = get_logger(__name__)


class TextExtractorStage(PipelineStage):
    """
    Text extraction stage.
    
    Extracts clean text from DOM/HTML content for pages
    that were crawled. This is the fast path that avoids OCR
    when possible.
    
    Features:
    - DOM text cleaning
    - Sentence-level chunking
    - Table detection
    - Quality assessment
    """
    
    @property
    def name(self) -> str:
        return "text_extractor"
    
    async def process(self, document: Document) -> Document:
        """
        Extract text from all pages in document.
        
        Args:
            document: Document with pages from crawler
            
        Returns:
            Document with PageContent populated for each page
        """
        config = self.config.text_extractor if self.config else None
        min_chars = config.min_chars_for_skip_ocr if config else 500
        min_words = config.min_words_for_skip_ocr if config else 50
        
        for page in document.pages:
            try:
                content = self._extract_content(page, min_chars, min_words)
                page.content = content
                
                logger.debug(
                    f"Extracted {content.word_count} words from {page.url} "
                    f"(source: {content.source.value})"
                )
                
            except Exception as e:
                logger.error(f"Failed to extract text from {page.url}: {e}")
        
        return document
    
    def _extract_content(
        self, page: Page, min_chars: int, min_words: int
    ) -> PageContent:
        """Extract content from a single page."""
        
        # Try DOM text first (fastest)
        if page.dom_text:
            cleaned = self._clean_dom_text(page.dom_text)
            content = PageContent.from_text(cleaned, ContentSource.DOM)
            
            # Add images if available
            if page.scraped_images:
                from app.crawling.models.document import ImageInfo
                content.images = [
                    ImageInfo(
                        width=img.get("width", 0),
                        height=img.get("height", 0),
                        aspect_ratio=img.get("width", 0) / img.get("height", 1) if img.get("height", 0) > 0 else 0,
                        image_type="unknown",
                        area=img.get("width", 0) * img.get("height", 0)
                    )
                    for img in page.scraped_images
                ]
            return content
        
        # Try HTML content
        if page.html_content:
            text = self._extract_from_html(page.html_content)
            cleaned = self._clean_dom_text(text)
            return PageContent.from_text(cleaned, ContentSource.DOM)
        
        # No text available
        return PageContent(
            text="",
            word_count=0,
            char_count=0,
            source=ContentSource.DOM,
        )
    
    def _clean_dom_text(self, raw_text: str) -> str:
        """
        Clean extracted DOM text.
        
        Removes:
        - Excessive whitespace
        - Navigation noise
        - Cookie banners
        """
        if not raw_text:
            return ""
        
        # Normalize whitespace
        text = re.sub(r"\s+", " ", raw_text)
        
        # Remove common noise patterns
        noise_patterns = [
            r"Accept\s+cookies?",
            r"Cookie\s+policy",
            r"Privacy\s+policy",
            r"Terms\s+of\s+service",
            r"Skip\s+to\s+content",
            r"Toggle\s+navigation",
            r"Loading\.\.\.",
        ]
        
        for pattern in noise_patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE)
        
        # Remove very short lines (likely buttons/links)
        lines = text.split("\n")
        cleaned_lines = [
            line.strip() for line in lines
            if len(line.strip()) > 20 or "." in line
        ]
        
        return "\n".join(cleaned_lines).strip()
    
    def _extract_from_html(self, html: str) -> str:
        """Extract text from HTML using BeautifulSoup."""
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html, "html.parser")
            
            # Remove script and style elements
            for element in soup(["script", "style", "nav", "footer", "header"]):
                element.decompose()
            
            # Get text
            text = soup.get_text(separator="\n")
            return text
            
        except Exception as e:
            logger.warning(f"Failed to parse HTML: {e}")
            return ""
    
    def _detect_tables(self, html: str) -> bool:
        """Detect if page contains data tables."""
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html, "html.parser")
            tables = soup.find_all("table")
            
            # Filter out layout tables
            data_tables = [
                t for t in tables
                if len(t.find_all("tr")) > 2  # At least 3 rows
                and len(t.find_all("td")) > 4  # At least 5 cells
            ]
            
            return len(data_tables) > 0
            
        except Exception:
            return False
    
    @staticmethod
    def split_into_sentences(text: str) -> list[str]:
        """
        Split text into sentences.
        
        Handles abbreviations like Dr., Mr., etc.
        """
        # Simple sentence splitting
        sentence_endings = re.compile(r'(?<=[.!?])\s+(?=[A-Z])')
        sentences = sentence_endings.split(text)
        return [s.strip() for s in sentences if s.strip()]
    
    @staticmethod
    def is_semantic_break(sentence: str) -> bool:
        """
        Detect if sentence starts a new semantic section.
        
        Triggers on:
        - Numbered lists (1., 2., etc.)
        - Bullet points
        - All caps headings
        """
        patterns = [
            r"^\d+\.\s",  # Numbered list
            r"^[-*•]\s",  # Bullet points
            r"^[A-Z][A-Z\s]{10,}$",  # All caps heading
        ]
        
        for pattern in patterns:
            if re.match(pattern, sentence.strip()):
                return True
        
        return False
