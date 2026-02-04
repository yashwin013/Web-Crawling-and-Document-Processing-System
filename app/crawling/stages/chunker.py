"""
Chunker stage - splits text into chunks for embeddings.
"""

import re
from typing import Optional

from app.crawling.stages.base import PipelineStage
from app.crawling.models.document import Document, Page

from app.config import get_logger

logger = get_logger(__name__)


class ChunkerStage(PipelineStage):
    """
    Chunker stage.
    
    Splits page content into chunks optimized for embedding
    and retrieval.
    
    Features:
    - Sentence-aware splitting
    - Semantic break detection
    - Configurable chunk sizes
    - Parent/child hierarchy
    """
    
    @property
    def name(self) -> str:
        return "chunker"
    
    async def process(self, document: Document) -> Document:
        """
        Chunk all pages in document.
        
        Args:
            document: Document with extracted text
            
        Returns:
            Document with chunks populated
        """
        config = self.config.chunker if self.config else None
        min_words = config.dom_chunk_min_words if config else 100
        max_words = config.dom_chunk_max_words if config else 300
        overlap = config.chunk_overlap_words if config else 30
        
        total_parents = 0
        total_children = 0
        
        for page in document.pages:
            if not page.content or not page.content.text:
                continue
            
            try:
                parents, children = self._chunk_page(
                    page, min_words, max_words, overlap
                )
                page.parent_chunks = parents
                page.child_chunks = children
                
                total_parents += len(parents)
                total_children += len(children)
                
            except Exception as e:
                logger.error(f"Chunking failed for {page.url}: {e}")
        
        logger.info(f"Created {total_parents} parent, {total_children} child chunks")
        
        return document
    
    def _chunk_page(
        self, page: Page, min_words: int, max_words: int, overlap: int
    ) -> tuple[list[dict], list[dict]]:
        """Chunk a single page."""
        text = page.content.text
        doc_id = self._get_doc_id(page)
        
        # Split into sentences
        sentences = self._split_into_sentences(text)
        
        # Group into chunks
        chunks = self._group_sentences(sentences, min_words, max_words, overlap)
        
        # Create parent/child structure
        parents = []
        children = []
        
        for i, chunk_text in enumerate(chunks):
            chunk_id = f"{doc_id}_chunk_{i}"
            
            parent = {
                "id": chunk_id,
                "text": chunk_text,
                "doc_id": doc_id,
                "url": page.url,
                "chunk_index": i,
                "type": "parent",
                "word_count": len(chunk_text.split()),
                "source": page.content.source.value,
            }
            parents.append(parent)
            
            # Create child chunks (smaller overlapping segments)
            child_sentences = self._split_into_sentences(chunk_text)
            for j, sent in enumerate(child_sentences):
                if len(sent.split()) >= 10:  # Minimum 10 words
                    child = {
                        "id": f"{chunk_id}_child_{j}",
                        "text": sent,
                        "parent_id": chunk_id,
                        "doc_id": doc_id,
                        "url": page.url,
                        "type": "child",
                        "word_count": len(sent.split()),
                        "source": page.content.source.value,
                    }
                    children.append(child)
        
        return parents, children
    
    def _split_into_sentences(self, text: str) -> list[str]:
        """Split text into sentences."""
        # Handle common abbreviations
        text = re.sub(r'\b(Dr|Mr|Mrs|Ms|Prof|Inc|Ltd|Jr|Sr)\.',
                      r'\1<PERIOD>', text)
        
        # Split on sentence endings
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
        # Restore abbreviations
        sentences = [s.replace('<PERIOD>', '.') for s in sentences]
        
        return [s.strip() for s in sentences if s.strip()]
    
    def _group_sentences(
        self,
        sentences: list[str],
        min_words: int,
        max_words: int,
        overlap: int,
    ) -> list[str]:
        """Group sentences into chunks."""
        chunks = []
        current_chunk = []
        current_words = 0
        
        for sentence in sentences:
            sentence_words = len(sentence.split())
            
            # Check if adding this sentence exceeds max
            if current_words + sentence_words > max_words and current_words >= min_words:
                # Save current chunk
                chunks.append(" ".join(current_chunk))
                
                # Start new chunk with overlap
                overlap_text = self._get_overlap(current_chunk, overlap)
                current_chunk = [overlap_text] if overlap_text else []
                current_words = len(overlap_text.split()) if overlap_text else 0
            
            current_chunk.append(sentence)
            current_words += sentence_words
        
        # Don't forget the last chunk
        if current_chunk and current_words >= min_words // 2:
            chunks.append(" ".join(current_chunk))
        
        return chunks
    
    def _get_overlap(self, sentences: list[str], target_words: int) -> str:
        """Get overlap text from end of sentences."""
        if not sentences:
            return ""
        
        overlap_sentences = []
        word_count = 0
        
        for sentence in reversed(sentences):
            words = len(sentence.split())
            if word_count + words <= target_words:
                overlap_sentences.insert(0, sentence)
                word_count += words
            else:
                break
        
        return " ".join(overlap_sentences)
    
    def _get_doc_id(self, page: Page) -> str:
        """Generate document ID from page."""
        from urllib.parse import urlparse
        
        parsed = urlparse(page.url)
        path = parsed.path.replace("/", "_").strip("_")
        return f"{parsed.netloc}_{path}"[:100]  # Limit length
