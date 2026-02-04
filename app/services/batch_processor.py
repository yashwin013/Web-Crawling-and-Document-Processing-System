"""
Batch PDF processing for RAG.

Processes multiple PDFs from a folder into a unified vector database
with document-level metadata for citation support.

Features:
- Batch-specific chunking config (larger chunks for many PDFs)
- Multiprocessing for parallel OCR
- Skips downloaded PDFs (only processes scraped pages)
"""
import sys
import json
import hashlib
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import uuid

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_logger, OUTPUT_DIR, DATA_DIR
from app.docling.qdrant_service import DoclingQdrantService, PageChunk, DocumentMetadata
from app.crawling.models.document import OCRAction
from app.crawling.stages.ocr_decision import OCRDecisionStage
from app.crawling.stages.text_extractor import TextExtractorStage
from app.crawling.stages.chunker import ChunkerStage

logger = get_logger(__name__)


# ============== Batch-Specific Chunking Config ==============
# These are SEPARATE from config.py settings (used for single PDFs)
# Optimized for processing many PDFs at once

BATCH_CHUNK_MIN_TOKENS = 50    # Merge small chunks (was 20 for single PDF)
BATCH_CHUNK_MAX_TOKENS = 800   # Allow larger chunks (was 512 for single PDF)


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    min_tokens: int = BATCH_CHUNK_MIN_TOKENS
    max_tokens: int = BATCH_CHUNK_MAX_TOKENS
    max_workers: int = 4  # Parallel processes (adjust based on CPU/GPU)
    skip_pdfs_folder: bool = True  # Skip downloaded PDFs in pdfs/ subfolder
    use_docling: bool = False  # Use Docling for high-quality PDF processing


@dataclass
class BatchResult:
    """Result of batch processing."""
    pdfs_processed: list[str] = field(default_factory=list)
    pdfs_failed: list[str] = field(default_factory=list)
    total_chunks: int = 0
    total_parents: int = 0
    total_children: int = 0


def extract_document_title(ordered_items: list[dict]) -> str:
    """
    Extract document title from the first few text items.
    Usually the first line is the title.
    """
    for item in ordered_items[:5]:
        text = item.get("text", "").strip()
        if text and len(text) > 5 and len(text) < 200:
            # Likely a title
            return text
    return "Untitled Document"


def add_document_metadata(
    chunks: list[dict],
    doc_id: str,
    doc_title: str,
    source_url: Optional[str] = None,
    source_type: str = "web_crawl"
) -> list[dict]:
    """
    Add document-level metadata to all chunks.
    
    Args:
        chunks: List of chunks
        doc_id: Unique document identifier (usually filename)
        doc_title: Human-readable document title
        source_url: Original URL if from web crawl
        source_type: Type of source (web_crawl, uploaded, etc.)
        
    Returns:
        Chunks with added metadata
    """
    for chunk in chunks:
        chunk["doc_id"] = doc_id
        chunk["doc_title"] = doc_title
        chunk["source_url"] = source_url or ""
        chunk["source_type"] = source_type
    
    return chunks


def get_source_url_from_filename(filename: str) -> str:
    """
    Try to reconstruct source URL from filename.
    Crawled PDFs are named like: example_com_path_to_page.pdf
    """
    stem = Path(filename).stem
    # Replace underscores back to potential URL components
    # This is a best-effort reconstruction
    return f"https://{stem.replace('_', '/')}"


# Removed legacy _process_single_pdf_worker - using process_single_pdf directly

def process_single_pdf(
    pdf_path: Path,
    output_dir: Path,
    source_url: Optional[str] = None,
    config: Optional[BatchConfig] = None,
) -> tuple[list[dict], list[dict]]:
    """
    Process a single PDF and return parent/child chunks with metadata.
    
    Uses smart OCR decision: Skip OCR if DOM text is sufficient.
    
    Args:
        pdf_path: Path to PDF file
        output_dir: Directory for intermediate outputs
        source_url: Original source URL if known
        config: Batch configuration (optional)
        
    Returns:
        Tuple of (parent_chunks, child_chunks)
    """
    if config is None:
        config = BatchConfig()
        
    doc_id = pdf_path.stem
    url = source_url or get_source_url_from_filename(pdf_path.name)
    
    logger.info(f"Processing: {pdf_path.name}")
    
    # ============== SMART OCR DECISION (FIRST!) ==============
    # Note: For batch processing, we'll use a simplified approach
    # The full OCR decision logic is in app.crawling.stages.ocr_decision
    # For now, default to using Docling if config.use_docling is True
    
    # Try to extract text directly first
    dom_text = None  # TODO: Implement DOM text cache reading if needed
    
    # ============== SKIP OCR PATH (Fastest) ==============
    # Try text extraction first (if not using Docling)
    # For now, we'll primarily use Docling or fallback to basic extraction
    
    # ============== DOCLING PIPELINE ==============
    # Use Docling for high-quality PDF processing
    if config.use_docling:
        try:
            from app.docling.processor import DocumentProcessor
            
            logger.info(f"  📄 Using Docling for PDF processing")
            
            # Use Docling processor
            processor = DocumentProcessor()
            docling_chunks = processor.process_pdf_to_chunks(str(pdf_path))
            
            # Extract title from chunks
            doc_title = extract_document_title(docling_chunks) if docling_chunks else "Untitled"
            docling_chunks = add_document_metadata(docling_chunks, doc_id, doc_title, url, source_type="docling")
            
            logger.info(f"  → {len(docling_chunks)} enhanced chunks generated")
            return [], docling_chunks
            
        except ImportError:
            logger.warning("Docling not available, falling back to standard pipeline")
        except Exception as e:
            logger.error(f"Docling processing failed: {e}, falling back")
            import traceback
            traceback.print_exc()
            
    # ============== FALLBACK: Basic Text Extraction ==============
    # If Docling is not used or failed, extract text from PDF text layer
    logger.info(f"  📄 Using basic text extraction (fallback)")
    
    ordered_items = _extract_text_layer(pdf_path, doc_id, url)
    
    if not ordered_items:
        logger.warning(f"  No text extracted from {pdf_path.name}")
        return [], []
    
    # Extract title
    doc_title = extract_document_title(ordered_items)
    
    # Simple chunking - split into paragraphs
    chunks = []
    for idx, item in enumerate(ordered_items):
        chunk = {
            "text": item["text"],
            "chunk_id": f"{doc_id}_chunk_{idx}",
            "chunk_index": idx,
            "page": item.get("page", 1),
            "type": item.get("type", "paragraph"),
            "source": "pdf_text_layer"
        }
        chunks.append(chunk)
    
    # Add metadata
    chunks = add_document_metadata(chunks, doc_id, doc_title, url)
    
    logger.info(f"  → {len(chunks)} chunks extracted")
    
    # Return as child chunks only (no parent/child hierarchy in fallback)
    return [], chunks


# Removed _run_surya_pipeline - Surya integration moved to separate module


def _extract_text_layer(pdf_path: Path, doc_id: str, source_url: str) -> list[dict]:
    """
    Extract text directly from PDF text layer using PyMuPDF.
    
    This is the FASTEST path - no OCR needed.
    Only works for PDFs with embedded text (not scanned).
    
    Args:
        pdf_path: Path to PDF file
        doc_id: Document identifier
        source_url: Source URL for metadata
        
    Returns:
        List of ordered items compatible with chunking pipeline
    """
    try:
        import fitz  # PyMuPDF
        
        # Use context manager to ensure PDF is always closed
        with fitz.open(str(pdf_path)) as doc:
            ordered_items = []
            
            for page_num, page in enumerate(doc, 1):
                # Extract text blocks with position info
                blocks = page.get_text("blocks")
                
                for block_idx, block in enumerate(blocks):
                    # block = (x0, y0, x1, y1, text, block_no, block_type)
                    if block[6] == 0:  # Text block (not image)
                        text = block[4].strip()
                        if text and len(text) > 5:
                            # Detect if text looks like a heading
                            is_heading = (
                                len(text) < 100 and 
                                '\n' not in text and
                                (text.isupper() or text.endswith(':'))
                            )
                            
                            ordered_items.append({
                                "text": text,
                                "type": "heading" if is_heading else "paragraph",
                                "page": page_num,
                                "order": len(ordered_items),
                                "confidence": 1.0,  # PDF text is 100% accurate
                                "source": "pdf_text_layer",
                                "doc_id": doc_id,
                                "source_url": source_url,
                            })
            
            # PDF automatically closed by context manager
            logger.info(f"  Extracted {len(ordered_items)} items from PDF text layer")
            return ordered_items
        
    except ImportError:
        logger.warning("PyMuPDF not available")
        return []
    except Exception as e:
        logger.error(f"Failed to extract PDF text layer: {e}")
        return []


# Removed _run_hybrid_pipeline - logic simplified for batch processing


def process_batch(
    pdf_folder: Path,
    output_dir: Optional[Path] = None,
    skip_existing: bool = True,
    max_pdfs: Optional[int] = None,
    config: Optional[BatchConfig] = None,
    parallel: bool = False,
) -> BatchResult:
    """
    Process all PDFs in a folder.
    
    Args:
        pdf_folder: Folder containing PDF files
        output_dir: Directory for outputs
        skip_existing: Skip already processed PDFs
        max_pdfs: Maximum number of PDFs to process
        config: Batch configuration
        parallel: Use multiprocessing (faster but uses more resources)
        
    Returns:
        BatchResult with processing statistics
    """
    if config is None:
        config = BatchConfig()
    
    if output_dir is None:
        output_dir = OUTPUT_DIR / "batch"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all PDFs (ONLY in root folder, not pdfs/ subfolder)
    pdf_files = [f for f in pdf_folder.glob("*.pdf") if f.is_file()]
    if max_pdfs:
        pdf_files = pdf_files[:max_pdfs]
    
    logger.info(f"Found {len(pdf_files)} PDFs in {pdf_folder}")
    
    result = BatchResult()
    all_parent_chunks = []
    all_child_chunks = []
    
    # Track processed docs for skip_existing
    processed_file = output_dir / "processed.json"
    processed_docs = set()
    if skip_existing and processed_file.exists():
        processed_docs = set(json.loads(processed_file.read_text()))
        logger.info(f"Skipping {len(processed_docs)} already processed PDFs")
    
    # Filter to unprocessed PDFs
    pdfs_to_process = [
        (idx, pdf_path) for idx, pdf_path in enumerate(pdf_files, 1)
        if pdf_path.stem not in processed_docs
    ]
    
    if not pdfs_to_process:
        logger.info("All PDFs already processed!")
        return result
    
    logger.info(f"Processing {len(pdfs_to_process)} PDFs...")
    
    # CRITICAL: Pre-load Surya models ONCE before parallel processing
    # This prevents race condition when multiple threads try to load simultaneously
    if parallel:
        logger.info("Pre-loading models for parallel processing...")
        try:
            from app.services.gpu_manager import get_surya_predictors
            get_surya_predictors()  # Load models in main thread
            logger.info("Models loaded, starting parallel processing...")
        except Exception as e:
            logger.warning(f"Could not pre-load models: {e}")
    
    # Use ThreadPoolExecutor for pipeline parallelism
    # GPU handles OCR, CPU threads handle chunking in parallel
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def process_pdf_task(args):
        """Process a single PDF - can run in thread."""
        from app.services.gpu_manager import cleanup_after_pdf
        idx, pdf_path = args
        try:
            logger.info(f"[{idx}/{len(pdf_files)}] Processing: {pdf_path.name}")
            parent_chunks, child_chunks = process_single_pdf(pdf_path, output_dir, config=config)
            
            # CRITICAL: Cleanup GPU memory after each PDF
            cleanup_after_pdf()
            
            return (pdf_path.name, pdf_path.stem, parent_chunks, child_chunks, True)
        except Exception as e:
            logger.error(f"Failed to process {pdf_path.name}: {e}")
            # Cleanup even on failure
            cleanup_after_pdf()
            return (pdf_path.name, pdf_path.stem, [], [], False)
    
    # Process with thread pool (2 threads = 1 doing OCR, 1 doing chunking)
    max_workers = 2 if parallel else 1
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_pdf_task, args) for args in pdfs_to_process]
        
        for future in as_completed(futures):
            pdf_name, doc_id, parents, children, success = future.result()
            
            if success:
                all_parent_chunks.extend(parents)
                all_child_chunks.extend(children)
                result.pdfs_processed.append(pdf_name)
                processed_docs.add(doc_id)
            else:
                result.pdfs_failed.append(pdf_name)
    
    # Save processed list
    processed_file.write_text(json.dumps(list(processed_docs), indent=2))
    
    # Calculate totals
    result.total_parents = len(all_parent_chunks)
    result.total_children = len(all_child_chunks)
    result.total_chunks = result.total_parents + result.total_children
    
    # Save combined chunks
    chunks_file = output_dir / "all_chunks.json"
    all_chunks = all_parent_chunks + all_child_chunks
    chunks_file.write_text(
        json.dumps(all_chunks, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    logger.info(f"Saved {len(all_chunks)} chunks to {chunks_file}")
    
    # Save parent/child separately
    parents_file = output_dir / "all_parents.json"
    parents_file.write_text(
        json.dumps(all_parent_chunks, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    
    children_file = output_dir / "all_children.json"
    children_file.write_text(
        json.dumps(all_child_chunks, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    
    return result


def store_batch_to_qdrant(
    chunks_file: Path,
    collection_suffix: str = "batch"
) -> None:
    """
    Store batch-processed chunks to Qdrant using DoclingQdrantService.
    """
    from app.docling.qdrant_service import DoclingQdrantService, PageChunk, DocumentMetadata
    import asyncio
    
    # Convert to Path if string is passed
    if isinstance(chunks_file, str):
        chunks_file = Path(chunks_file)
    
    logger.info("Initializing Docling Qdrant Service...")
    # Use asyncio to call the async from_config method
    service = asyncio.run(DoclingQdrantService.from_config())
    
    # Load chunks
    chunks_data = json.loads(chunks_file.read_text(encoding='utf-8'))
    
    # Group by document (file_id or doc_id)
    docs = {}
    for chunk in chunks_data:
        doc_id = chunk.get("doc_id") or chunk.get("file_id") or "unknown_doc"
        if doc_id not in docs:
            docs[doc_id] = {
                "chunks": [],
                "filename": chunk.get("filename", f"{doc_id}.pdf"),
                "title": chunk.get("doc_title", "Untitled")
            }
        docs[doc_id]["chunks"].append(chunk)
    
    logger.info(f"Storing {len(chunks_data)} chunks across {len(docs)} documents to Qdrant")
    
    total_stored = 0
    
    for doc_id, doc_data in docs.items():
        raw_chunks = doc_data["chunks"]
        filename = doc_data["filename"]
        doc_title = doc_data["title"]
        
        # Sort by index if available
        raw_chunks.sort(key=lambda x: x.get("chunk_index", 0))
        
        # Convert to PageChunk objects
        page_chunks = []
        for c in raw_chunks:
            # Handle both old (Surya) and new (Docling) formats
            p_num = c.get("page", 1)
            if "page_number" in c: p_num = c["page_number"]
            
            # Create PageChunk
            pc = PageChunk(
                chunk_id=c.get("chunk_id") or c.get("id") or str(uuid.uuid4()),
                text=c.get("text", ""),
                page_number=p_num,
                chunk_index=c.get("chunk_index", 0),
                page_context=c.get("page_context") or c.get("text_with_context") or "",
                previous_chunk_id=c.get("previous_chunk_id"),
                next_chunk_id=c.get("next_chunk_id"),
                file_id=doc_id
            )
            page_chunks.append(pc)
            
        # Create DocumentMetadata
        doc_meta = DocumentMetadata(
            file_id=doc_id,
            filename=filename,
            total_pages=max([c.page_number for c in page_chunks]) if page_chunks else 1,
            total_chunks=len(page_chunks),
            page_chunks_map={} # Populated if needed, but service generates it internally if needed
        )
        
        # Store using enhanced service with duplicate detection enabled
        stored = service.insert_document_chunks(
            page_chunks, 
            doc_id, 
            filename, 
            doc_meta,
            check_duplicates=True,  # Enable duplicate detection
            similarity_threshold=0.95
        )
        total_stored += stored
        
    logger.info(f"Successfully stored {total_stored} chunks in Enhanced Qdrant")
