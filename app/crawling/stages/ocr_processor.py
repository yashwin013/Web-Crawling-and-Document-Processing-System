"""
OCR processor stage - performs OCR using Surya.
"""

from pathlib import Path
from typing import Optional

from app.crawling.stages.base import PipelineStage
from app.crawling.models.document import (
    Document, Page, PageContent, OCRAction, ContentSource
)
from app.services.gpu_manager import get_surya_predictors

from app.config import get_logger, OCR_MAX_BBOXES_PER_PAGE

logger = get_logger(__name__)


class OCRProcessorStage(PipelineStage):
    """
    OCR processor stage.
    
    Performs OCR on pages that require it based on the
    decision from OCRDecisionStage.
    
    Uses Surya OCR for high-quality text extraction.
    """
    
    def __init__(self, config=None):
        super().__init__(config)
        # Models managed by gpu_manager
    
    @property
    def name(self) -> str:
        return "ocr_processor"
    
    async def process(self, document: Document) -> Document:
        """
        Perform OCR on pages that need it.
        
        Args:
            document: Document with OCR decisions set
            
        Returns:
            Document with OCR text added to pages
        """
        pages_to_ocr = [
            p for p in document.pages
            if p.ocr_action in (OCRAction.FULL_PAGE_OCR, OCRAction.OCR_IMAGES_ONLY)
        ]
        
        if not pages_to_ocr:
            logger.info("No pages require OCR")
            return document
        
        logger.info(f"Running OCR on {len(pages_to_ocr)} pages")
        
        for page in pages_to_ocr:
            try:
                await self._process_page(page)
            except Exception as e:
                logger.error(f"OCR failed for {page.url}: {e}")
        
        return document
    
    async def _process_page(self, page: Page) -> None:
        """Process a single page with OCR."""
        if not page.pdf_path or not page.pdf_path.exists():
            logger.warning(f"No PDF for OCR: {page.url}")
            return
        
        if page.ocr_action == OCRAction.FULL_PAGE_OCR:
            text = await self._run_full_page_ocr(page.pdf_path)
        else:
            text = await self._run_images_only_ocr(page.pdf_path)
        
        if text:
            # Update or merge with existing content
            if page.content and page.content.text:
                # Merge DOM text with OCR text
                combined = f"{page.content.text}\n\n{text}"
                page.content = PageContent.from_text(combined, ContentSource.HYBRID)
            else:
                page.content = PageContent.from_text(text, ContentSource.OCR)
    
    async def _run_full_page_ocr(self, pdf_path: Path) -> str:
        """Run full-page OCR on PDF using Surya v0.17 API."""
        try:
            import fitz  # PyMuPDF
            from PIL import Image
            import io
            
            # Use centralized GPU manager
            det_predictor, rec_predictor = get_surya_predictors()
            
            # Extract images from PDF
            doc = fitz.open(str(pdf_path))
            images = []
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                # Lower DPI from 150 to 96 for speed/noise reduction
                pix = page.get_pixmap(dpi=96)
                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))
                images.append(img)
            
            doc.close()
            
            # Run OCR
            # Run OCR with optimized parameters
            logger.info(f"Running OCR on {len(images)} images (DPI=96)...")
            
            # Preprocess images (simple binarization to reduce noise)
            # This helps prevent hallucination of thousands of text lines
            processed_images = []
            for img in images:
                # Convert to grayscale
                img = img.convert('L') 
                # Binarize (threshold) - stricter threshold 200 to kill more gray noise
                img = img.point(lambda p: 255 if p > 200 else 0)
                processed_images.append(img.convert('RGB'))

            # Run Detection first to check for complexity/hallucination
            logger.info("Running detection pass...")
            det_predictions = det_predictor(processed_images)

            # Log per-page bbox counts and total boxes
            per_page_counts = [len(pred.bboxes) for pred in det_predictions]
            total_boxes = sum(per_page_counts)
            logger.info(f"Detected bboxes per page: {per_page_counts}")
            logger.info(f"Total detected text regions across pages: {total_boxes}")

            final_images_to_recognize = []
            final_indices = []

            all_text_placeholders = {}  # Map index to skipped message

            skipped_pages = {}
            for i, pred in enumerate(det_predictions):
                # If too many bboxes, it's likely noise/tables/chart -> Skip recognition for that page
                if len(pred.bboxes) > OCR_MAX_BBOXES_PER_PAGE:
                    skipped_pages[i] = len(pred.bboxes)
                    logger.warning(
                        f"Page {i} has {len(pred.bboxes)} text regions (Limit: {OCR_MAX_BBOXES_PER_PAGE}). Skipping recognition to prevent hanging."
                    )
                    all_text_placeholders[i] = f"[SKIPPED_COMPLEX_PAGE: {len(pred.bboxes)} regions detected]"
                else:
                    final_images_to_recognize.append(processed_images[i])
                    final_indices.append(i)

            if skipped_pages:
                logger.info(f"Skipped pages due to high bbox counts: {skipped_pages}")
            
            if not final_images_to_recognize:
                # All pages skipped
                sorted_text = [all_text_placeholders.get(i, "") for i in range(len(images))]
                return "\\n\\n".join(sorted_text)

            # Prepare polygons for recognition from detection predictions
            # Convert PolygonBox objects to the correct format for recognition
            polygons_to_recognize = []
            for idx in final_indices:
                page_polygons = []
                for bbox in det_predictions[idx].bboxes:
                    # Extract polygon coordinates from PolygonBox
                    # PolygonBox has a .polygon attribute or .bbox attribute
                    if hasattr(bbox, 'polygon'):
                        page_polygons.append(bbox.polygon)
                    elif hasattr(bbox, 'bbox'):
                        # bbox is [x1, y1, x2, y2]
                        x1, y1, x2, y2 = bbox.bbox
                        # Convert to polygon format [[x1,y1], [x2,y1], [x2,y2], [x1,y2]]
                        page_polygons.append([[x1, y1], [x2, y1], [x2, y2], [x1, y2]])
                    else:
                        # Try to convert PolygonBox directly
                        try:
                            # If it's already a list of points, use it
                            if isinstance(bbox, list):
                                page_polygons.append(bbox)
                            else:
                                # Extract coordinates from the object
                                page_polygons.append(bbox.tolist() if hasattr(bbox, 'tolist') else list(bbox))
                        except:
                            logger.warning(f"Could not convert bbox to polygon format: {type(bbox)}")
                            continue
                polygons_to_recognize.append(page_polygons)
            
            # Run Recognition ONLY on safe pages with their detected polygons
            rec_predictions = rec_predictor(
                final_images_to_recognize,
                polygons=polygons_to_recognize
            )
            # Log recognition sizes: lines per page and total recognized lines
            try:
                rec_lines_counts = [len(p.text_lines) for p in rec_predictions]
                total_rec_lines = sum(rec_lines_counts)
                logger.info(f"Recognition produced text lines per page: {rec_lines_counts}")
                logger.info(f"Total recognition text lines across pages: {total_rec_lines}")
                
                # Skip if total recognized lines exceed 500 (likely noise/hallucination)
                if total_rec_lines > 500:
                    logger.warning(
                        f"Total recognized text lines ({total_rec_lines}) exceeds limit (500). "
                        f"Skipping recognition results to prevent hallucination."
                    )
                    # Mark all recognized pages as skipped
                    for orig_idx in final_indices:
                        all_text_placeholders[orig_idx] = f"[SKIPPED_EXCESSIVE_RECOGNITION: {total_rec_lines} lines detected]"
                    sorted_text = [all_text_placeholders.get(i, "") for i in range(len(images))]
                    return "\\n\\n".join(sorted_text)
            except Exception:
                logger.debug("Could not compute recognition line counts; unexpected rec_predictions format.")
            
            # Merge results back
            text_by_page = all_text_placeholders.copy()
                
            for i, page_pred in enumerate(rec_predictions):
                page_text = "\\n".join([line.text for line in page_pred.text_lines])
                original_idx = final_indices[i]
                text_by_page[original_idx] = page_text
                
            # Flatten to list in order
            sorted_text = [text_by_page.get(i, "") for i in range(len(images))]
            return "\\n\\n".join(sorted_text)
            

            
        except ImportError as e:
            logger.error(f"Surya not available: {e}")
            return await self._fallback_ocr(pdf_path)
        except Exception as e:
            logger.error(f"Surya OCR failed: {e}")
            return ""
    
    async def _run_images_only_ocr(self, pdf_path: Path) -> str:
        """Run OCR on embedded images only."""
        # For now, same as full page
        # Future: extract only image regions
        return await self._run_full_page_ocr(pdf_path)
    
    async def _fallback_ocr(self, pdf_path: Path) -> str:
        """Fallback OCR using PyMuPDF text extraction."""
        try:
            import fitz
            
            doc = fitz.open(str(pdf_path))
            text_parts = []
            
            for page in doc:
                text = page.get_text()
                if text.strip():
                    text_parts.append(text)
            
            doc.close()
            return "\n\n".join(text_parts)
            
        except Exception as e:
            logger.error(f"Fallback OCR failed: {e}")
            return ""
