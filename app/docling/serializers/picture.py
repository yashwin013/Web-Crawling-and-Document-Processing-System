#proper class implementation 


import asyncio
import uuid
from docling_core.transforms.serializer.base import (
    BaseDocSerializer,
    SerializationResult,
)
from docling_core.transforms.serializer.markdown import MarkdownPictureSerializer
from docling_core.types.doc.labels import DocItemLabel
from docling_core.types.doc.document import (
    DoclingDocument,
    ImageRefMode,
    PictureDescriptionData,
    PictureItem,
)
from docling_core.transforms.serializer.common import create_ser_result
from typing import Iterable, Optional, Any, Dict
from typing_extensions import override
from pathlib import Path
import re
from concurrent.futures import ThreadPoolExecutor
from app.config import app_config

class FilePictureSerializer_new(MarkdownPictureSerializer):
    """Custom async picture serializer that saves images to files with dynamic placeholders"""
    
    def __init__(
        self, 
        output_dir: Path = Path("exported_images"),         
        executor: Optional[ThreadPoolExecutor] = None
    ):
        super().__init__()
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.image_counter = 0
        
        self.image_map: Dict[str, Dict[str, Any]] = {}
        # Track if we own the executor to know if we should shut it down
        self._owns_executor = executor is None
        self.executor = executor or ThreadPoolExecutor(max_workers=4)
        self._executor_shutdown = False
        self._lock = asyncio.Lock()
    
    @classmethod
    async def from_config(cls,executor: Optional[ThreadPoolExecutor] = None):
        """Initialize Enhanced DocumentProcessor with all required dependencies"""
        output_dir=app_config.exported_images
        return cls(output_dir=Path(output_dir), executor=executor)
     


    async def _save_image_async(self, pil_img, img_path: Path) -> None:
        """Save PIL image asynchronously"""
        if self._executor_shutdown:
            raise RuntimeError("Executor has been shut down")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self.executor,
            lambda: pil_img.save(img_path, format='PNG')
        )
    
    async def _extract_image_async(self, item: PictureItem, doc: DoclingDocument) -> Optional[Any]:
        """Extract PIL image from item asynchronously"""
        if self._executor_shutdown:
            raise RuntimeError("Executor has been shut down")
        pil_img = None
        
        # Method 1: Try to get image from doc.pictures (most reliable)
        if hasattr(doc, 'pictures') and doc.pictures:
            if hasattr(item, 'self_ref') and item.self_ref:
                for pic_data in doc.pictures:
                    pic_ref = getattr(pic_data, 'self_ref', None) or getattr(pic_data, 'name', None)
                    if pic_ref == item.self_ref:
                        if hasattr(pic_data, 'pil_image') and pic_data.pil_image:
                            pil_img = pic_data.pil_image
                            break
                        elif hasattr(pic_data, 'get_image'):
                            # Run get_image in executor if it's blocking
                            loop = asyncio.get_event_loop()
                            pil_img = await loop.run_in_executor(
                                self.executor,
                                lambda: pic_data.get_image(doc)
                            )
                            break
                        elif hasattr(pic_data, 'image'):
                            image_ref = pic_data.image
                            if hasattr(image_ref, 'pil_image') and image_ref.pil_image:
                                pil_img = image_ref.pil_image
                                break
                            elif hasattr(image_ref, 'get_image'):
                                loop = asyncio.get_event_loop()
                                pil_img = await loop.run_in_executor(
                                    self.executor,
                                    lambda: image_ref.get_image(doc)
                                )
                                break
        
        # Method 2: Try from item.image directly (fallback)
        if pil_img is None and hasattr(item, 'image') and item.image:
            image_obj = item.image
            if hasattr(image_obj, 'pil_image') and image_obj.pil_image:
                pil_img = image_obj.pil_image
            elif hasattr(image_obj, 'get_image'):
                loop = asyncio.get_event_loop()
                pil_img = await loop.run_in_executor(
                    self.executor,
                    lambda: image_obj.get_image(doc)
                )
            elif hasattr(image_obj, 'size') and hasattr(image_obj, 'save'):
                pil_img = image_obj
        
        return pil_img
    
    @override
    async def serialize_async(
        self,
        *,
        item: PictureItem,
        doc_serializer: BaseDocSerializer,
        doc: DoclingDocument,
        **kwargs: Any,
    ) -> SerializationResult:
        """Async serialize picture item to markdown with dynamic file reference placeholder"""
        try:
            # Generate unique ID
            _idval = str(uuid.uuid4()).replace('-', '')
            first5 = _idval[:5]
            middle5 = _idval[25:30]
            raw_id = first5 + middle5
            
            print(f"Processing image with UUID: {raw_id}")
            
            # Extract image asynchronously
            pil_img = await self._extract_image_async(item, doc)
            
            if pil_img is not None:
                # Convert to RGB if needed (run in executor)
                if pil_img.mode not in ('RGB', 'L', 'RGBA'):
                    if self._executor_shutdown:
                        # Try direct conversion if executor is shutdown
                        pil_img = pil_img.convert('RGB')
                    else:
                        loop = asyncio.get_event_loop()
                        pil_img = await loop.run_in_executor(
                            self.executor,
                            lambda: pil_img.convert('RGB')
                        )
                
                # Generate unique filename
                img_filename = f"image_{raw_id}.png"
                img_path = self.output_dir / img_filename
                
                # Save image asynchronously
                await self._save_image_async(pil_img, img_path)
                
                # Get caption asynchronously
                caption = await self._get_caption_async(item, doc)
                print(f"Caption: {caption}")
                
                # Store in mapping (thread-safe)
                async with self._lock:
                    if hasattr(item, 'self_ref'):
                        self.image_map[item.self_ref] = {
                            'path': str(img_path),
                            'caption': caption,
                            'id': raw_id
                        }
                    self.image_counter += 1
                
                # Create dynamic placeholder
                placeholder = f"<!-- IMAGE_ID:{raw_id}|PATH:{img_path}|CAPTION:{caption} -->"
                
                print(f"✓ Saved image {raw_id}: {img_path} - {caption[:50]}...")
                
                text_res = placeholder
                text_res = doc_serializer.post_process(text=text_res)
                return create_ser_result(text=text_res, span_source=item)
            
        except RuntimeError as e:
            # Handle executor shutdown errors specifically
            if "shutdown" in str(e).lower():
                print(f"✗ Executor shutdown during image processing: {e}")
            else:
                print(f"✗ Runtime error during image processing: {e}")
                import traceback
                traceback.print_exc()
        except Exception as e:
            print(f"✗ Failed to save image: {e}")
            import traceback
            traceback.print_exc()
        
        # Fallback
        return create_ser_result(text="<!-- IMAGE_SAVE_FAILED -->\n\n")
    
    @override
    def serialize(
        self,
        *,
        item: PictureItem,
        doc_serializer: BaseDocSerializer,
        doc: DoclingDocument,
        **kwargs: Any,
    ) -> SerializationResult:
        """Synchronous wrapper - processes images synchronously to avoid executor issues"""
        try:
            # Generate unique ID
            _idval = str(uuid.uuid4()).replace('-', '')
            first5 = _idval[:5]
            middle5 = _idval[25:30]
            raw_id = first5 + middle5
            
            print(f"Processing image with UUID: {raw_id}")
            
            # Extract image synchronously to avoid executor issues in sync context
            pil_img = self._extract_image_sync(item, doc)
            
            if pil_img is not None:
                # Convert to RGB if needed
                if pil_img.mode not in ('RGB', 'L', 'RGBA'):
                    pil_img = pil_img.convert('RGB')
                
                # Generate unique filename
                img_filename = f"image_{raw_id}.png"
                img_path = self.output_dir / img_filename
                
                # Save image synchronously
                pil_img.save(img_path, format='PNG')
                
                # Get caption synchronously
                caption = self._get_caption_sync(item, doc)
                
                # Store in mapping
                if hasattr(item, 'self_ref'):
                    self.image_map[item.self_ref] = {
                        'path': str(img_path),
                        'caption': caption,
                        'id': raw_id
                    }
                self.image_counter += 1
                
                # Create dynamic placeholder
                placeholder = f"<!-- IMAGE_ID:{raw_id}|PATH:{img_path}|CAPTION:{caption} -->"
                
                print(f"✓ Saved image {raw_id}: {img_path} - {caption[:50]}...")
                
                text_res = placeholder
                text_res = doc_serializer.post_process(text=text_res)
                return create_ser_result(text=text_res, span_source=item)
                
        except Exception as e:
            print(f"✗ Failed to save image: {e}")
            import traceback
            traceback.print_exc()
        
        # Fallback
        return create_ser_result(text="<!-- IMAGE_SAVE_FAILED -->\n\n")
    
    async def _get_caption_async(self, item: PictureItem, doc: DoclingDocument) -> str:
        """Extract complete caption from picture item asynchronously"""
        
        # Method 1: Try to export item to markdown and extract caption from it
        try:
            if hasattr(item, 'export_to_markdown'):
                if self._executor_shutdown:
                    # Skip executor-based operations if shutdown
                    return f"Figure {self.image_counter + 1}"
                loop = asyncio.get_event_loop()
                exported_md = await loop.run_in_executor(
                    self.executor,
                    lambda: item.export_to_markdown(doc)
                )
                
                if exported_md:
                    parts = exported_md.split('![Image](')
                    
                    if len(parts) > 1:
                        caption_candidate = parts[0].strip()
                        
                        if caption_candidate and len(caption_candidate) > 2:
                            caption_candidate = caption_candidate.replace('\n', ' ')
                            caption_candidate = ' '.join(caption_candidate.split())
                            
                            lower_caption = caption_candidate.lower()
                            if any(keyword in lower_caption for keyword in 
                                ['figure', 'fig.', 'table', 'image', 'diagram', 'chart']):
                                return caption_candidate
                            
                            if len(caption_candidate) > 10:
                                return caption_candidate
                    
                    if len(parts) > 1 and len(parts[1]) > 2:
                        after_image = parts[1]
                        if ')' in after_image:
                            caption_after = after_image.split(')', 1)[1].strip()
                            if caption_after and len(caption_after) > 10:
                                return caption_after
        
        except Exception as e:
            print(f"Could not parse markdown export: {e}")
        
        # Method 2: Check item's direct caption attribute
        if hasattr(item, 'caption') and item.caption:
            if hasattr(item.caption, 'text') and item.caption.text:
                caption_text = item.caption.text.strip()
                if caption_text and not caption_text.startswith('#/'):
                    return caption_text
            elif isinstance(item.caption, str) and not item.caption.startswith('#/'):
                return item.caption.strip()
        
        # Method 3: Search in document body for nearby CAPTION elements
        if hasattr(item, 'self_ref') and hasattr(doc, 'body'):
            item_index = None
            for idx, doc_item in enumerate(doc.body):
                if hasattr(doc_item, 'self_ref') and doc_item.self_ref == item.self_ref:
                    item_index = idx
                    break
            
            if item_index is not None:
                search_range = 5
                caption_parts = []
                
                for offset in range(-search_range, search_range + 1):
                    check_idx = item_index + offset
                    if 0 <= check_idx < len(doc.body):
                        nearby_item = doc.body[check_idx]
                        
                        if hasattr(nearby_item, 'label') and nearby_item.label == DocItemLabel.CAPTION:
                            caption_text = await self._extract_full_text_async(nearby_item)
                            if caption_text:
                                caption_parts.append(caption_text)
                
                if caption_parts:
                    full_caption = " ".join(caption_parts).strip()
                    return full_caption
        
        # Method 4: Check picture metadata
        if hasattr(doc, 'pictures') and doc.pictures:
            if hasattr(item, 'self_ref'):
                for pic_data in doc.pictures:
                    pic_ref = getattr(pic_data, 'self_ref', None)
                    if pic_ref == item.self_ref:
                        if hasattr(pic_data, 'description') and pic_data.description:
                            return pic_data.description.strip()
                        break
        
        # Fallback
        return f"Figure {self.image_counter + 1}"
    
    async def _extract_full_text_async(self, item) -> str:
        """Extract all text content from an item recursively"""
        text_parts = []
        
        if hasattr(item, 'text') and item.text:
            text_parts.append(item.text.strip())
        
        if hasattr(item, 'children') and item.children:
            # Process children concurrently
            tasks = [self._extract_full_text_async(child) for child in item.children]
            child_texts = await asyncio.gather(*tasks)
            text_parts.extend([text for text in child_texts if text])
        
        if hasattr(item, 'caption'):
            if hasattr(item.caption, 'text') and item.caption.text:
                cap_text = item.caption.text.strip()
                if cap_text and not cap_text.startswith('#/'):
                    text_parts.append(cap_text)
            elif isinstance(item.caption, str) and not item.caption.startswith('#/'):
                text_parts.append(item.caption.strip())
        
        full_text = " ".join(text_parts).strip()
        full_text = re.sub(r'\s+', ' ', full_text)
        
        return full_text
    
    def _extract_full_text(self, item) -> str:
        """Synchronous wrapper for _extract_full_text_async"""
        return asyncio.run(self._extract_full_text_async(item))
    
    def _extract_image_sync(self, item: PictureItem, doc: DoclingDocument) -> Optional[Any]:
        """Extract PIL image synchronously (for use in sync serialize method)"""
        pil_img = None
        
        # Method 1: Try to get image from doc.pictures (most reliable)
        if hasattr(doc, 'pictures') and doc.pictures:
            if hasattr(item, 'self_ref') and item.self_ref:
                for pic_data in doc.pictures:
                    pic_ref = getattr(pic_data, 'self_ref', None) or getattr(pic_data, 'name', None)
                    if pic_ref == item.self_ref:
                        if hasattr(pic_data, 'pil_image') and pic_data.pil_image:
                            pil_img = pic_data.pil_image
                            break
                        elif hasattr(pic_data, 'get_image'):
                            pil_img = pic_data.get_image(doc)
                            break
                        elif hasattr(pic_data, 'image'):
                            image_ref = pic_data.image
                            if hasattr(image_ref, 'pil_image') and image_ref.pil_image:
                                pil_img = image_ref.pil_image
                                break
                            elif hasattr(image_ref, 'get_image'):
                                pil_img = image_ref.get_image(doc)
                                break
        
        # Method 2: Try from item.image directly (fallback)
        if pil_img is None and hasattr(item, 'image') and item.image:
            image_obj = item.image
            if hasattr(image_obj, 'pil_image') and image_obj.pil_image:
                pil_img = image_obj.pil_image
            elif hasattr(image_obj, 'get_image'):
                pil_img = image_obj.get_image(doc)
            elif hasattr(image_obj, 'size') and hasattr(image_obj, 'save'):
                pil_img = image_obj
        
        return pil_img
    
    def _get_caption_sync(self, item: PictureItem, doc: DoclingDocument) -> str:
        """Extract caption synchronously (for use in sync serialize method)"""
        try:
            if hasattr(item, 'export_to_markdown'):
                exported_md = item.export_to_markdown(doc)
                
                if exported_md:
                    parts = exported_md.split('![Image](')
                    
                    if len(parts) > 1:
                        caption_candidate = parts[0].strip()
                        
                        if caption_candidate and len(caption_candidate) > 2:
                            caption_candidate = caption_candidate.replace('\n', ' ')
                            caption_candidate = ' '.join(caption_candidate.split())
                            
                            lower_caption = caption_candidate.lower()
                            if any(keyword in lower_caption for keyword in 
                                ['figure', 'fig.', 'table', 'image', 'diagram', 'chart']):
                                return caption_candidate
                            
                            if len(caption_candidate) > 10:
                                return caption_candidate
                    
                    if len(parts) > 1 and len(parts[1]) > 2:
                        after_image = parts[1]
                        if ')' in after_image:
                            caption_after = after_image.split(')', 1)[1].strip()
                            if caption_after and len(caption_after) > 10:
                                return caption_after
        
        except Exception as e:
            print(f"Could not parse markdown export: {e}")
        
        # Method 2: Check item's direct caption attribute
        if hasattr(item, 'caption') and item.caption:
            if hasattr(item.caption, 'text') and item.caption.text:
                caption_text = item.caption.text.strip()
                if caption_text and not caption_text.startswith('#/'):
                    return caption_text
            elif isinstance(item.caption, str) and not item.caption.startswith('#/'):
                return item.caption.strip()
        
        # Method 3: Search in document body for nearby CAPTION elements
        if hasattr(item, 'self_ref') and hasattr(doc, 'body'):
            item_index = None
            for idx, doc_item in enumerate(doc.body):
                if hasattr(doc_item, 'self_ref') and doc_item.self_ref == item.self_ref:
                    item_index = idx
                    break
            
            if item_index is not None:
                search_range = 5
                caption_parts = []
                
                for offset in range(-search_range, search_range + 1):
                    check_idx = item_index + offset
                    if 0 <= check_idx < len(doc.body):
                        nearby_item = doc.body[check_idx]
                        
                        if hasattr(nearby_item, 'label') and nearby_item.label == DocItemLabel.CAPTION:
                            caption_text = self._extract_full_text_sync(nearby_item)
                            if caption_text:
                                caption_parts.append(caption_text)
                
                if caption_parts:
                    full_caption = " ".join(caption_parts).strip()
                    return full_caption
        
        # Method 4: Check picture metadata
        if hasattr(doc, 'pictures') and doc.pictures:
            if hasattr(item, 'self_ref'):
                for pic_data in doc.pictures:
                    pic_ref = getattr(pic_data, 'self_ref', None)
                    if pic_ref == item.self_ref:
                        if hasattr(pic_data, 'description') and pic_data.description:
                            return pic_data.description.strip()
                        break
        
        # Fallback
        return f"Figure {self.image_counter + 1}"
    
    def _extract_full_text_sync(self, item) -> str:
        """Extract all text content from an item recursively (sync version)"""
        text_parts = []
        
        if hasattr(item, 'text') and item.text:
            text_parts.append(item.text.strip())
        
        if hasattr(item, 'children') and item.children:
            for child in item.children:
                child_text = self._extract_full_text_sync(child)
                if child_text:
                    text_parts.append(child_text)
        
        if hasattr(item, 'caption'):
            if hasattr(item.caption, 'text') and item.caption.text:
                cap_text = item.caption.text.strip()
                if cap_text and not cap_text.startswith('#/'):
                    text_parts.append(cap_text)
            elif isinstance(item.caption, str) and not item.caption.startswith('#/'):
                text_parts.append(item.caption.strip())
        
        full_text = " ".join(text_parts).strip()
        full_text = re.sub(r'\s+', ' ', full_text)
        
        return full_text
    
    async def close(self):
        """Cleanup method to shutdown executor"""
        # Only shutdown if we own the executor and haven't already shut it down
        if self._owns_executor and not self._executor_shutdown:
            try:
                self.executor.shutdown(wait=True)
                self._executor_shutdown = True
            except Exception as e:
                print(f"Warning: Error during executor shutdown: {e}")
