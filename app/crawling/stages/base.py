"""
Abstract base class for pipeline stages.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
import time

if TYPE_CHECKING:
    from app.crawling.models.document import Document
    from app.crawling.models.config import PipelineConfig


class PipelineStage(ABC):
    """
    Abstract base class for all pipeline stages.
    
    Each stage processes a Document and returns the modified Document.
    Stages should be stateless - all state is carried in the Document.
    
    Usage:
        class MyStage(PipelineStage):
            @property
            def name(self) -> str:
                return "my_stage"
            
            async def process(self, document: Document) -> Document:
                # Process document
                return document
    """
    
    def __init__(self, config: "PipelineConfig" = None):
        """Initialize stage with optional pipeline config."""
        self._config = config
        self._logger = None
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        Stage name for logging and identification.
        
        Returns:
            Human-readable stage name (e.g., "crawler", "text_extractor")
        """
        pass
    
    @abstractmethod
    async def process(self, document: "Document") -> "Document":
        """
        Process the document.
        
        This is the main entry point for the stage. Implementations should:
        1. Read data from the document
        2. Perform processing
        3. Update document with results
        4. Return the modified document
        
        Args:
            document: Document to process
            
        Returns:
            Modified document with stage results
        """
        pass
    
    @property
    def config(self) -> "PipelineConfig":
        """Get pipeline config."""
        return self._config
    
    @config.setter
    def config(self, value: "PipelineConfig"):
        """Set pipeline config."""
        self._config = value
    
    @property
    def logger(self):
        """Get logger for this stage."""
        if self._logger is None:
            from app.config import get_logger
            self._logger = get_logger(f"pipeline.{self.name}")
        return self._logger
    
    async def setup(self) -> None:
        """
        Optional setup hook called before processing.
        
        Override this to perform one-time initialization.
        """
        pass
    
    async def teardown(self) -> None:
        """
        Optional teardown hook called after processing.
        
        Override this to clean up resources.
        """
        pass
    
    async def run(self, document: "Document") -> "Document":
        """
        Execute the stage with timing and logging.
        
        This wraps process() with:
        - Logging of stage start/end
        - Timing measurement
        - Error handling
        
        Args:
            document: Document to process
            
        Returns:
            Processed document
        """
        self.logger.info(f"Starting stage: {self.name}")
        start_time = time.time()
        
        try:
            await self.setup()
            result = await self.process(document)
            await self.teardown()
            
            elapsed = (time.time() - start_time) * 1000
            self.logger.info(f"Completed stage: {self.name} ({elapsed:.1f}ms)")
            
            return result
            
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            self.logger.error(f"Stage {self.name} failed after {elapsed:.1f}ms: {e}")
            raise
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name={self.name})>"
