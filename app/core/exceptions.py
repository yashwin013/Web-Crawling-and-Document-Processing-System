"""
Custom exceptions for the application.

Usage:
    from app.core.exceptions import ProcessingError
    raise ProcessingError("Document processing failed", details={"file": path})
"""

from typing import Any, Dict, Optional


class AppException(Exception):
    """Base exception for all application errors."""
    
    def __init__(
        self, 
        message: str, 
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.cause = cause
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(AppException):
    """Raised when configuration is invalid or missing."""
    pass


class ProcessingError(AppException):
    """Raised when document processing fails."""
    pass


class VectorDBError(AppException):
    """Raised when vector database operations fail."""
    pass


class CrawlerError(AppException):
    """Raised when web crawling fails."""
    pass


class OCRError(ProcessingError):
    """Raised when OCR processing fails."""
    pass


class ChunkingError(ProcessingError):
    """Raised when document chunking fails."""
    pass
