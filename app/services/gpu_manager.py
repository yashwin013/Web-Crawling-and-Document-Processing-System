"""
GPU Manager for Surya OCR models.

Provides centralized management of Surya detection and recognition predictors
with GPU optimization and caching.
"""

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Global model cache
_foundation_predictor = None
_det_predictor = None
_rec_predictor = None


def get_surya_predictors() -> Tuple:
    """
    Get Surya detection and recognition predictors.
    
    Uses global singleton pattern to avoid reloading models.
    
    Returns:
        Tuple of (detection_predictor, recognition_predictor)
    """
    global _foundation_predictor, _det_predictor, _rec_predictor
    
    if _det_predictor is None or _rec_predictor is None:
        try:
            # Try the new API first (surya >= 0.7.0)
            try:
                from surya.recognition import RecognitionPredictor
                from surya.detection import DetectionPredictor
                from surya.foundation import FoundationPredictor
                
                logger.info("Loading Surya OCR models (new API)...")
                
                if _foundation_predictor is None:
                    _foundation_predictor = FoundationPredictor()
                
                # DetectionPredictor uses its own model (not foundation)
                _det_predictor = DetectionPredictor()
                
                # RecognitionPredictor uses the foundation model
                _rec_predictor = RecognitionPredictor(_foundation_predictor)
                
                logger.info("Surya OCR models loaded successfully (new API)")
                
            except (ImportError, TypeError):
                # Fall back to old API (surya < 0.7.0)
                from surya.recognition import RecognitionPredictor
                from surya.detection import DetectionPredictor
                
                logger.info("Loading Surya OCR models (legacy API)...")
                
                _det_predictor = DetectionPredictor()
                _rec_predictor = RecognitionPredictor()
                
                logger.info("Surya OCR models loaded successfully (legacy API)")
            
        except ImportError as e:
            logger.error(f"Surya OCR not available: {e}")
            raise ImportError(
                "Surya OCR is required but not installed. "
                "Install with: pip install surya-ocr"
            ) from e
    
    return _det_predictor, _rec_predictor


def clear_models() -> None:
    """Clear cached models to free GPU memory."""
    global _foundation_predictor, _det_predictor, _rec_predictor
    
    _foundation_predictor = None
    _det_predictor = None
    _rec_predictor = None
    
    import gc
    import torch
    
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    
    logger.info("Cleared Surya OCR models from memory")
