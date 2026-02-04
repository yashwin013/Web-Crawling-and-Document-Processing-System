"""
Application lifecycle management.

Provides startup and shutdown hooks for proper resource management.
Ensures all connections, executors, and resources are properly initialized and cleaned up.
"""

import logging
import asyncio
from typing import Optional

from app.core.database import db_manager, startup_database, shutdown_database
from app.core.executor import executor_manager, shutdown_executor

logger = logging.getLogger("app_lifecycle")


class ApplicationLifecycle:
    """
    Manages application lifecycle for all resources.
    
    Use this to ensure proper startup/shutdown sequence:
    - MongoDB connections
    - ThreadPool executors
    - GPU resources (if applicable)
    """
    
    _initialized = False
    _shutdown_called = False
    
    @classmethod
    async def startup(cls):
        """
        Initialize all application resources.
        Call this once at application startup.
        """
        if cls._initialized:
            logger.warning("Application already initialized")
            return
        
        try:
            logger.info("=" * 60)
            logger.info("Starting application initialization...")
            logger.info("=" * 60)
            
            # 1. Initialize MongoDB connections
            logger.info("1/2 Initializing database connections...")
            await startup_database()
            
            # 2. Pre-create thread pool executor (optional)
            logger.info("2/2 Initializing thread pool executor...")
            await executor_manager.acquire()
            
            cls._initialized = True
            logger.info("=" * 60)
            logger.info("✓ Application initialized successfully")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ Application startup failed: {e}", exc_info=True)
            # Attempt cleanup on failed startup
            await cls.shutdown()
            raise
    
    @classmethod
    async def shutdown(cls):
        """
        Cleanup all application resources.
        Call this once at application shutdown.
        """
        if cls._shutdown_called:
            logger.warning("Shutdown already called")
            return
        
        cls._shutdown_called = True
        
        try:
            logger.info("=" * 60)
            logger.info("Starting application shutdown...")
            logger.info("=" * 60)
            
            # 1. Shutdown thread pool executor
            logger.info("1/2 Shutting down thread pool executor...")
            await shutdown_executor()
            
            # 2. Close MongoDB connections
            logger.info("2/2 Closing database connections...")
            await shutdown_database()
            
            logger.info("=" * 60)
            logger.info("✓ Application shutdown complete")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error during application shutdown: {e}", exc_info=True)
    
    @classmethod
    def is_initialized(cls) -> bool:
        """Check if application is initialized."""
        return cls._initialized
    
    @classmethod
    async def health_check(cls) -> dict:
        """
        Check health status of all resources.
        
        Returns:
            Dict with health status of each component
        """
        health = {
            "database": {
                "connected": db_manager.is_connected,
                "status": "healthy" if db_manager.is_connected else "disconnected"
            },
            "executor": {
                "active": executor_manager.is_active,
                "references": executor_manager.reference_count,
                "status": "healthy" if executor_manager.is_active else "inactive"
            },
            "application": {
                "initialized": cls._initialized,
                "status": "healthy" if cls._initialized else "not_initialized"
            }
        }
        
        # Overall health check
        all_healthy = (
            db_manager.is_connected and
            executor_manager.is_active and
            cls._initialized
        )
        health["overall"] = "healthy" if all_healthy else "degraded"
        
        return health


# Convenience functions
async def startup_app():
    """Convenience function for application startup."""
    await ApplicationLifecycle.startup()


async def shutdown_app():
    """Convenience function for application shutdown."""
    await ApplicationLifecycle.shutdown()


async def health_check():
    """Convenience function for health check."""
    return await ApplicationLifecycle.health_check()


# Context manager for clean startup/shutdown
class ApplicationContext:
    """
    Context manager for application lifecycle.
    
    Usage:
        async with ApplicationContext():
            # Your application code here
            pass
    """
    
    async def __aenter__(self):
        await ApplicationLifecycle.startup()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await ApplicationLifecycle.shutdown()
        return False  # Don't suppress exceptions
