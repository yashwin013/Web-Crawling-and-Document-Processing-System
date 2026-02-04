"""
Rate limiter with exponential backoff.
"""

import asyncio
import time


class RateLimiter:
    """
    Rate limiter with exponential backoff for failed requests.
    
    Features:
    - Configurable base delay between requests
    - Exponential backoff on failures
    - Maximum delay cap
    - Async-compatible
    
    Usage:
        limiter = RateLimiter(base_delay=1.0)
        
        async def fetch(url):
            await limiter.wait()
            try:
                response = await client.get(url)
                limiter.success()
                return response
            except Exception:
                limiter.failure()
                raise
    """
    
    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize rate limiter.
        
        Args:
            base_delay: Base delay between requests in seconds
            max_delay: Maximum delay (cap for backoff)
            backoff_factor: Multiplier for exponential backoff
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.current_delay = base_delay
        self.last_request_time = 0.0
        self.consecutive_failures = 0
    
    async def wait(self) -> None:
        """
        Wait before next request.
        
        Calculates required delay based on last request time
        and current backoff state.
        """
        now = time.time()
        elapsed = now - self.last_request_time
        wait_time = max(0, self.current_delay - elapsed)
        
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def wait_sync(self) -> None:
        """Synchronous version of wait()."""
        now = time.time()
        elapsed = now - self.last_request_time
        wait_time = max(0, self.current_delay - elapsed)
        
        if wait_time > 0:
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def success(self) -> None:
        """
        Call after successful request.
        
        Resets delay to base level.
        """
        self.consecutive_failures = 0
        self.current_delay = self.base_delay
    
    def failure(self) -> None:
        """
        Call after failed request.
        
        Increases delay exponentially up to max_delay.
        """
        self.consecutive_failures += 1
        self.current_delay = min(
            self.base_delay * (self.backoff_factor ** self.consecutive_failures),
            self.max_delay
        )
    
    @property
    def is_backing_off(self) -> bool:
        """Check if currently in backoff state."""
        return self.consecutive_failures > 0
    
    def reset(self) -> None:
        """Reset all state."""
        self.current_delay = self.base_delay
        self.last_request_time = 0.0
        self.consecutive_failures = 0
