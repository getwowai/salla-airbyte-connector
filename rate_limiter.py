"""
Global Rate Limiter for Salla API

Enforces a global request rate across all operations to stay under
Cloudflare's rate limit (~1.5 req/sec sustained) and Salla's API limits.

Key features:
- Thread-safe singleton pattern ensures one limiter across all streams
- Configurable requests per second (default 1.0)
- Blocks until safe to make next request
- Tracks request count and actual rate for logging
"""

import threading
import time
from typing import Optional


class GlobalRateLimiter:
    """
    Thread-safe rate limiter that enforces a maximum request rate globally.
    
    Uses a simple token bucket approach: tracks time of last request and
    ensures minimum interval between requests.
    """
    
    _instance: Optional['GlobalRateLimiter'] = None
    _lock = threading.Lock()
    
    def __new__(cls, requests_per_second: float = 1.0):
        """Singleton pattern - only one rate limiter instance exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance
    
    def __init__(self, requests_per_second: float = 1.0):
        """
        Initialize the rate limiter.
        
        Args:
            requests_per_second: Maximum requests per second (default 1.0)
        """
        # Only initialize once (singleton)
        if self._initialized:
            return
            
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time: float = 0
        self._request_lock = threading.Lock()
        self._initialized = True
        
        # Stats tracking
        self._request_count = 0
        self._start_time: Optional[float] = None
        self._recent_intervals: list = []  # Last 10 intervals for rolling average
    
    def wait(self) -> float:
        """
        Wait until it's safe to make the next request.
        
        Returns:
            float: The actual wait time in seconds (0 if no wait needed)
        """
        with self._request_lock:
            current_time = time.time()
            
            # Track first request time
            if self._start_time is None:
                self._start_time = current_time
            
            elapsed = current_time - self.last_request_time
            
            wait_time = 0.0
            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                time.sleep(wait_time)
            
            # Update stats
            if self.last_request_time > 0:
                actual_interval = time.time() - self.last_request_time
                self._recent_intervals.append(actual_interval)
                # Keep only last 10 intervals
                if len(self._recent_intervals) > 10:
                    self._recent_intervals.pop(0)
            
            self.last_request_time = time.time()
            self._request_count += 1
            
            return wait_time
    
    def wait_for_backoff(self, seconds: float) -> None:
        """
        Wait for a specific backoff period (e.g., after 429 response).
        
        Args:
            seconds: Number of seconds to wait
        """
        with self._request_lock:
            time.sleep(seconds)
            self.last_request_time = time.time()
    
    def get_stats(self) -> dict:
        """
        Get current rate limiting stats.
        
        Returns:
            dict: Statistics including request count, elapsed time, and rates
        """
        with self._request_lock:
            if self._start_time is None:
                return {
                    "request_count": 0,
                    "elapsed_seconds": 0,
                    "overall_rate": 0,
                    "recent_rate": 0,
                    "target_rate": self.requests_per_second,
                }
            
            elapsed = time.time() - self._start_time
            overall_rate = self._request_count / elapsed if elapsed > 0 else 0
            
            # Calculate recent rate from last 10 intervals
            recent_rate = 0
            if self._recent_intervals:
                avg_interval = sum(self._recent_intervals) / len(self._recent_intervals)
                recent_rate = 1.0 / avg_interval if avg_interval > 0 else 0
            
            return {
                "request_count": self._request_count,
                "elapsed_seconds": round(elapsed, 1),
                "overall_rate": round(overall_rate, 3),
                "recent_rate": round(recent_rate, 3),
                "target_rate": self.requests_per_second,
            }
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        with cls._lock:
            cls._instance = None


def get_rate_limiter(requests_per_second: float = 1.0) -> GlobalRateLimiter:
    """
    Get the global rate limiter instance.
    
    Args:
        requests_per_second: Maximum requests per second (only used on first call)
    
    Returns:
        GlobalRateLimiter: The singleton rate limiter instance
    """
    return GlobalRateLimiter(requests_per_second)
