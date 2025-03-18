# core/cache.py
"""
Thread-safe caching utilities for performance optimization.
"""
import logging
import time
import threading
from typing import Dict, Any, Optional, TypeVar, Generic, Callable, Tuple, List, Union
from datetime import datetime, timedelta
from functools import wraps
import inspect

logger = logging.getLogger(__name__)

# Type variables
K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type


class ThreadSafeCache(Generic[K, V]):
    """
    Thread-safe cache with expiration.

    Generic Parameters:
        K: Cache key type
        V: Cache value type
    """

    def __init__(self, ttl_seconds: Optional[float] = None, max_size: Optional[int] = None):
        """
        Initialize the cache.

        Args:
            ttl_seconds: Time-to-live in seconds (None for no expiration)
            max_size: Maximum cache size (None for unlimited)
        """
        self._cache: Dict[K, Tuple[V, Optional[float]]] = {}  # (value, expiration_time)
        self._lock = threading.RLock()
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """
        Get a value from the cache.

        Args:
            key: Cache key
            default: Default value if key not found or expired

        Returns:
            Cached value or default
        """
        with self._lock:
            if key not in self._cache:
                return default

            value, expiration_time = self._cache[key]

            # Check if expired
            if expiration_time is not None and time.time() > expiration_time:
                del self._cache[key]
                return default

            return value

    def set(self, key: K, value: V, ttl_seconds: Optional[float] = None) -> None:
        """
        Set a value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Time-to-live in seconds (overrides default)
        """
        with self._lock:
            # Check if we need to evict items
            if self.max_size is not None and len(self._cache) >= self.max_size and key not in self._cache:
                self._evict_oldest()

            # Calculate expiration time
            expiration_time = None
            if ttl_seconds is not None:
                expiration_time = time.time() + ttl_seconds
            elif self.ttl_seconds is not None:
                expiration_time = time.time() + self.ttl_seconds

            # Store the value
            self._cache[key] = (value, expiration_time)

    def delete(self, key: K) -> bool:
        """
        Delete a key from the cache.

        Args:
            key: Cache key

        Returns:
            True if key was deleted, False if key not found
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear the cache."""
        with self._lock:
            self._cache.clear()

    def get_or_set(self, key: K, value_func: Callable[[], V], ttl_seconds: Optional[float] = None) -> V:
        """
        Get a value from the cache, or set it if not found.

        Args:
            key: Cache key
            value_func: Function to call to get the value if not in cache
            ttl_seconds: Time-to-live in seconds (overrides default)

        Returns:
            Cached or newly computed value
        """
        # Try to get from cache first
        value = self.get(key)
        if value is not None:
            return value

        # Not in cache, compute the value
        with self._lock:
            # Check again in case another thread set it while we were waiting
            value = self.get(key)
            if value is not None:
                return value

            # Compute the value
            value = value_func()

            # Store in cache
            self.set(key, value, ttl_seconds)

            return value

    def contains(self, key: K) -> bool:
        """
        Check if a key is in the cache and not expired.

        Args:
            key: Cache key

        Returns:
            True if key is in cache and not expired
        """
        with self._lock:
            if key not in self._cache:
                return False

            _, expiration_time = self._cache[key]

            # Check if expired
            if expiration_time is not None and time.time() > expiration_time:
                del self._cache[key]
                return False

            return True

    def size(self) -> int:
        """
        Get the current cache size.

        Returns:
            Number of items in cache
        """
        with self._lock:
            return len(self._cache)

    def keys(self) -> List[K]:
        """
        Get all keys in the cache.

        Returns:
            List of keys
        """
        with self._lock:
            return list(self._cache.keys())

    def cleanup(self) -> int:
        """
        Remove expired items from the cache.

        Returns:
            Number of items removed
        """
        with self._lock:
            now = time.time()
            expired_keys = [
                key for key, (_, expiration_time) in self._cache.items()
                if expiration_time is not None and now > expiration_time
            ]

            for key in expired_keys:
                del self._cache[key]

            return len(expired_keys)

    def _evict_oldest(self) -> None:
        """Evict the oldest item from the cache."""
        # Simple LRU implementation - just remove a random item
        # In a production system, this should be replaced with a proper LRU algorithm
        if self._cache:
            key = next(iter(self._cache))
            del self._cache[key]


def cached(cache: Optional[ThreadSafeCache] = None,
           ttl_seconds: Optional[float] = None,
           key_func: Optional[Callable[..., Any]] = None):
    """
    Decorator to cache function results.

    Args:
        cache: Cache to use (creates a new one if None)
        ttl_seconds: Time-to-live in seconds
        key_func: Function to generate cache key from function arguments

    Returns:
        Decorated function
    """
    if cache is None:
        cache = ThreadSafeCache(ttl_seconds=ttl_seconds)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                # Default key is based on function name and arguments
                key = f"{func.__module__}.{func.__name__}:{hash(str(args))}-{hash(str(sorted(kwargs.items())))}"

            # Check if in cache
            cached_result = cache.get(key)
            if cached_result is not None:
                return cached_result

            # Call function and cache result
            result = func(*args, **kwargs)
            cache.set(key, result, ttl_seconds)

            return result

        return wrapper

    return decorator


# Global caches for common entities
ASSET_CACHE = ThreadSafeCache[str, Any](ttl_seconds=3600, max_size=1000)  # 1 hour TTL
EXCHANGE_CACHE = ThreadSafeCache[str, Any](ttl_seconds=3600, max_size=100)
FIAT_CACHE = ThreadSafeCache[str, Any](ttl_seconds=3600, max_size=100)