import redis
import json
import os
from datetime import timedelta
from typing import Optional, Any
from dotenv import load_dotenv

load_dotenv()

class CacheManager:
    """Redis cache manager with graceful fallback"""
    
    def __init__(self):
        self.redis_client = None
        self.enabled = False
        self._initialize_redis()
    
    def _initialize_redis(self):
        """Initialize Redis connection with error handling"""
        try:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", 6379))
            redis_db = int(os.getenv("REDIS_DB", 0))
            redis_password = os.getenv("REDIS_PASSWORD", None)
            
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password if redis_password else None,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2
            )
            
            # Test connection
            self.redis_client.ping()
            self.enabled = True
            print(f"✅ Redis cache connected: {redis_host}:{redis_port}")
            
        except Exception as e:
            print(f"⚠️ Redis cache unavailable: {e}")
            print("📝 Falling back to direct queries (no caching)")
            self.enabled = False
            self.redis_client = None
    
    def get_cached_data(self, key: str) -> Optional[Any]:
        """
        Retrieve cached data by key.
        Returns None if cache miss or Redis unavailable.
        """
        if not self.enabled or not self.redis_client:
            return None
        
        try:
            data = self.redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            print(f"⚠️ Cache read error for key '{key}': {e}")
            return None
    
    def set_cached_data(self, key: str, data: Any, ttl_minutes: int = 5) -> bool:
        """
        Store data in cache with TTL.
        Returns True if successful, False otherwise.
        """
        if not self.enabled or not self.redis_client:
            return False
        
        try:
            ttl_seconds = ttl_minutes * 60
            self.redis_client.setex(
                key,
                timedelta(seconds=ttl_seconds),
                json.dumps(data)
            )
            return True
        except Exception as e:
            print(f"⚠️ Cache write error for key '{key}': {e}")
            return False
    
    def invalidate_cache(self, pattern: str = "*") -> int:
        """
        Invalidate cache keys matching pattern.
        Returns number of keys deleted.
        """
        if not self.enabled or not self.redis_client:
            return 0
        
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            print(f"⚠️ Cache invalidation error for pattern '{pattern}': {e}")
            return 0
    
    def get_cache_stats(self) -> dict:
        """Get cache statistics"""
        if not self.enabled or not self.redis_client:
            return {
                "enabled": False,
                "status": "unavailable"
            }
        
        try:
            info = self.redis_client.info("stats")
            return {
                "enabled": True,
                "status": "connected",
                "total_keys": self.redis_client.dbsize(),
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "hit_rate": self._calculate_hit_rate(
                    info.get("keyspace_hits", 0),
                    info.get("keyspace_misses", 0)
                )
            }
        except Exception as e:
            return {
                "enabled": False,
                "status": f"error: {e}"
            }
    
    def _calculate_hit_rate(self, hits: int, misses: int) -> float:
        """Calculate cache hit rate percentage"""
        total = hits + misses
        if total == 0:
            return 0.0
        return round((hits / total) * 100, 2)

# Global cache instance
cache = CacheManager()

# Helper functions for easy access
def get_cached_data(key: str) -> Optional[Any]:
    """Get cached data"""
    return cache.get_cached_data(key)

def set_cached_data(key: str, data: Any, ttl_minutes: int = 5) -> bool:
    """Set cached data with TTL"""
    return cache.set_cached_data(key, data, ttl_minutes)

def invalidate_cache(pattern: str = "*") -> int:
    """Invalidate cache keys"""
    return cache.invalidate_cache(pattern)

def get_cache_stats() -> dict:
    """Get cache statistics"""
    return cache.get_cache_stats()
