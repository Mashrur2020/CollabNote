import json
import os
from typing import Any
import redis.asyncio as aioredis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))


class RedisClient:
    """Async Redis Client wrapper for colabnote"""

    def __init__(self):
        self.redis: aioredis.Redis = None

    # connection
    async def connect(self) -> None:
        """Initialize Redis Connection"""
        self.redis = await aioredis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}",
            encoding="utf-8",
            decode_responses=True,
        )
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    async def close(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.close()
            print("Redis Connection Closed")

    # ==================== CACHE ====================

    async def get_note(self, note_id: str) -> dict | None:
        """Get cached note by ID"""
        if not self.redis:
            return None
        data = await self.redis.get(f"note:{note_id}")
        return json.loads(data) if data else None

    async def set_note(self, note_id: str, note_data: dict) -> None:
        """Cache a note"""
        if not self.redis:
            return
        await self.redis.setex(
            f"note:{note_id}",
            CACHE_TTL,
            json.dumps(note_data, default=str)
        )

    async def invalidate_note(self, note_id: str) -> None:
        """Remove note from cache"""
        if not self.redis:
            return
        await self.redis.delete(f"note:{note_id}")

    async def invalidate_user_notes(self, user_id: str) -> None:
        """Remove all cached notes for a user"""
        if not self.redis:
            return
        await self.redis.delete(f"notes:user:{user_id}")

    async def invalidate_search_cache(self, user_id: str) -> None:
        """Remove all search cache for a user"""
        if not self.redis:
            return
        keys = []
        async for key in self.redis.scan_iter(match=f"search:{user_id}:*"):
            keys.append(key)
        if keys:
            await self.redis.delete(*keys)

    async def get_user_notes(self, user_id: str) -> list | None:
        """Get cached user's notes list"""
        if not self.redis:
            return None
        data = await self.redis.get(f"notes:user:{user_id}")
        return json.loads(data) if data else None

    async def set_user_notes(self, user_id: str, notes: list) -> None:
        """Cache user's notes list"""
        if not self.redis:
            return
        await self.redis.setex(
            f"notes:user:{user_id}",
            CACHE_TTL,
            json.dumps(notes, default=str)
        )

    async def get_search_results(self, user_id: str, query: str) -> list | None:
        """Get cached search results"""
        if not self.redis:
            return None
        data = await self.redis.get(f"search:{user_id}:{query}")
        return json.loads(data) if data else None

    async def set_search_results(self, user_id: str, query: str, results: list) -> None:
        """Cache search results"""
        if not self.redis:
            return
        await self.redis.setex(
            f"search:{user_id}:{query}",
            CACHE_TTL,
            json.dumps(results, default=str)
        )


redis_client = RedisClient()