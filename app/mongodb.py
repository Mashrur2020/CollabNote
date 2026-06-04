"""MongoDB connection (Motor async client only)."""
import os

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "colabnote")

_async_client: AsyncIOMotorClient | None = None


def _get_client() -> AsyncIOMotorClient:
    global _async_client
    if _async_client is None:
        _async_client = AsyncIOMotorClient(MONGO_URL)
    return _async_client


class _AsyncDBProxy:
    """Lazy proxy so ``async_db["notes"]`` still works at import time."""

    def __getitem__(self, name: str):
        return _get_client()[MONGO_DB][name]


async_db = _AsyncDBProxy()
