from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
MONGO_DB = os.getenv("MONGO_DB", "colabnote")


# ─── Sync Client (PyMongo) - for scripts/testing ────────────

sync_client = MongoClient(MONGO_URL)
sync_db = sync_client[MONGO_DB]
notes = sync_db["notes"]


# ─── Async Client (Motor) - for FastAPI endpoints ───────────

async_client = AsyncIOMotorClient(MONGO_URL)
async_db = async_client[MONGO_DB]
async_notes = async_db["notes"]