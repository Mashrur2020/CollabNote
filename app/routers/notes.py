"""Notes router - CRUD over MongoDB via Motor, with Redis caching,
Elasticsearch indexing, and Kafka activity events.
"""
import hashlib
import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app import elasticsearch_client
from app.auth import decode_access_token
from app.events import (
    log_note_created,
    log_note_deleted,
    log_note_searched,
    log_note_updated,
)
from app.mongodb import async_db
from app.redis_client import redis_client
from app.schemas import NoteCreate, NoteOut, NoteUpdate, SearchResults

logger = logging.getLogger(__name__)

router = APIRouter(tags=["notes"])

security = HTTPBearer(auto_error=False)


def _user_id_from_email(email: str) -> int:
    """Stable, cross-process user_id derived from email."""
    digest = hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16)


def _parse_object_id(note_id: str) -> ObjectId:
    try:
        return ObjectId(note_id)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=404, detail="Note not found")


def get_current_user_email(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> str:
    """Decode the bearer token and return the user's email."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    email = decode_access_token(credentials.credentials)
    if email is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return email


def _note_doc_to_out(doc: dict) -> dict:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


@router.post("/", response_model=NoteOut, status_code=201)
async def create_note(
    note_data: NoteCreate,
    background: BackgroundTasks,
    email: str = Depends(get_current_user_email),
):
    """Create a new note for the authenticated user."""
    note = {
        "title": note_data.title,
        "content": note_data.content,
        "tags": note_data.tags or [],
        "user_email": email,
        "created_at": datetime.utcnow(),
        "updated_at": None,
    }

    result = await async_db["notes"].insert_one(note)
    note_id = str(result.inserted_id)
    note["id"] = note_id

    user_id = _user_id_from_email(email)

    # Fire-and-forget side-effects via BackgroundTasks so we don't block the response.
    background.add_task(
        elasticsearch_client.index_note,
        note_id=note_id,
        title=note["title"],
        content=note["content"],
        user_email=email,
        created_at=note["created_at"].isoformat(),
        tags=note["tags"],
    )
    background.add_task(redis_client.set_note, note_id, note)
    log_note_created(user_id, note_id, note["title"], note["tags"])

    return note


@router.get("/", response_model=list[NoteOut])
async def get_notes(email: str = Depends(get_current_user_email)):
    """Get all notes for the authenticated user."""
    cursor = async_db["notes"].find({"user_email": email})
    docs = await cursor.to_list(length=None)
    return [_note_doc_to_out(doc) for doc in docs]


@router.get("/search/", response_model=SearchResults)
async def search_notes(
    q: str = "",
    background: BackgroundTasks = BackgroundTasks(),
    email: str = Depends(get_current_user_email),
):
    """Search notes for the authenticated user via Elasticsearch."""
    if not q:
        return SearchResults(total=0, results=[])

    user_id = _user_id_from_email(email)

    try:
        result = await elasticsearch_client.search_notes(q, email)
        hits = result.get("results", []) if isinstance(result, dict) else []

        background.add_task(redis_client.set_search_results, user_id, q, hits)
        log_note_searched(user_id, q, len(hits))

        return SearchResults(total=result.get("total", 0), results=hits)
    except Exception as e:
        logger.warning("Search failed: %s", e)
        return SearchResults(total=0, results=[])


@router.get("/{note_id}", response_model=NoteOut)
async def get_note(
    note_id: str,
    response: Response,
    background: BackgroundTasks,
    email: str = Depends(get_current_user_email),
):
    """Get a specific note by ID (Redis cache HIT/MISS)."""
    object_id = _parse_object_id(note_id)

    cached = await redis_client.get_note(note_id)
    if cached and cached.get("user_email") == email:
        response.headers["Cache"] = "HIT"
        return cached

    doc = await async_db["notes"].find_one(
        {"_id": object_id, "user_email": email}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Note not found")

    note = _note_doc_to_out(doc)
    background.add_task(redis_client.set_note, note_id, note)
    response.headers["Cache"] = "MISS"
    return note


@router.put("/{note_id}", response_model=NoteOut)
async def update_note(
    note_id: str,
    note_data: NoteUpdate,
    background: BackgroundTasks,
    email: str = Depends(get_current_user_email),
):
    """Update a note by ID."""
    object_id = _parse_object_id(note_id)

    update_data = {
        k: v for k, v in note_data.model_dump().items() if v is not None
    }
    if update_data:
        update_data["updated_at"] = datetime.utcnow()
        await async_db["notes"].update_one(
            {"_id": object_id, "user_email": email},
            {"$set": update_data},
        )

    doc = await async_db["notes"].find_one(
        {"_id": object_id, "user_email": email}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Note not found")

    note = _note_doc_to_out(doc)
    user_id = _user_id_from_email(email)

    background.add_task(
        elasticsearch_client.update_note_index,
        note_id=note_id,
        title=note.get("title"),
        content=note.get("content"),
        tags=note.get("tags"),
    )
    background.add_task(redis_client.invalidate_note, note_id)
    background.add_task(redis_client.invalidate_search_cache, user_id)
    log_note_updated(
        user_id,
        note_id,
        note.get("title", ""),
        changed_fields=list(update_data.keys()),
    )

    return note


@router.delete("/{note_id}")
async def delete_note(
    note_id: str,
    background: BackgroundTasks,
    email: str = Depends(get_current_user_email),
):
    """Delete a note by ID."""
    object_id = _parse_object_id(note_id)

    result = await async_db["notes"].delete_one(
        {"_id": object_id, "user_email": email}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Note not found")

    user_id = _user_id_from_email(email)
    background.add_task(elasticsearch_client.delete_note_index, note_id)
    background.add_task(redis_client.invalidate_note, note_id)
    background.add_task(redis_client.invalidate_search_cache, user_id)
    log_note_deleted(user_id, note_id)

    return {"message": "Note deleted successfully"}
