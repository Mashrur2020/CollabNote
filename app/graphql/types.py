"""GraphQL types for CollabNote - Phase 4 spec.

Field names use camelCase (Strawberry converts from snake_case via
``strawberry.field(name=...)``) so the schema matches the capstone shape:

  - User       : id, username, email, createdAt, notes, activityLogs
  - Note       : id, userId, title, content, tags, createdAt, author
  - ActivityLog: id, eventType, userId, resourceId, timestamp, metadata
"""
import json
from typing import Optional

import strawberry


@strawberry.type
class User:
    id: strawberry.ID
    username: str
    email: str
    created_at: str = strawberry.field(name="createdAt")

    @strawberry.field
    async def notes(self, info: strawberry.Info) -> list["Note"]:
        """Resolver - MongoDB notes collection, scoped to this user."""
        ctx = info.context
        cursor = (
            ctx.mongodb["notes"]
            .find({"user_email": self.email})
            .sort("created_at", -1)
        )
        docs = await cursor.to_list(length=None)
        return [_note_from_doc(doc) for doc in docs]

    @strawberry.field(name="activityLogs")
    async def activity_logs(self, info: strawberry.Info) -> list["ActivityLog"]:
        """Resolver - MongoDB activity_logs collection, scoped to this user."""
        ctx = info.context
        cursor = (
            ctx.mongodb["activity_logs"]
            .find({"user_email": self.email})
            .sort("timestamp", -1)
            .limit(20)
        )
        docs = await cursor.to_list(length=20)
        return [_activity_from_doc(doc) for doc in docs]


@strawberry.type
class Note:
    id: strawberry.ID
    user_id: str = strawberry.field(name="userId")
    title: str
    content: str
    tags: list[str]
    created_at: str = strawberry.field(name="createdAt")

    @strawberry.field
    async def author(self, info: strawberry.Info) -> Optional[User]:
        """Resolver - PostgreSQL users table.

        ``user_id`` is the user's email (the foreign-key field stored on the
        note document). We resolve to a full User via SQLAlchemy.
        """
        from app.models import User as UserModel

        db = info.context.db
        user = (
            db.query(UserModel)
            .filter(UserModel.email == self.user_id)
            .first()
        )
        if not user:
            return None
        return User(
            id=strawberry.ID(str(user.id)),
            username=user.username,
            email=user.email,
            created_at=user.created_at.isoformat() if user.created_at else "",
        )


@strawberry.type
class ActivityLog:
    id: strawberry.ID
    event_type: str = strawberry.field(name="eventType")
    user_id: int = strawberry.field(name="userId")
    resource_id: Optional[str] = strawberry.field(name="resourceId", default=None)
    timestamp: str
    metadata: str


def _note_from_doc(doc: dict) -> Note:
    """Map a MongoDB notes document to the GraphQL Note type."""
    return Note(
        id=strawberry.ID(str(doc["_id"])),
        user_id=doc.get("user_email", ""),
        title=doc.get("title", ""),
        content=doc.get("content", ""),
        tags=doc.get("tags", []),
        created_at=str(doc.get("created_at", "")),
    )


def _activity_from_doc(doc: dict) -> ActivityLog:
    """Map a MongoDB activity_logs document to the GraphQL ActivityLog type.

    metadata is stored as a dict in Mongo, but the spec exposes it as a string
    - we JSON-encode it for safe serialization.
    """
    meta = doc.get("metadata", {})
    if not isinstance(meta, str):
        try:
            meta = json.dumps(meta, default=str)
        except (TypeError, ValueError):
            meta = str(meta)
    return ActivityLog(
        id=strawberry.ID(str(doc["_id"])),
        event_type=doc.get("event_type", ""),
        user_id=doc.get("user_id", 0),
        resource_id=doc.get("resource_id"),
        timestamp=str(doc.get("timestamp", "")),
        metadata=meta,
    )
