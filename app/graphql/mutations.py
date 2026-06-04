"""GraphQL Mutation resolvers for CollabNote - Phase 4 spec.

Mutations:
  - createNote(title, content, tags)
        Mirror of POST /api/notes: MongoDB insert, Elasticsearch indexing,
        Redis cache, and Kafka event publication.
  - updateUser(id, username, email)
        Update a PostgreSQL user. Only the authenticated user may update
        their own profile (others get a 403-style error).
"""
import asyncio
import hashlib
from datetime import datetime
from typing import Optional

import strawberry

from app import elasticsearch_client
from app.events import log_note_created
from app.graphql.context import GraphQLContext
from app.graphql.types import Note, User, _note_from_doc
from app.models import User as UserModel
from app.redis_client import redis_client


def _user_id_from_email(email: str) -> int:
    """Stable, cross-process user_id derived from email (matches REST)."""
    digest = hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16)


def _require_user(info: strawberry.Info):
    """Return the authenticated user or raise a 401-style error."""
    ctx: GraphQLContext = info.context
    if not ctx.user:
        raise Exception("Unauthorized: valid Bearer token required")
    return ctx.user


@strawberry.type
class Mutation:
    @strawberry.mutation
    async def create_note(
        self,
        info: strawberry.Info,
        title: str,
        content: str,
        tags: list[str],
    ) -> Optional[Note]:
        """Create a note. Same side-effects as POST /api/notes."""
        u = _require_user(info)
        ctx: GraphQLContext = info.context
        email = u.email
        user_id = _user_id_from_email(email)

        note_doc = {
            "title": title,
            "content": content,
            "tags": tags or [],
            "user_email": email,
            "created_at": datetime.utcnow(),
            "updated_at": None,
        }
        result = await ctx.mongodb["notes"].insert_one(note_doc)
        note_id = str(result.inserted_id)
        note_doc["id"] = note_id

        loop = asyncio.get_running_loop()
        loop.create_task(
            elasticsearch_client.index_note(
                note_id=note_id,
                title=title,
                content=content,
                user_email=email,
                created_at=note_doc["created_at"].isoformat(),
                tags=note_doc["tags"],
            )
        )
        loop.create_task(redis_client.set_note(note_id, note_doc))

        log_note_created(user_id, note_id, title, note_doc["tags"])

        return _note_from_doc({**note_doc, "_id": result.inserted_id})

    @strawberry.mutation
    async def update_user(
        self,
        info: strawberry.Info,
        id: strawberry.ID,
        username: Optional[str] = None,
        email: Optional[str] = None,
    ) -> Optional[User]:
        """Update a PostgreSQL user. Only the authenticated user may
        update their own profile."""
        u = _require_user(info)
        ctx: GraphQLContext = info.context

        try:
            target_id = int(id)
        except (TypeError, ValueError):
            raise Exception(f"Invalid user id: {id!r}")

        if target_id != u.id:
            raise Exception("Forbidden: you can only update your own profile")

        user = ctx.db.query(UserModel).filter(UserModel.id == target_id).first()
        if not user:
            raise Exception("User not found")

        if username is not None:
            user.username = username
        if email is not None:
            user.email = email
        ctx.db.commit()
        ctx.db.refresh(user)

        return User(
            id=strawberry.ID(str(user.id)),
            username=user.username,
            email=user.email,
            created_at=user.created_at.isoformat() if user.created_at else "",
        )
