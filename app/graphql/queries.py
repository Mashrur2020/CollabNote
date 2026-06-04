"""GraphQL Query resolvers for CollabNote - Phase 4 spec.

Queries:
  - me              : returns the authenticated user from JWT context
  - user(id: ID!)   : returns a user by PostgreSQL primary key
  - users           : returns all users
  - note(id: ID!)   : returns a single note by MongoDB ObjectId
  - notes           : returns all notes for the authenticated user

User-scoped queries (me, notes) raise a 401-style GraphQL error when the
Authorization header is missing or invalid.
"""
from typing import Optional

import strawberry
from bson import ObjectId

from app.graphql.context import GraphQLContext
from app.graphql.types import User, Note, _note_from_doc
from app.models import User as UserModel


def _require_user(info: strawberry.Info):
    """Return the authenticated user or raise an auth error.

    Maps to a 401-style GraphQL error in the response's errors array.
    """
    ctx: GraphQLContext = info.context
    if not ctx.user:
        raise Exception("Unauthorized: valid Bearer token required")
    return ctx.user


@strawberry.type
class Query:
    # me
    @strawberry.field
    async def me(self, info: strawberry.Info) -> Optional[User]:
        """Authenticated user's profile. Reads the JWT from context."""
        u = _require_user(info)
        return User(
            id=strawberry.ID(str(u.id)),
            username=u.username,
            email=u.email,
            created_at=u.created_at.isoformat() if u.created_at else "",
        )

    # user(id)
    @strawberry.field
    async def user(self, info: strawberry.Info, id: strawberry.ID) -> Optional[User]:
        """Look up a user by PostgreSQL primary key."""
        ctx: GraphQLContext = info.context
        try:
            user_id = int(id)
        except (TypeError, ValueError):
            raise Exception(f"Invalid user id: {id!r}")
        user = ctx.db.query(UserModel).filter(UserModel.id == user_id).first()
        if not user:
            return None
        return User(
            id=strawberry.ID(str(user.id)),
            username=user.username,
            email=user.email,
            created_at=user.created_at.isoformat() if user.created_at else "",
        )

    # users
    @strawberry.field
    async def users(self, info: strawberry.Info) -> list[User]:
        """All users (PostgreSQL)."""
        ctx: GraphQLContext = info.context
        users = ctx.db.query(UserModel).all()
        return [
            User(
                id=strawberry.ID(str(u.id)),
                username=u.username,
                email=u.email,
                created_at=u.created_at.isoformat() if u.created_at else "",
            )
            for u in users
        ]

    # note(id)
    @strawberry.field
    async def note(self, info: strawberry.Info, id: strawberry.ID) -> Optional[Note]:
        """Single note by MongoDB ObjectId, scoped to the authenticated user."""
        u = _require_user(info)
        ctx: GraphQLContext = info.context
        try:
            oid = ObjectId(str(id))
        except Exception:
            raise Exception(f"Invalid note id: {id!r}")
        doc = await ctx.mongodb["notes"].find_one(
            {"_id": oid, "user_email": u.email}
        )
        return _note_from_doc(doc) if doc else None

    # notes
    @strawberry.field
    async def notes(self, info: strawberry.Info) -> list[Note]:
        """All notes for the authenticated user, newest first."""
        u = _require_user(info)
        ctx: GraphQLContext = info.context
        cursor = (
            ctx.mongodb["notes"]
            .find({"user_email": u.email})
            .sort("created_at", -1)
        )
        docs = await cursor.to_list(length=None)
        return [_note_from_doc(doc) for doc in docs]
