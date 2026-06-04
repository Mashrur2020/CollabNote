import logging
from dataclasses import dataclass
from typing import Optional

from fastapi import Request
from motor.motor_asyncio import AsyncIOMotorDatabase
from sqlalchemy.orm import Session
from strawberry.fastapi import BaseContext

from app.auth import decode_access_token
from app.database import SessionLocal
from app.models import User as UserModel
from app.mongodb import async_db

logger = logging.getLogger(__name__)


@dataclass
class GraphQLContext(BaseContext):
    request: Request
    user: Optional[UserModel]
    db: Session
    mongodb: AsyncIOMotorDatabase = async_db

    def on_close(self) -> None:
        """Strawberry hook: close the SQLAlchemy session after each request."""
        try:
            self.db.close()
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("Failed to close GraphQL DB session: %s", e)


async def get_context(request: Request) -> GraphQLContext:
    db = SessionLocal()
    user: Optional[UserModel] = None
    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header[7:]
        email = decode_access_token(token)
        if email:
            user = db.query(UserModel).filter(UserModel.email == email).first()
    return GraphQLContext(request=request, user=user, db=db, mongodb=async_db)
