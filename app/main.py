import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import User
from .schemas import UserOut, NoteOut
from .auth import decode_access_token
from app.routers import auth_router, notes_router, activity_router
from app.mongodb import async_db
from .elasticsearch_client import create_index, close_es
from .redis_client import redis_client
from .events import start_publisher, stop_publisher


# GraphQL
from strawberry.fastapi import GraphQLRouter
from app.graphql.schema import schema
from app.graphql.context import get_context


load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    # startup
    print("Starting up ColabNote...")
    await redis_client.connect()
    await create_index()
    await start_publisher()
    print("All services initialized")

    yield  # app runs here

    # shutdown
    print("Shutting down ColabNote...")
    await stop_publisher()
    await redis_client.close()
    await close_es()


app = FastAPI(
    title="ColabNote API",
    description="API for ColabNote application with Elasticsearch search + Redis caching",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

INSTANCE_ID = os.getenv("INSTANCE_ID", "unknown")


class InstanceIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Instance-ID"] = INSTANCE_ID
        return response


app.add_middleware(InstanceIDMiddleware)

# Include routers
app.include_router(auth_router, prefix="/api/auth", tags=["auth"])
app.include_router(notes_router, prefix="/api/notes", tags=["notes"])
app.include_router(activity_router, prefix="/api/activity", tags=["activity"])

graphql_router = GraphQLRouter(schema, context_getter=get_context)
app.include_router(graphql_router, prefix="/graphql")

security = HTTPBearer()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Dependency to get current authenticated user"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token = credentials.credentials

    # Decode token to get email
    email = decode_access_token(token)
    if email is None:
        raise credentials_exception

    # Get user from database
    user = db.query(User).filter(User.email == email).first()
    if user is None:
        raise credentials_exception

    return user


# ─── General Endpoints ──────────────────────────────────────

@app.get("/ping")
def ping():
    """Health check endpoint"""
    return {"status": "ok", "message": "pong"}


@app.get("/profile", response_model=UserOut)
def get_profile(current_user: User = Depends(get_current_user)):
    """Get current user's profile"""
    return current_user


@app.get("/users", response_model=list[UserOut])
def get_users(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get all users (requires authentication)"""
    users = db.query(User).all()
    return users


@app.get("/users/{user_id}/notes", response_model=list[NoteOut])
async def get_user_notes(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Hybrid endpoint: verify the user exists in PostgreSQL, then return
    all of their notes from MongoDB.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    cursor = async_db["notes"].find({"user_email": user.email})
    docs = await cursor.to_list(length=None)
    result = []
    for note in docs:
        note["id"] = str(note.pop("_id"))
        result.append(note)

    return result