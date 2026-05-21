import os
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from .database import SessionLocal, get_db
from .models import User
from .schemas import UserOut
from .auth import decode_access_token
from .routers import auth_router, notes_router

load_dotenv()

app = FastAPI(
    title="ColabNote API",
    description="API for ColabNote application",
    version="0.1.0"
)

# Include routers
app.include_router(auth_router)    # /auth/signup, /auth/login
app.include_router(notes_router)   # /notes/ CRUD operations

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