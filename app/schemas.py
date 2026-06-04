from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


# user schema

class UserCreate(BaseModel):
    email: EmailStr
    username: str = Field(min_length=6, max_length=50)
    password: str = Field(min_length=8, max_length=50)
    
    class Config:
        json_schema_extra = {
            "example": {
                "email": "alice@example.com",
                "username": "alice",
                "password": "securepassword123"
            }
        }


class UserOut(BaseModel):
    id: int
    email: EmailStr
    username: str

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "email": "alice@example.com",
                "username": "alice"
            }
        }


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

    class Config:
        json_schema_extra = {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer"
            }
        }


class TokenData(BaseModel):
    email: Optional[str] = None


# ─── Note Schemas ──────────────────────────────────────────

class NoteBase(BaseModel):
    title: str
    content: str
    tags: list[str] = []


class NoteCreate(NoteBase):
    pass


class NoteUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    tags: Optional[list[str]] = None


class NoteOut(BaseModel):
    id: str
    title: str
    content: str
    user_email: str
    tags: list[str] = []
    created_at: datetime
    updated_at: Optional[datetime] = None