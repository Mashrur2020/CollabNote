from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from bson import ObjectId

from ..mongodb import notes
from ..schemas import NoteCreate, NoteUpdate, NoteOut
from ..auth import decode_access_token
from ..main import HTTPBearer

router = APIRouter(prefix="/notes", tags=["notes"])


def get_token(credentials: HTTPBearer = Depends(HTTPBearer())):
    """Extract bearer token from Authorization header"""
    return credentials.credentials


def get_current_user_email(token: str = Depends(get_token)) -> str:
    """Decode token and return user's email"""
    try:
        email = decode_access_token(token)
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return email
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.post("/", response_model=NoteOut, status_code=201)
def create_note(note_data: NoteCreate, email: str = Depends(get_current_user_email)):
    """Create a new note for the authenticated user"""
    note = {
        "title": note_data.title,
        "content": note_data.content,
        "user_email": email,
        "created_at": datetime.utcnow(),
        "updated_at": None,
    }

    result = notes.insert_one(note)
    note["id"] = str(result.inserted_id)
    
    return note


@router.get("/", response_model=list[NoteOut])
def get_notes(email: str = Depends(get_current_user_email)):
    """Get all notes for the authenticated user"""
    user_notes = notes.find({"user_email": email})
    result = []

    for note in user_notes:
        note["id"] = str(note.pop("_id"))
        result.append(note)

    return result


@router.get("/{note_id}", response_model=NoteOut)
def get_note(note_id: str, email: str = Depends(get_current_user_email)):
    """Get a specific note by ID"""
    note = notes.find_one({"_id": ObjectId(note_id), "user_email": email})

    if not note:
        raise HTTPException(status_code=404, detail="Note not found")

    note["id"] = str(note.pop("_id"))
    return note


@router.put("/{note_id}", response_model=NoteOut)
def update_note(note_id: str, note_data: NoteUpdate, email: str = Depends(get_current_user_email)):
    """Update a note by ID"""
    # Filter out None values
    update_data = {k: v for k, v in note_data.model_dump().items() if v is not None}

    if update_data:
        update_data["updated_at"] = datetime.utcnow()
        notes.update_one(
            {"_id": ObjectId(note_id), "user_email": email},
            {"$set": update_data}
        )

    note = notes.find_one({"_id": ObjectId(note_id), "user_email": email})
    if not note:
        raise HTTPException(status_code=404, detail="Note not found")

    note["id"] = str(note.pop("_id"))
    return note


@router.delete("/{note_id}")
def delete_note(note_id: str, email: str = Depends(get_current_user_email)):
    """Delete a note by ID"""
    result = notes.delete_one({"_id": ObjectId(note_id), "user_email": email})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Note not found")
    
    return {"message": "Note deleted successfully"}