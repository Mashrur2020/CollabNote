"""Activity router - returns activity logs from MongoDB."""
import hashlib

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session

from app.auth import decode_access_token
from app.database import get_db
from app.models import User
from app.mongodb import async_db

router = APIRouter(tags=["activity"])

security = HTTPBearer(auto_error=False)


def _user_id_from_email(email: str) -> int:
    """Stable, cross-process user_id derived from email."""
    digest = hashlib.sha256(email.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16)


def get_current_user_email(
    credentials=Depends(security),
    db: Session = Depends(get_db),
) -> str:
    """Decode bearer token and return the authenticated user's email."""
    try:
        if credentials is None or credentials.credentials is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        email = decode_access_token(credentials.credentials)
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        user = db.query(User).filter(User.email == email).first()
        if user is None:
            raise HTTPException(status_code=401, detail="User not found")
        return email
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.get("/")
async def get_activity(email: str = Depends(get_current_user_email)):
    """
    Get last 20 activity log entries for the authenticated user.
    Returns entries sorted by timestamp descending.
    """
    user_id = _user_id_from_email(email)

    cursor = async_db["activity_logs"].find(
        {"user_id": user_id}
    ).sort("timestamp", -1).limit(20)

    activities = await cursor.to_list(length=20)

    return {
        "activities": [
            {
                "event_type": a.get("event_type"),
                "user_id": a.get("user_id"),
                "resource_id": a.get("resource_id"),
                "timestamp": a.get("timestamp"),
                "metadata": a.get("metadata", {}),
            }
            for a in activities
        ],
        "count": len(activities),
    }
