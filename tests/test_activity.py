"""Tests for the activity log endpoint (Phase 3 acceptance criteria).

The activity log is written to MongoDB by the Kafka consumer; these tests
seed the fake activity_logs collection directly and verify the endpoint
returns a chronological, user-scoped log.
"""
import pytest
from bson import ObjectId

from app.routers.activity import _user_id_from_email

EMAIL = "test@example.com"
USER_ID = _user_id_from_email(EMAIL)


def _seed_activity(fake_mongo, event_type, user_id=USER_ID, **metadata):
    return fake_mongo["activity_logs"]._docs.append(
        {
            "_id": ObjectId(),
            "event_type": event_type,
            "user_id": user_id,
            "resource_id": "note123",
            "timestamp": "2026-01-01T12:00:00Z",
            "metadata": metadata,
        }
    )


@pytest.mark.asyncio
async def test_activity_requires_auth(client):
    """GET /api/activity/ without a token should return 401."""
    response = await client.get("/api/activity/")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_activity_returns_logs(client, auth_headers, fake_mongo):
    """GET /api/activity/ returns the seeded activity entries."""
    _seed_activity(fake_mongo, "note_created", title="Hello")
    _seed_activity(fake_mongo, "note_searched", query="fastapi")

    response = await client.get("/api/activity/", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    event_types = {a["event_type"] for a in data["activities"]}
    assert event_types == {"note_created", "note_searched"}


@pytest.mark.asyncio
async def test_activity_scoped_to_user(client, auth_headers, fake_mongo):
    """Only the authenticated user's activity is returned."""
    _seed_activity(fake_mongo, "note_created", user_id=USER_ID)
    _seed_activity(fake_mongo, "note_deleted", user_id=999999)

    response = await client.get("/api/activity/", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["activities"][0]["event_type"] == "note_created"


@pytest.mark.asyncio
async def test_activity_empty_for_new_user(client, auth_headers):
    """A user with no activity gets an empty log."""
    response = await client.get("/api/activity/", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {"activities": [], "count": 0}


@pytest.mark.asyncio
async def test_activity_sorted_descending(client, auth_headers, fake_mongo):
    """Entries are sorted by timestamp descending."""
    _seed_activity(fake_mongo, "note_created")
    _seed_activity(fake_mongo, "note_deleted")

    response = await client.get("/api/activity/", headers=auth_headers)
    data = response.json()
    timestamps = [a["timestamp"] for a in data["activities"]]
    assert timestamps == sorted(timestamps, reverse=True)
