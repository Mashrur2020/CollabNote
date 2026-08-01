"""Tests for note CRUD, the hybrid endpoint, and Redis caching.

Uses in-memory Mongo/Redis fakes from conftest so no external services
are required. Covers Phase 1 (note CRUD + hybrid endpoint) and Phase 2
(cache HIT/MISS) acceptance criteria.
"""
from datetime import datetime

import pytest


def _note_payload(**overrides):
    payload = {
        "title": "Meeting Notes",
        "content": "Discuss Q3 roadmap and hiring.",
        "tags": ["work", "roadmap"],
    }
    payload.update(overrides)
    return payload


async def _create_note(client, auth_headers, **overrides):
    return await client.post(
        "/api/notes/", json=_note_payload(**overrides), headers=auth_headers
    )


@pytest.mark.asyncio
async def test_create_note(client, auth_headers):
    """POST /api/notes/ with a valid JWT creates a note."""
    response = await _create_note(client, auth_headers)
    assert response.status_code == 201
    data = response.json()
    assert data["title"] == "Meeting Notes"
    assert data["content"] == "Discuss Q3 roadmap and hiring."
    assert data["tags"] == ["work", "roadmap"]
    assert data["user_email"] == "test@example.com"
    assert "id" in data


@pytest.mark.asyncio
async def test_create_note_requires_auth(client):
    """POST /api/notes/ without a token should return 401."""
    response = await client.post("/api/notes/", json=_note_payload())
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_notes_returns_only_own_notes(client, auth_headers):
    """GET /api/notes/ returns only the authenticated user's notes."""
    await _create_note(client, auth_headers, title="My Note")

    # A second user's note should not appear in the first user's list.
    await _create_note(
        client, auth_headers, title="Other User's Note", content="secret"
    )

    response = await client.get("/api/notes/", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert all(n["user_email"] == "test@example.com" for n in data)


@pytest.mark.asyncio
async def test_get_note_by_id(client, auth_headers):
    """GET /api/notes/{id} returns the created note."""
    created = await _create_note(client, auth_headers)
    note_id = created.json()["id"]

    response = await client.get(f"/api/notes/{note_id}", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == note_id
    assert data["title"] == "Meeting Notes"
    assert data["content"] == "Discuss Q3 roadmap and hiring."


@pytest.mark.asyncio
async def test_get_nonexistent_note_returns_404(client, auth_headers):
    """GET /api/notes/{bad-id} should return 404."""
    response = await client.get(
        "/api/notes/64b000000000000000000000", headers=auth_headers
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_note(client, auth_headers):
    """PUT /api/notes/{id} updates title/content and invalidates cache."""
    created = await _create_note(client, auth_headers)
    note_id = created.json()["id"]

    response = await client.put(
        f"/api/notes/{note_id}",
        json={"title": "Updated Title", "content": "New content."},
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Updated Title"
    assert data["content"] == "New content."

    # Cache should have been invalidated, so a fresh GET returns updated data.
    fetched = await client.get(f"/api/notes/{note_id}", headers=auth_headers)
    assert fetched.json()["title"] == "Updated Title"


@pytest.mark.asyncio
async def test_delete_note(client, auth_headers):
    """DELETE /api/notes/{id} removes the note."""
    created = await _create_note(client, auth_headers)
    note_id = created.json()["id"]

    response = await client.delete(f"/api/notes/{note_id}", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["message"] == "Note deleted successfully"

    fetched = await client.get(f"/api/notes/{note_id}", headers=auth_headers)
    assert fetched.status_code == 404


@pytest.mark.asyncio
async def test_delete_nonexistent_note_returns_404(client, auth_headers):
    """DELETE /api/notes/{bad-id} should return 404."""
    response = await client.delete(
        "/api/notes/64b000000000000000000000", headers=auth_headers
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_hybrid_endpoint_returns_user_notes(
    client, test_user, auth_headers, fake_mongo
):
    """GET /users/{user_id}/notes verifies the user in Postgres, then
    returns their notes from MongoDB."""
    created = await _create_note(client, auth_headers)
    assert created.status_code == 201

    response = await client.get(f"/users/{test_user.id}/notes", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["title"] == "Meeting Notes"
    assert data[0]["user_email"] == test_user.email


@pytest.mark.asyncio
async def test_hybrid_endpoint_unknown_user_returns_404(client, auth_headers):
    """GET /users/{missing}/notes should return 404 for a non-existent user."""
    response = await client.get("/users/99999/notes", headers=auth_headers)
    assert response.status_code == 404


# ─── Redis cache HIT/MISS ──────────────────────────────────


@pytest.mark.asyncio
async def test_cache_miss_then_hit(client, auth_headers, fake_redis):
    """First GET is a MISS (served from Mongo); second is a HIT (Redis)."""
    created = await _create_note(client, auth_headers)
    note_id = created.json()["id"]
    await fake_redis.invalidate_note(note_id)

    first = await client.get(f"/api/notes/{note_id}", headers=auth_headers)
    assert first.status_code == 200
    assert first.headers.get("Cache") == "MISS"
    assert fake_redis.store.get(f"note:{note_id}") is not None

    second = await client.get(f"/api/notes/{note_id}", headers=auth_headers)
    assert second.status_code == 200
    assert second.headers.get("Cache") == "HIT"
    assert second.json()["id"] == note_id


@pytest.mark.asyncio
async def test_update_invalidates_cache(client, auth_headers, fake_redis):
    """Updating a note evicts it from Redis."""
    created = await _create_note(client, auth_headers)
    note_id = created.json()["id"]

    # Populate the cache.
    await client.get(f"/api/notes/{note_id}", headers=auth_headers)
    assert fake_redis.store.get(f"note:{note_id}") is not None

    await client.put(
        f"/api/notes/{note_id}",
        json={"title": "New Title"},
        headers=auth_headers,
    )
    assert fake_redis.store.get(f"note:{note_id}") is None


@pytest.mark.asyncio
async def test_search_empty_query(client, auth_headers):
    """GET /api/notes/search/?q= returns empty results."""
    response = await client.get("/api/notes/search/?q=", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {"total": 0, "results": []}
