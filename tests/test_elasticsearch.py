"""Elasticsearch integration tests for ColabNote.

These tests require a running Elasticsearch instance at ES_URL.
They are skipped if ES is not available.
"""
import os
import pytest
from app.elasticsearch_client import (
    es_client,
    create_index,
    index_note,
    search_notes,
    delete_note_index,
    close_es,
)

pytestmark = pytest.mark.skipif(
    os.getenv("SKIP_ES_TESTS", "true").lower() == "true",
    reason="Elasticsearch not available in test environment",
)


@pytest.fixture(autouse=True)
async def setup_es():
    """Ensure index exists before each test and clean up after."""
    await create_index()
    yield
    try:
        await es_client.indices.delete(index="_all")
    except Exception:
        pass


@pytest.mark.asyncio
async def test_create_index():
    """Index should be created successfully."""
    exists = await es_client.indices.exists(index="notes")
    assert exists is True


@pytest.mark.asyncio
async def test_index_and_search_note():
    """Index a note and verify it is searchable."""
    await index_note(
        note_id="test123",
        title="FastAPI Testing",
        content="This is a test note about FastAPI and Elasticsearch.",
        user_email="test@example.com",
        created_at="2026-01-01T00:00:00",
        tags=["fastapi", "testing"],
    )

    results = await search_notes("FastAPI", "test@example.com")
    assert results["total"] >= 1
    assert any(r["title"] == "FastAPI Testing" for r in results["results"])


@pytest.mark.asyncio
async def test_search_scoped_to_user():
    """Search should only return notes belonging to the specified user."""
    await index_note(
        note_id="user1_note",
        title="Private Note",
        content="Only user1 should see this.",
        user_email="user1@example.com",
        created_at="2026-01-01T00:00:00",
    )
    await index_note(
        note_id="user2_note",
        title="Another Note",
        content="Only user2 should see this.",
        user_email="user2@example.com",
        created_at="2026-01-01T00:00:00",
    )

    results = await search_notes("Note", "user1@example.com")
    assert results["total"] >= 1
    for r in results["results"]:
        assert r["user_email"] == "user1@example.com"


@pytest.mark.asyncio
async def test_delete_note_from_index():
    """Deleted note should not appear in search results."""
    await index_note(
        note_id="delete_me",
        title="To Be Deleted",
        content="This note will be deleted.",
        user_email="test@example.com",
        created_at="2026-01-01T00:00:00",
    )
    await delete_note_index("delete_me")

    results = await search_notes("Deleted", "test@example.com")
    assert results["total"] == 0


@pytest.mark.asyncio
async def test_fuzzy_search():
    """Elasticsearch fuzzy matching should find notes with typos."""
    await index_note(
        note_id="fuzzy_test",
        title="Elasticsearch Basics",
        content="Learning about fuzzy search in Elasticsearch.",
        user_email="test@example.com",
        created_at="2026-01-01T00:00:00",
    )

    # Intentional typo: "Elasticsearch" -> "Elastiksearch"
    results = await search_notes("Elastiksearch", "test@example.com")
    assert results["total"] >= 1