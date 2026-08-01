"""Elasticsearch integration tests for ColabNote.

These tests require a running Elasticsearch instance at ES_URL.
They are skipped if ES is not available.

Each test runs in its own event loop via asyncio.run() to avoid
cross-test "Event loop is closed" issues with the shared async client.
"""
import asyncio
import os

import pytest
from elasticsearch import AsyncElasticsearch

import app.elasticsearch_client as es_mod

pytestmark = pytest.mark.skipif(
    os.getenv("SKIP_ES_TESTS", "true").lower() == "true",
    reason="Elasticsearch not available in test environment",
)

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
INDEX_NAME = os.getenv("ELASTICSEARCH_INDEX", "notes")


def _run(coro):
    """Run a coroutine with a fresh client installed in the module."""
    client = AsyncElasticsearch(hosts=[ES_URL], verify_certs=False)
    old_client = es_mod.es_client
    es_mod.es_client = client
    try:
        return asyncio.run(coro)
    finally:
        try:
            asyncio.run(es_mod.close_es())
        except Exception:
            pass
        es_mod.es_client = old_client


def _cleanup():
    async def _delete():
        try:
            await es_mod.es_client.indices.delete(index="_all")
        except Exception:
            pass

    _run(_delete())


def test_create_index():
    """Index should be created successfully."""
    _cleanup()

    async def run():
        await es_mod.create_index()
        return bool(await es_mod.es_client.indices.exists(index=INDEX_NAME))

    assert _run(run()) is True


def test_index_and_search_note():
    """Index a note and verify it is searchable."""
    _cleanup()

    async def run():
        await es_mod.create_index()
        await es_mod.index_note(
            note_id="test123",
            title="FastAPI Testing",
            content="This is a test note about FastAPI and Elasticsearch.",
            user_email="test@example.com",
            created_at="2026-01-01T00:00:00",
            tags=["fastapi", "testing"],
        )
        return await es_mod.search_notes("FastAPI", "test@example.com")

    results = _run(run())
    assert results["total"] >= 1
    assert any(r["title"] == "FastAPI Testing" for r in results["results"])


def test_search_scoped_to_user():
    """Search should only return notes belonging to the specified user."""
    _cleanup()

    async def run():
        await es_mod.create_index()
        await es_mod.index_note(
            note_id="user1_note",
            title="Private Note",
            content="Only user1 should see this.",
            user_email="user1@example.com",
            created_at="2026-01-01T00:00:00",
        )
        await es_mod.index_note(
            note_id="user2_note",
            title="Another Note",
            content="Only user2 should see this.",
            user_email="user2@example.com",
            created_at="2026-01-01T00:00:00",
        )
        return await es_mod.search_notes("Note", "user1@example.com")

    results = _run(run())
    assert results["total"] >= 1
    for r in results["results"]:
        assert r["user_email"] == "user1@example.com"


def test_delete_note_from_index():
    """Deleted note should not appear in search results."""
    _cleanup()

    async def run():
        await es_mod.create_index()
        await es_mod.index_note(
            note_id="delete_me",
            title="To Be Deleted",
            content="This note will be deleted.",
            user_email="test@example.com",
            created_at="2026-01-01T00:00:00",
        )
        await es_mod.delete_note_index("delete_me")
        return await es_mod.search_notes("Deleted", "test@example.com")

    results = _run(run())
    assert results["total"] == 0


def test_fuzzy_search():
    """Elasticsearch fuzzy matching should find notes with typos."""
    _cleanup()

    async def run():
        await es_mod.create_index()
        await es_mod.index_note(
            note_id="fuzzy_test",
            title="Elasticsearch Basics",
            content="Learning about fuzzy search in Elasticsearch.",
            user_email="test@example.com",
            created_at="2026-01-01T00:00:00",
        )
        # Intentional typo: "Elasticsearch" -> "Elastiksearch"
        return await es_mod.search_notes("Elastiksearch", "test@example.com")

    results = _run(run())
    assert results["total"] >= 1
